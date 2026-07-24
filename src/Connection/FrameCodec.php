<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Compression\Lz4Compressor;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Request\Request;

final class FrameCodec extends NodeImplementation {
    final public const CRC24_INIT = 0x875060;
    final public const CRC24_POLYNOMIAL = 0x1974F0B;
    final public const PAYLOAD_MAX_SIZE = 131071;

    /**
     * Payload size (bytes) at or above which the CRC32 is computed incrementally
     * rather than by concatenating the prefix onto the payload. Below it the
     * concat is cheaper than the hash_init/update/final overhead; above it,
     * avoiding the full-payload copy wins (measured crossover is ~3 KiB).
     */
    private const PAYLOAD_CRC32_INCREMENTAL_THRESHOLD = 4096;

    /**
     * Shared, lazily built CRC24 byte lookup table.
     *
     * @var ?array<int, int>
     */
    private static ?array $crc24Table = null;

    private string $compression;

    private string $crc32Prefix;

    /**
     * @var ?array{
     *  payloadLength: int,
     *  uncompressedLength: int,
     * } $currentFrameHeader
     */
    private ?array $currentFrameHeader;

    private ?Lz4Compressor $lz4Compressor;

    private ?Lz4Decompressor $lz4Decompressor;

    private Node $node;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function __construct(Node $node, string $compression = '') {
        if ($compression && $compression !== 'lz4') {
            throw new NodeException(
                message: 'Unsupported frame compression algorithm',
                code: ExceptionCode::NODE_UNSUPPORTED_COMPRESSION->value,
                context: [
                    'compression' => $compression,
                    'supported' => ['lz4'],
                    'host' => $node->getConfig()->host,
                    'port' => $node->getConfig()->port,
                ]
            );
        }

        $this->crc32Prefix = pack('N', 0xFA2D55CA);

        if ($compression) {
            $this->lz4Decompressor = new Lz4Decompressor();
            $this->lz4Compressor = new Lz4Compressor();
        } else {
            $this->lz4Decompressor = null;
            $this->lz4Compressor = null;
        }

        $this->node = $node;
        $this->compression = $compression;
        $this->currentFrameHeader = null;
    }

    #[\Override]
    public function close(): void {
        $this->node->close();
    }

    #[\Override]
    public function getConfig(): NodeConfig {
        return $this->node->getConfig();
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\CompressionException
     */
    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string {

        $data = '';
        $length = 0;
        do {
            $frameData = $this->readFrameData($waitForData);
            if ($frameData === null) {
                break;
            }

            if ($frameData !== '') {
                $length += strlen($frameData);
                $data .= $frameData;
            }

            $waitForData = false;

        } while ($length < $upperBoundaryLength);

        return $data;
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    public function write(string $data): void {
        $this->node->write($data);
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $data = $request->__toString();
        $dataLength = strlen($data);

        if ($dataLength <= self::PAYLOAD_MAX_SIZE) {
            $this->writeFrame($data, true, 0, $dataLength);
        } else {
            $offset = 0;
            $remainingDataLength = $dataLength;

            do {
                $writeDataLength = min(self::PAYLOAD_MAX_SIZE, $remainingDataLength);

                $this->writeFrame($data, false, $offset, $writeDataLength);

                $remainingDataLength -= $writeDataLength;
                $offset += $writeDataLength;
            } while ($remainingDataLength);
        }
    }

    private function crc24(string $data, int $length = 0): int {
        // Table-driven, one lookup per byte instead of eight bit operations —
        // ~5x faster, and this runs once per frame in each direction. Every step
        // stays within 24 bits (the 0xFFFFFF masks), so it is 32-bit safe.
        $table = self::$crc24Table ??= self::buildCrc24Table();

        $crc = self::CRC24_INIT;
        $len = $length > 0 ? $length : strlen($data);
        for ($i = 0; $i < $len; $i++) {
            $crc = (($crc << 8) & 0xFFFFFF) ^ $table[(($crc >> 16) ^ ord($data[$i])) & 0xFF];
        }

        return $crc;
    }

    /**
     * Compute the raw (big-endian) CRC32 of the fixed prefix followed by the
     * payload — the checksum the Cassandra v5 framing puts after each payload.
     *
     * For large payloads the prefix is fed incrementally so the payload is not
     * copied just to prepend four bytes; for small payloads a plain concat is
     * cheaper than the hashing-context overhead.
     */
    private function payloadCrc32(string $payload): string {
        if (strlen($payload) >= self::PAYLOAD_CRC32_INCREMENTAL_THRESHOLD) {
            $context = hash_init('crc32b');
            hash_update($context, $this->crc32Prefix);
            hash_update($context, $payload);

            return hash_final($context, true);
        }

        return hash('crc32b', $this->crc32Prefix . $payload, true);
    }

    /**
     * Build the 256-entry CRC24 byte lookup table (one entry = the bitwise CRC24
     * of a single byte). Computed once and cached in {@see self::$crc24Table}.
     *
     * @return array<int, int>
     */
    private static function buildCrc24Table(): array {
        $table = [];
        for ($b = 0; $b < 256; $b++) {
            $crc = $b << 16;
            for ($j = 0; $j < 8; $j++) {
                $crc <<= 1;
                if ($crc & 0x1000000) {
                    $crc ^= self::CRC24_POLYNOMIAL;
                }
            }
            $table[$b] = $crc & 0xFFFFFF;
        }

        return $table;
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\CompressionException
     */
    private function readFrameData(bool $waitForData): ?string {

        if ($this->currentFrameHeader === null) {
            $this->currentFrameHeader = $this->readFrameHeader($waitForData);
            if ($this->currentFrameHeader === null) {
                return null;
            }
        }

        $frameHeader = $this->currentFrameHeader;

        $payloadLength = $frameHeader['payloadLength'];
        $uncompressedLength = $frameHeader['uncompressedLength'];

        if ($payloadLength === 0) {
            $this->currentFrameHeader = null;

            return '';
        }

        $payload = $this->node->read($payloadLength + 4, $waitForData);
        if ($payload === '') {
            return null;
        }

        $this->currentFrameHeader = null;

        $checksum = substr($payload, $payloadLength, 4);
        $payload = substr($payload, 0, $payloadLength);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('V', $checksum);
        if ($unpacked === false) {
            throw new NodeException(
                message: 'Failed to decode frame payload checksum',
                code: ExceptionCode::NODE_DECODE_PAYLOAD_CRC32_FAILED->value,
                context: [
                    'host' => $this->getConfig()->host,
                    'port' => $this->getConfig()->port,
                    'stage' => 'read_payload_crc32',
                    'compression' => $this->compression,
                    'payload_length' => $payloadLength,
                ]
            );
        }
        $payloadCrc32 = $unpacked[1];

        $currentChecksum = $this->payloadCrc32($payload);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('N', $currentChecksum);
        if ($unpacked === false) {
            throw new NodeException(
                message: 'Failed to decode computed payload checksum',
                code: ExceptionCode::NODE_DECODE_COMPUTED_CRC32_FAILED->value,
                context: [
                    'host' => $this->getConfig()->host,
                    'port' => $this->getConfig()->port,
                    'stage' => 'decode_computed_crc32',
                    'compression' => $this->compression,
                    'payload_length' => $payloadLength,
                ]
            );
        }
        if ($unpacked[1] !== $payloadCrc32) {
            throw new NodeException(
                message: 'Invalid frame payload checksum',
                code: ExceptionCode::NODE_INVALID_PAYLOAD_CRC32->value,
                context: [
                    'host' => $this->getConfig()->host,
                    'port' => $this->getConfig()->port,
                    'stage' => 'verify_payload_crc32',
                    'expected_crc32' => $unpacked[1],
                    'actual_crc32' => $payloadCrc32,
                    'compression' => $this->compression,
                    'payload_length' => $payloadLength,
                ]
            );
        }

        if ($this->compression) {
            if ($this->lz4Decompressor === null) {
                throw new NodeException(
                    message: 'Decompression failed: LZ4 decompressor not initialized',
                    code: ExceptionCode::NODE_DECOMPRESSOR_NOT_INITIALIZED->value,
                    context: [
                        'host' => $this->getConfig()->host,
                        'port' => $this->getConfig()->port,
                        'stage' => 'decompress_payload_init',
                        'compression' => $this->compression,
                    ]
                );
            }

            if ($uncompressedLength > 0) {
                $compressedLength = $payloadLength;
                $this->lz4Decompressor->setInput($payload, 0, $payloadLength);

                $payload = $this->lz4Decompressor->decompressBlock($uncompressedLength);
                $payloadLength = strlen($payload);

                if ($payloadLength !== $uncompressedLength) {
                    throw new NodeException(
                        message: 'Decompression failed: invalid uncompressed length',
                        code: ExceptionCode::NODE_INVALID_UNCOMPRESSED_LENGTH->value,
                        context: [
                            'host' => $this->getConfig()->host,
                            'port' => $this->getConfig()->port,
                            'stage' => 'decompress_payload_length_check',
                            'expected_uncompressed_length' => $uncompressedLength,
                            'actual_uncompressed_length' => $payloadLength,
                            'compressed_length' => $compressedLength,
                        ]
                    );
                }
            }
        }

        return $payload;
    }

    /**
     * @return ?array{
     *  payloadLength: int,
     *  uncompressedLength: int,
     * }
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    private function readFrameHeader(bool $waitForData): ?array {
        if ($this->compression) {
            // 5-byte header + 3-byte CRC24.
            $header = $this->node->read(8, $waitForData);
            if ($header === '') {
                return null;
            }

            $headerLength = 5;
            [$payloadLength, $uncompressedLength, $headerCrc24] = $this->decodeCompressedFrameHeader($header);
        } else {
            // 3-byte header + 3-byte CRC24.
            $header = $this->node->read(6, $waitForData);
            if ($header === '') {
                return null;
            }

            $headerLength = 3;
            [$payloadLength, $uncompressedLength, $headerCrc24] = $this->decodeUncompressedFrameHeader($header);
        }

        $computedCrc24 = $this->crc24($header, $headerLength);
        if ($computedCrc24 !== $headerCrc24) {
            throw new NodeException(
                message: 'Invalid frame header checksum',
                code: ExceptionCode::NODE_INVALID_HEADER_CRC24->value,
                context: [
                    'host' => $this->getConfig()->host,
                    'port' => $this->getConfig()->port,
                    'stage' => 'verify_header_crc24',
                    'expected_crc24' => $computedCrc24,
                    'actual_crc24' => $headerCrc24,
                    'compression' => $this->compression,
                    'header_length' => $headerLength,
                    'header_hex' => bin2hex($header),
                ]
            );
        }

        return [
            'payloadLength' => $payloadLength,
            'uncompressedLength' => $uncompressedLength,
        ];
    }

    private function frameHeaderDecodeException(string $header, int $headerLength): NodeException {
        return new NodeException(
            message: 'Failed to decode frame header',
            code: ExceptionCode::NODE_DECODE_FRAME_HEADER_FAILED->value,
            context: [
                'host' => $this->getConfig()->host,
                'port' => $this->getConfig()->port,
                'stage' => 'read_frame_header',
                'compression' => $this->compression,
                'header_length' => $headerLength,
                'header_hex' => bin2hex($header),
            ]
        );
    }

    /**
     * Decode the 8-byte compressed (protocol v5) frame header.
     *
     * Layout, little-endian, of the 5 header bytes followed by a 3-byte CRC24:
     *   bits  0-16 : compressed payload length (17 bits)
     *   bits 17-33 : uncompressed payload length (17 bits)
     *   bit  34    : self-contained flag
     *   bits 35-39 : unused
     *
     * The two little-endian 32-bit words are read with unpack() (the fast path),
     * then every field is extracted with a mask. That masking is what keeps this
     * 32-bit safe: unpack('V') returns a signed (possibly negative) int on 32-bit
     * PHP, but the sign bits never survive the AND, so each field is correct.
     *
     * @return array{0: int, 1: int, 2: int} [payloadLength, uncompressedLength, headerCrc24]
     *
     * @throws \Cassandra\Exception\NodeException
     */
    private function decodeCompressedFrameHeader(string $header): array {
        /** @var false|array<int> $words */
        $words = unpack('V2', $header);
        if ($words === false) {
            throw $this->frameHeaderDecodeException($header, 5);
        }

        $payloadLength = $words[1] & 0x1FFFF;
        $uncompressedLength = (($words[1] >> 17) & 0x7FFF) | (($words[2] & 0x03) << 15);
        //$isSelfContained = $words[2] & 0x04;
        $headerCrc24 = ($words[2] >> 8) & 0xFFFFFF;

        return [$payloadLength, $uncompressedLength, $headerCrc24];
    }

    /**
     * Decode the 6-byte uncompressed (protocol v5) frame header.
     *
     * Layout, little-endian, of the 3 header bytes followed by a 3-byte CRC24:
     *   bits 0-16 : payload length (17 bits)
     *   bit  17   : self-contained flag
     *   bits 18-23: unused
     *
     * Read as three unsigned 16-bit words, which are always non-negative even on
     * 32-bit PHP, so no sign handling is needed.
     *
     * @return array{0: int, 1: int, 2: int} [payloadLength, uncompressedLength (always 0), headerCrc24]
     *
     * @throws \Cassandra\Exception\NodeException
     */
    private function decodeUncompressedFrameHeader(string $header): array {
        /** @var false|array<int> $words */
        $words = unpack('v3', $header);
        if ($words === false) {
            throw $this->frameHeaderDecodeException($header, 3);
        }

        $payloadLength = $words[1] | (($words[2] & 0x01) << 16);
        //$isSelfContained = $words[2] & 0x02;
        $headerCrc24 = (($words[2] >> 8) & 0xFF) | ($words[3] << 8);

        return [$payloadLength, 0, $headerCrc24];
    }

    /**
     * Encode the 3-byte little-endian CRC24 trailer appended to every frame
     * header. pack('VX') writes the value as a 32-bit little-endian word and then
     * backs up one byte, leaving the low 3 bytes; the CRC24 is <= 0xFFFFFF, so
     * this stays within a 32-bit integer.
     */
    private function encodeCrc24(string $header, int $headerLength): string {
        return pack('VX', $this->crc24($header, $headerLength));
    }

    /**
     * Build the 8-byte compressed (protocol v5) frame header.
     *
     * Mirrors {@see decodeCompressedFrameHeader()}. The full value spans 35 bits,
     * so it cannot be packed as a single 32-bit word on 32-bit PHP. Instead the
     * low 32 bits are emitted with pack('V') and the remaining 3 bits go in a
     * trailing byte. The low word only ever holds bits 0-31 (the uncompressed
     * length is masked to its low 15 bits before shifting, so nothing shifts past
     * bit 31), which keeps the intermediate within a signed 32-bit int; pack('V')
     * then writes its low 32 bits regardless of sign.
     */
    private function encodeCompressedFrameHeader(int $payloadLength, int $uncompressedLength, bool $isSelfContained): string {
        $lowWord = $payloadLength | (($uncompressedLength & 0x7FFF) << 17);
        $highByte = (($uncompressedLength >> 15) & 0x03) | ($isSelfContained ? 0x04 : 0);

        $header = pack('V', $lowWord) . chr($highByte);

        return $header . $this->encodeCrc24($header, 5);
    }

    /**
     * Build the 6-byte uncompressed (protocol v5) frame header.
     *
     * Mirrors {@see decodeUncompressedFrameHeader()}. The whole value fits in 18
     * bits, well within a 32-bit int, so pack('VX') (3 low bytes) is enough.
     */
    private function encodeUncompressedFrameHeader(int $payloadLength, bool $isSelfContained): string {
        $header = pack('VX', $payloadLength | ($isSelfContained ? 1 << 17 : 0));

        return $header . $this->encodeCrc24($header, 3);
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    private function writeFrame(string $outputData, bool $isSelfContained, int $dataOffset = 0, int $payloadLength = 0): void {
        if ($payloadLength < 1) {
            $payloadLength = strlen($outputData);
        }

        if ($payloadLength > self::PAYLOAD_MAX_SIZE) {
            throw new NodeException(
                message: 'Output data exceeds frame payload maximum size',
                code: ExceptionCode::NODE_PAYLOAD_EXCEEDS_MAX->value,
                context: [
                    'host' => $this->getConfig()->host,
                    'port' => $this->getConfig()->port,
                    'stage' => 'write_frame_size_check',
                    'payload_length' => $payloadLength,
                    'payload_max' => self::PAYLOAD_MAX_SIZE,
                ]
            );
        }

        $payload = substr($outputData, $dataOffset, $payloadLength);

        if ($this->compression) {
            // uncompressedLength == 0 signals to the peer that the payload is
            // stored uncompressed; we only compress when it actually shrinks
            // the payload, otherwise (e.g. incompressible data) we send it raw.
            $uncompressedLength = 0;

            if ($this->lz4Compressor !== null && $payloadLength > 0) {
                $compressed = $this->lz4Compressor->compressBlock($payload);
                $compressedLength = strlen($compressed);

                if ($compressedLength < $payloadLength) {
                    $uncompressedLength = $payloadLength;
                    $payload = $compressed;
                    $payloadLength = $compressedLength;
                }
            }

            $header = $this->encodeCompressedFrameHeader($payloadLength, $uncompressedLength, $isSelfContained);
        } else {
            $header = $this->encodeUncompressedFrameHeader($payloadLength, $isSelfContained);
        }

        $payloadCrc32Raw = $this->payloadCrc32($payload);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('N', $payloadCrc32Raw);
        if ($unpacked === false) {
            throw new NodeException(
                message: 'Failed to decode payload checksum',
                code: ExceptionCode::NODE_ENCODE_PAYLOAD_CRC32_FAILED->value,
                context: [
                    'host' => $this->getConfig()->host,
                    'port' => $this->getConfig()->port,
                    'stage' => 'encode_payload_crc32',
                    'compression' => $this->compression,
                    'payload_length' => $payloadLength,
                ]
            );
        }

        $payloadCrc32 = pack('V', $unpacked[1]);

        $this->node->write($header . $payload . $payloadCrc32);
    }
}
