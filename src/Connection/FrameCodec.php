<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Request\Request;

final class FrameCodec implements Node {
    final public const CRC24_INIT = 0x875060;
    final public const CRC24_POLYNOMIAL = 0x1974F0B;
    final public const PAYLOAD_MAX_SIZE = 131071;

    protected string $compression;

    protected string $crc32Prefix;

    protected string $inputData;
    protected int $inputDataLength;
    protected int $inputDataOffset;

    protected ?Lz4Decompressor $lz4Decompressor;

    protected NodeImplementation $node;

    /**
     * @throws \Cassandra\Connection\NodeException
     */
    public function __construct(NodeImplementation $node, string $compression = '') {
        if ($compression && $compression !== 'lz4') {
            throw new NodeException('Unsupported compression');
        }

        $this->crc32Prefix = pack('N', 0xFA2D55CA);

        if ($compression) {
            $this->lz4Decompressor = new Lz4Decompressor();
        } else {
            $this->lz4Decompressor = null;
        }

        $this->node = $node;
        $this->compression = $compression;

        $this->inputData = '';
        $this->inputDataOffset = 0;
        $this->inputDataLength = 0;
    }

    #[\Override]
    public function close(): void {
        $this->node->close();
    }

    /**
     * @return array{
     *  class: string,
     *  host: ?string,
     *  port: int,
     *  username: ?string,
     *  password: ?string,
     * } & array<string, mixed> $options
     */
    #[\Override]
    public function getOptions(): array {
        return $this->node->getOptions();
    }

    /**
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Compression\Exception
     */
    #[\Override]
    public function read(int $length): string {
        if ($this->inputDataOffset + $length > $this->inputDataLength) {
            $this->readFrame();
        }

        $inputData = substr($this->inputData, $this->inputDataOffset, $length);
        $this->inputDataOffset += $length;

        return $inputData;
    }

    /**
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Compression\Exception
     */
    #[\Override]
    public function readOnce(int $length): string {
        if ($this->inputDataOffset >= $this->inputDataLength) {
            $this->readFrame();
        }

        $length = min($length, $this->inputDataLength - $this->inputDataOffset);
        $inputData = substr($this->inputData, $this->inputDataOffset, $length);
        $this->inputDataOffset += $length;

        return $inputData;
    }

    /**
     * @throws \Cassandra\Connection\NodeException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $data = $request->__toString();
        $dataLength = strlen($data);

        if ($dataLength < self::PAYLOAD_MAX_SIZE) {
            $this->writeFrame($data, true, 0, $dataLength);
        } else {
            do {
                $offset = 0;
                $remainingDataLength = $dataLength;
                $writeDataLength = min(self::PAYLOAD_MAX_SIZE, $remainingDataLength);

                $this->writeFrame($data, false, $offset, $writeDataLength);

                $remainingDataLength -= $writeDataLength;
                $offset += $writeDataLength;
            } while ($remainingDataLength);
        }
    }

    protected function crc24(string $data, int $length = 0): int {
        $crc = self::CRC24_INIT;
        $len = $length > 0 ? $length : strlen($data);
        for ($i = 0; $i < $len; $i++) {
            $crc ^= ord($data[$i]) << 16;
            for ($j = 0; $j < 8; $j++) {
                $crc <<= 1;
                if ($crc & 0x1000000) {
                    $crc ^= self::CRC24_POLYNOMIAL;
                }
            }
        }

        return $crc & 0xFFFFFF;
    }

    /**
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Compression\Exception
     */
    protected function readFrame(): void {
        if ($this->compression) {
            $header = $this->node->read(8);
            $headerLength = 5;

            /** @var false|array<int> $unpacked */
            $unpacked = unpack('V2', $header);
        } else {
            $header = $this->node->read(6);
            $headerLength = 3;

            /** @var false|array<int> $unpacked */
            $unpacked = unpack('v3', $header);
        }

        if ($unpacked === false) {
            throw new NodeException('cannot decode frame data');
        }

        if ($this->compression) {
            $payloadLength = $unpacked[1] & 0x1FFFF;
            $uncompressedLength = (($unpacked[1] >> 17) & 0x7FFF) + (($unpacked[2] & 0x3) << 15);
            //$isSelfContained = $unpacked[2] & 0x4;
            $headerCrc24 = ($unpacked[2] >> 8) & 0xFFFFFF;
        } else {
            $payloadLength = $unpacked[1] + (($unpacked[2] & 0x1) << 16);
            $uncompressedLength = 0;
            //$isSelfContained = $unpacked[2] & 0x2;
            $headerCrc24 = (($unpacked[2] >> 8) & 0xFF) + ($unpacked[3] << 8);
        }

        if ($this->crc24($header, $headerLength) !== $headerCrc24) {
            throw new NodeException('invalid frame header checksum');
        }

        $payload = $this->node->read($payloadLength);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('V', $this->node->read(4));
        if ($unpacked === false) {
            throw new NodeException('cannot decode frame payload checksum');
        }
        $payloadCrc32 = $unpacked[1];

        $currentChecksum = hash('crc32b', $this->crc32Prefix . $payload, true);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('N', $currentChecksum);
        if ($unpacked === false) {
            throw new NodeException('cannot decode payload checksum');
        }
        if ($unpacked[1] !== $payloadCrc32) {
            throw new NodeException('invalid frame payload checksum');
        }

        if ($this->compression) {
            if ($this->lz4Decompressor === null) {
                throw new NodeException('decompressing frame payload failed - lz4 decompressor not initialized');
            }

            if ($uncompressedLength > 0) {
                $this->lz4Decompressor->setInput($payload, 0, $payloadLength);

                $payload = $this->lz4Decompressor->decompressBlock();
                $payloadLength = strlen($payload);

                if ($payloadLength !== $uncompressedLength) {
                    throw new NodeException('decompressing frame payload failed - invalid uncompressed length');
                }
            }
        }

        if ($this->inputDataOffset < $this->inputDataLength) {
            $length = $this->inputDataLength - $this->inputDataOffset;
            $this->inputData = substr($this->inputData, $this->inputDataOffset, $length) . $payload;
            $this->inputDataOffset = 0;
            $this->inputDataLength = $length + $payloadLength;
        } else {
            $this->inputData = $payload;
            $this->inputDataOffset = 0;
            $this->inputDataLength = $payloadLength;
        }
    }

    /**
     * @throws \Cassandra\Connection\NodeException
     */
    protected function writeFrame(string $outputData, bool $isSelfContained, int $dataOffset = 0, int $payloadLength = 0): void {
        if ($payloadLength < 1) {
            $payloadLength = strlen($outputData);
        }

        if ($payloadLength > self::PAYLOAD_MAX_SIZE) {
            throw new NodeException('output data does not fit into a frame');
        }

        if ($this->compression) {
            $header = pack(
                'VC',
                $payloadLength,
                $isSelfContained ? 1 << 2 : 0,
            );
            $header .= pack('VX', $this->crc24($header, 5));
        } else {
            $header = pack('VX', $payloadLength + ($isSelfContained ? 1 << 17 : 0));
            $header .= pack('VX', $this->crc24($header, 3));
        }

        $payload = substr($outputData, $dataOffset, $payloadLength);

        $payloadCrc32Raw = hash('crc32b', $this->crc32Prefix . $payload, true);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('N', $payloadCrc32Raw);
        if ($unpacked === false) {
            throw new NodeException('cannot decode payload checksum');
        }

        $payloadCrc32 = pack('V', $unpacked[1]);

        $this->node->write($header . $payload . $payloadCrc32);
    }
}
