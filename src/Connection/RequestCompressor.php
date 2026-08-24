<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Compression\Lz4Compressor;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Protocol\Flag;
use Cassandra\Request\Request;

/**
 * Node decorator that LZ4-compresses outgoing request frames on the legacy
 * (protocol v3/v4) framing.
 *
 * Protocol v5 handles compression inside {@see FrameCodec}; on v3/v4 there is no
 * outer framing, so compression is applied per request frame: the frame body is
 * replaced with a 4-byte big-endian uncompressed length followed by a raw LZ4
 * block, and the COMPRESSION flag is set in the frame header. This mirrors the
 * format {@see ResponseReader} already understands when decompressing responses.
 *
 * Reads pass straight through to the wrapped node; response decompression stays
 * in {@see ResponseReader}.
 */
final class RequestCompressor extends NodeImplementation {
    /**
     * Byte offset of the frame body within the serialized request frame
     * produced by {@see Request::__toString()}: version(1) + flags(1) +
     * stream(2) + opcode(1) + length(4).
     */
    private const FRAME_HEADER_LENGTH = 9;

    private Lz4Compressor $lz4Compressor;

    private Node $node;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function __construct(Node $node, string $compression) {
        if ($compression !== 'lz4') {
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

        $this->node = $node;
        $this->lz4Compressor = new Lz4Compressor();
    }

    #[\Override]
    public function close(): void {
        $this->node->close();
    }

    #[\Override]
    public function getConfig(): NodeConfig {
        return $this->node->getConfig();
    }
    #[\Override]
    public function getReceivedByteCount(): int {
        return $this->node->getReceivedByteCount();
    }

    /**
     * Reads are handed to the wrapped node whole, rather than being reassembled
     * out of {@see NodeImplementation}'s buffer here.
     *
     * This decorator only rewrites what goes out, so there is nothing for it to
     * buffer — and buffering here anyway would strand whatever the wrapped node
     * had already read into its own buffer. It is installed part way through the
     * handshake ({@see Session::completeHandshake()}), by which
     * point that buffer may hold anything the node sent past the frame the
     * handshake was reading, and those bytes would then never be seen again,
     * leaving every response after them parsed at the wrong offset.
     * {@see FrameCodec} keeps a buffer of its own because it re-frames what it
     * reads, but it too takes its input through the wrapped node's read().
     *
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\CompressionException
     */
    #[\Override]
    public function read(int $length, ?float $readDeadline): string {
        return $this->node->read($length, $readDeadline);
    }

    /**
     * Never used by {@see self::read()}, which delegates whole; kept because the
     * interface requires it and a caller reaching past read() should still get
     * the wrapped node's data rather than nothing.
     *
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        return $this->node->readAvailableDataFromSource($expectedLength, $upperBoundaryLength, $readDeadline);
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
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $frame = $request->__toString();

        $body = substr($frame, self::FRAME_HEADER_LENGTH);
        if ($body === '') {
            $this->node->write($frame);

            return;
        }

        $uncompressedLength = strlen($body);
        $compressedBody = pack('N', $uncompressedLength) . $this->lz4Compressor->compressBlock($body);

        // Only send compressed when it actually shrinks the frame; incompressible
        // payloads are sent as-is (without the COMPRESSION flag).
        if (strlen($compressedBody) >= $uncompressedLength) {
            $this->node->write($frame);

            return;
        }

        $flags = (ord($frame[1]) | Flag::COMPRESSION) & 0xFF;

        $header = $frame[0]                       // protocol version
            . chr($flags)                         // flags (+ COMPRESSION)
            . substr($frame, 2, 3)                // stream id (2) + opcode (1)
            . pack('N', strlen($compressedBody)); // new body length

        $this->node->write($header . $compressedBody);
    }
}
