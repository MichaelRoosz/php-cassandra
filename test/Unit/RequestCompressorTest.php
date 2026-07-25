<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\RequestCompressor;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Exception\NodeException;
use Cassandra\Protocol\Flag;
use Cassandra\Request\Options;
use Cassandra\Request\Query;
use Cassandra\Request\Request;

/**
 * Node that records every byte written to it, so tests can inspect the exact
 * frame the {@see RequestCompressor} produced.
 */
final class CapturingNode extends NodeImplementation {
    public string $written = '';

    private NodeConfig $config;

    public function __construct() {
        $this->config = new SocketNodeConfig(host: '127.0.0.1', port: 9042);
    }

    #[\Override]
    public function close(): void {
    }

    #[\Override]
    public function getConfig(): NodeConfig {
        return $this->config;
    }

    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string {
        return '';
    }

    #[\Override]
    public function write(string $data): void {
        $this->written .= $data;
    }

    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }
}

class RequestCompressorTest extends AbstractUnitTestCase {
    public function testCompressibleRequestIsCompressedAndDecodesBackToOriginalBody(): void {
        $capture = new CapturingNode();
        $compressor = new RequestCompressor($capture, 'lz4');

        $request = new Query(str_repeat('SELECT * FROM ks.tbl WHERE id = 1; ', 500), [], Consistency::ONE);
        $request->setStream(7);

        $uncompressedFrame = $request->__toString();
        $uncompressedBody = substr($uncompressedFrame, 9);

        $compressor->writeRequest($request);
        $written = $capture->written;

        // Header: version preserved, COMPRESSION flag set, stream+opcode preserved.
        $this->assertSame($uncompressedFrame[0], $written[0]);
        $this->assertSame(Flag::COMPRESSION, ord($written[1]) & Flag::COMPRESSION);
        $this->assertSame(substr($uncompressedFrame, 2, 3), substr($written, 2, 3));

        // The compressed frame is genuinely smaller.
        $this->assertLessThan(strlen($uncompressedFrame), strlen($written));

        $compressedBody = substr($written, 9);

        // Declared frame length matches the actual compressed body length.
        /** @var array<int> $lengthField */
        $lengthField = unpack('N', substr($written, 5, 4));
        $this->assertSame(strlen($compressedBody), $lengthField[1]);

        // Body starts with the big-endian uncompressed length ...
        /** @var array<int> $uncompressedLengthField */
        $uncompressedLengthField = unpack('N', substr($compressedBody, 0, 4));
        $this->assertSame(strlen($uncompressedBody), $uncompressedLengthField[1]);

        // ... followed by an LZ4 block that decodes back to the original body.
        $decompressor = new Lz4Decompressor();
        $decompressor->setInput(substr($compressedBody, 4));
        $this->assertSame($uncompressedBody, $decompressor->decompressBlock());
    }

    public function testConstructorRejectsUnsupportedAlgorithm(): void {
        $this->expectException(NodeException::class);

        new RequestCompressor(new CapturingNode(), 'snappy');
    }

    public function testEmptyBodyRequestIsWrittenUnchanged(): void {
        $capture = new CapturingNode();
        $compressor = new RequestCompressor($capture, 'lz4');

        $request = new Options();
        $request->setStream(7);
        $expected = $request->__toString();

        $compressor->writeRequest($request);

        $this->assertSame($expected, $capture->written);
        $this->assertSame(0, ord($capture->written[1]) & Flag::COMPRESSION);
    }

    public function testIncompressibleRequestIsSentUncompressed(): void {
        $capture = new CapturingNode();
        $compressor = new RequestCompressor($capture, 'lz4');

        // Random bytes are incompressible, so LZ4 would expand them; the
        // compressor must fall back to sending the frame unchanged.
        $request = new Query(random_bytes(4096), [], Consistency::ONE);
        $request->setStream(7);
        $expected = $request->__toString();

        $compressor->writeRequest($request);

        $this->assertSame($expected, $capture->written);
        $this->assertSame(0, ord($capture->written[1]) & Flag::COMPRESSION);
    }
}
