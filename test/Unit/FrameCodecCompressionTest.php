<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\FrameCodec;
use Cassandra\Connection\Node;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Request\Request;

/**
 * In-memory {@see Node} that feeds everything written to it straight back on
 * read, so a {@see FrameCodec} can be exercised against itself.
 */
final class LoopbackNode extends NodeImplementation {
    private string $buffer = '';

    private NodeConfig $config;

    private int $offset = 0;

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
        $available = strlen($this->buffer) - $this->offset;
        if ($available < 1) {
            return '';
        }

        $returnLength = min($upperBoundaryLength, $available);
        $data = substr($this->buffer, $this->offset, $returnLength);
        $this->offset += $returnLength;

        return $data;
    }

    #[\Override]
    public function write(string $data): void {
        $this->buffer .= $data;
    }

    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }
}

class FrameCodecCompressionTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{0: string, 1: string}>
     */
    public static function payloadProvider(): array {
        return [
            'small self-contained' => ['lz4', 'SELECT key FROM system.local'],
            'highly compressible' => ['lz4', str_repeat('INSERT INTO ks.kv (id, v) VALUES (1, 2);', 3000)],
            'incompressible fallback' => ['lz4', random_bytes(70000)],
            'larger than one frame' => ['lz4', str_repeat('The quick brown fox. ', 20000)],
            'uncompressed codec' => ['', str_repeat('SELECT * FROM ks.tbl;', 10000)],
        ];
    }

    /**
     * @dataProvider payloadProvider
     */
    public function testWriteThenReadRoundTrip(string $compression, string $payload): void {
        // A single codec over a loopback node encodes on write and decodes what
        // it produced on read, so the on-wire frame format (header bit-packing,
        // CRCs and, for lz4, the compressed payload) must be self-consistent.
        $codec = new FrameCodec(new LoopbackNode(), $compression);

        $request = new PlainBodyRequest($payload);
        $request->setStream(7);
        $expected = $request->__toString();

        $codec->writeRequest($request);

        $received = '';
        $expectedLength = strlen($expected);
        while (strlen($received) < $expectedLength) {
            $chunk = $codec->readAvailableDataFromSource($expectedLength, $expectedLength, true);
            if ($chunk === '') {
                break;
            }
            $received .= $chunk;
        }

        $this->assertSame($expected, $received);
    }
}

/**
 * Minimal request whose frame body is an arbitrary string, so we can round-trip
 * payloads of any size and content through the {@see FrameCodec}.
 */
final class PlainBodyRequest extends Request {
    public function __construct(private string $bodyData) {
        parent::__construct(\Cassandra\Protocol\Opcode::REQUEST_QUERY);
    }

    #[\Override]
    public function getBody(): string {
        return $this->bodyData;
    }
}
