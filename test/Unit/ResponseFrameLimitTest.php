<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\ResponseReader;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Request;

/**
 * Regression: a frame was buffered, or decompressed, towards whatever length its
 * header declared, up to 4 GiB.
 */
final class ResponseFrameLimitTest extends AbstractUnitTestCase {
    public function testABodyLongerThanTheProtocolAllowsIsRefused(): void {
        $this->assertRefused(pack('CCnCN', 0x84, 0, 1, 0x08, 1024 * 1024 * 1024));
    }

    public function testADecompressedBodyLongerThanTheProtocolAllowsIsRefused(): void {
        $body = pack('N', 1024 * 1024 * 1024) . "\x00";
        $this->assertRefused(pack('CCnCN', 0x84, Flag::COMPRESSION, 1, 0x08, strlen($body)) . $body);
    }

    private function assertRefused(string $bytes): void {
        $node = new class($bytes) extends NodeImplementation {
            public function __construct(private string $bytes) {
            }

            public function close(): void {
            }

            public function getConfig(): NodeConfig {
                return new StreamNodeConfig();
            }

            public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string {
                $chunk = substr($this->bytes, 0, $upperBoundaryLength);
                $this->bytes = substr($this->bytes, strlen($chunk));

                return $chunk;
            }

            public function write(string $data): void {
            }

            public function writeRequest(Request $request): void {
            }
        };

        try {
            (new ResponseReader())->readResponse($node, ProtocolVersion::V4, false);
            $this->fail('expected the frame to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_RESPONSE_BODY_TOO_LARGE->value, $e->getCode());
        }
    }
}
