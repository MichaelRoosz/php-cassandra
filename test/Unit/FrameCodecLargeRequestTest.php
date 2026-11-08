<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\FrameCodec;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Query;
use Cassandra\Request\Request;
use LengthException;

final class FrameCodecLargeRequestTest extends AbstractUnitTestCase {
    public function testARequestLargerThanOneSegmentIsSplitAcrossSegments(): void {
        // Regression: the split loop reset its offset every pass, writing the first segment forever.
        $node = new class() extends NodeImplementation {
            public string $written = '';

            public function close(): void {
            }

            public function getConfig(): NodeConfig {
                return new StreamNodeConfig();
            }

            public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string {
                return '';
            }

            public function write(string $data): void {
                $this->written .= $data;
                if (strlen($this->written) > 1_000_000) {
                    throw new LengthException('the request is being written forever');
                }
            }

            public function writeRequest(Request $request): void {
            }
        };

        $request = new Query('SELECT * FROM t WHERE k = \'' . str_repeat('x', 300_000) . '\'');
        $request->setVersion(ProtocolVersion::V5);
        $request->setStream(1);
        (new FrameCodec($node, ''))->writeRequest($request);

        $payload = '';
        $segments = 0;
        for ($offset = 0; $offset < strlen($node->written); ++$segments) {
            $header = unpack('V', substr($node->written, $offset, 3) . "\0");
            $this->assertIsArray($header);
            $this->assertIsInt($header[1]);
            $length = $header[1] & 0x1FFFF;
            $this->assertSame(0, $header[1] & 0x20000, 'a split request is not self-contained');
            $payload .= substr($node->written, $offset + 6, $length);
            $offset += 6 + $length + 4;
        }

        $this->assertSame(3, $segments);
        $this->assertSame((string) $request, $payload);
    }
}
