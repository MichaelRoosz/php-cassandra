<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\Node;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\ResponseReader;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Request;
use ReflectionProperty;

/**
 * A response the reader could not finish belongs to the connection it was read
 * from, and goes away with it.
 *
 * The reader deliberately keeps the header of a frame whose body has not
 * arrived yet, so that a bounded read can pick the same frame up again — and a
 * bounded read coming back mid-frame is ordinary, since the header and the body
 * share one absolute deadline. The bytes it is still waiting for live in that
 * transport's receive buffer, though, so a header carried over to the next
 * connection would be completed with the first bytes that connection sends, and
 * every response after it would be read at the wrong offset.
 */
final class ResponseReaderResetTest extends AbstractUnitTestCase {
    public function testAHalfReadFrameIsDroppedWithTheConnectionItCameFrom(): void {
        $reader = new ResponseReader();

        // A header announcing a body that never arrives.
        $truncated = new FakeReadOnlyNode(self::truncatedFrame());

        $this->assertNull(
            $reader->readResponse($truncated, ProtocolVersion::V4, Node::DO_NOT_WAIT),
            'the body is missing, so there is no response yet'
        );

        $reader->reset();

        // A brand new connection whose first frame is a complete READY.
        $fresh = new FakeReadOnlyNode(self::readyFrame(stream: 5));

        $response = $reader->readResponse($fresh, ProtocolVersion::V4, Node::DO_NOT_WAIT);

        $this->assertNotNull($response, 'the fresh connection is read from its first byte');
        $this->assertSame(5, $response->getStream());
    }

    public function testDisconnectDropsAHalfReadFrame(): void {
        $connection = new Connection([new SocketNodeConfig(host: '127.0.0.1')]);

        $readerProperty = new ReflectionProperty(Connection::class, 'responseReader');
        $reader = $readerProperty->getValue($connection);
        $this->assertInstanceOf(ResponseReader::class, $reader);

        $headerProperty = new ReflectionProperty(ResponseReader::class, 'currentHeader');
        $reader->readResponse(
            new FakeReadOnlyNode(self::truncatedFrame()),
            ProtocolVersion::V4,
            Node::DO_NOT_WAIT,
        );
        $this->assertNotNull($headerProperty->getValue($reader), 'the header is kept for the next read');

        // Nothing was ever connected, so this only exercises the bookkeeping —
        // which is the point: every path that drops a connection goes through it.
        $connection->disconnect();

        $this->assertNull(
            $headerProperty->getValue($reader),
            'the half-read frame belongs to the connection that was just dropped'
        );
    }

    private static function header(Opcode $opcode, int $stream, int $bodyLength): string {
        return "\x84\x00" . pack('n', $stream) . chr($opcode->value) . pack('N', $bodyLength);
    }

    /**
     * A complete v4 READY frame, which carries no body at all.
     */
    private static function readyFrame(int $stream): string {
        return self::header(Opcode::RESPONSE_READY, $stream, bodyLength: 0);
    }

    /**
     * A v4 RESULT header announcing a ten-byte body that is not supplied, so
     * that reading the frame can never complete.
     */
    private static function truncatedFrame(): string {
        return self::header(Opcode::RESPONSE_RESULT, stream: 7, bodyLength: 10);
    }
}

/**
 * A node that replays a fixed buffer and, like the real transports, returns an
 * empty string rather than a short read when it cannot satisfy the request.
 */
final class FakeReadOnlyNode implements Node {
    public function __construct(private string $buffer) {
    }

    public function close(): void {
    }

    public function getConfig(): NodeConfig {
        return new SocketNodeConfig(host: '127.0.0.1');
    }

    public function read(int $length, ?float $readDeadline): string {
        if (strlen($this->buffer) < $length) {
            return '';
        }

        $data = substr($this->buffer, 0, $length);
        $this->buffer = substr($this->buffer, $length);

        return $data;
    }

    public function readAvailableData(int $expectedLength, int $maxLength, ?float $readDeadline): string {
        return $this->read(min($maxLength, strlen($this->buffer)), $readDeadline);
    }

    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        return $this->read($expectedLength, $readDeadline);
    }

    public function write(string $data): void {
    }

    public function writeRequest(Request $request): void {
    }
}
