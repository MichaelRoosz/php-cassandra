<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\Session;
use Cassandra\Connection\ConnectionOptions;
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

        $readerProperty = new ReflectionProperty(Session::class, 'responseReader');
        $reader = $readerProperty->getValue(self::sessionOf($connection));
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

    public function testDisconnectDropsTheNegotiatedProtocolVersion(): void {
        // The negotiated version belongs to the connection, not to this object:
        // the next one may be a different node, picked by the selector or
        // reached after this one failed. Kept, it would open the next handshake
        // at a version that node never agreed to, and a node that cannot answer
        // there is read as a protocol mismatch and fails outright — which is
        // exactly what starting from initialProtocolVersion avoids.
        $connection = new Connection(
            [new SocketNodeConfig(host: '127.0.0.1')],
            options: new ConnectionOptions(initialProtocolVersion: ProtocolVersion::V4),
        );

        $versionProperty = new ReflectionProperty(Session::class, 'version');
        $versionProperty->setValue(self::sessionOf($connection), ProtocolVersion::V5);

        $this->assertSame(ProtocolVersion::V5, $connection->getProtocolVersion());

        $connection->disconnect();

        $this->assertSame(
            ProtocolVersion::V4,
            $connection->getProtocolVersion(),
            'a disconnected connection reports the initial version again, as getProtocolVersion() documents'
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
    private int $receivedByteCount = 0;

    public function __construct(private string $buffer) {
    }

    public function close(): void {
    }

    public function getConfig(): NodeConfig {
        return new SocketNodeConfig(host: '127.0.0.1');
    }

    public function getReceivedByteCount(): int {
        return $this->receivedByteCount;
    }

    public function read(int $length, ?float $readDeadline): string {
        if (strlen($this->buffer) < $length) {
            return '';
        }

        $data = substr($this->buffer, 0, $length);
        $this->buffer = substr($this->buffer, $length);
        $this->receivedByteCount += $length;

        return $data;
    }

    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        return $this->read($expectedLength, $readDeadline);
    }

    public function write(string $data): void {
    }

    public function writeRequest(Request $request): void {
    }
}
