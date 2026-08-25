<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\Node;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\ResponseReader;
use Cassandra\Connection\Session;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamIdPool;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\TypeNameParserException;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Query;
use Cassandra\Request\Request;
use Cassandra\Statement;
use ReflectionMethod;
use ReflectionProperty;

/**
 * A RESULT frame whose column metadata the type layer refuses is one bad
 * answer, not a bad connection: the frame was consumed whole, so the stream is
 * still in step and only the request it belonged to is lost.
 *
 * Regression: the response classes are constructed from a class name only known
 * at run time, and a Result reads its metadata in that constructor — so it can
 * raise the type layer's own exceptions, which are siblings of ResponseException
 * rather than subclasses of it. {@see Session::readResponse()} catches the
 * reader's declared contract to finish the request a refused frame belonged to,
 * so those went straight past it: the statement stayed registered and un-given
 * up on, holding a stream id nothing could ever release, and a caller waiting
 * without a request timeout waited for an answer that had already come and gone.
 */
final class MalformedResponseMetadataTest extends AbstractUnitTestCase {
    public function testAnAsyncStatementIsFinishedAndItsStreamReleased(): void {
        $connection = new Connection(
            [new SocketNodeConfig(host: '127.0.0.1')],
            // Nothing bounds the wait, so a statement left pending here could
            // never be finished by anything else.
            options: new ConnectionOptions(requestTimeoutInSeconds: null),
        );

        $streamIds = $this->streamIdPoolOf($connection);
        $streamId = $streamIds->claim();
        $this->assertNotNull($streamId);

        $statement = new Statement(
            connection: $connection,
            streamId: $streamId,
            streamGeneration: $streamIds->getGeneration(),
            request: new Query('SELECT * FROM t'),
        );
        self::statementsOf($connection)->register($streamId, $statement);

        $session = self::sessionOf($connection);
        (new ReflectionProperty(Session::class, 'node'))
            ->setValue($session, new FakeSingleFrameNode(self::unreadableMetadataFrame($streamId)));

        $drainedResponses = false;

        try {
            (new ReflectionMethod(Session::class, 'readResponse'))->invokeArgs($session, [&$drainedResponses]);
            $this->fail('expected the unreadable metadata to be reported');
        } catch (ResponseException $e) {
            $this->assertSame(ExceptionCode::RESPONSE_DECODE_FAILED->value, $e->getCode());
        }

        $this->assertTrue($statement->isAbandoned(), 'the refused frame was the statement answer, so it cannot remain pending');
        $this->assertSame(0, self::statementsOf($connection)->getCount());
        $this->assertSame([], $this->outstandingStreamsOf($connection));
        $this->assertSame([], $this->orphanedStreamsOf($connection), 'the complete answer was consumed, so its id need not be parked');
    }

    public function testTheReaderReportsItAsADecodeFailureAndKeepsFrameSync(): void {
        $reader = new ResponseReader();
        $node = new FakeSingleFrameNode(self::unreadableMetadataFrame(streamId: 7));

        try {
            $reader->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT);
            $this->fail('expected the unreadable metadata to be reported');
        } catch (ResponseException $e) {
            $this->assertSame(ExceptionCode::RESPONSE_DECODE_FAILED->value, $e->getCode());

            // The cause stays reachable, so what the type layer objected to is
            // not lost by being reported as the decode failure it is.
            $this->assertInstanceOf(TypeNameParserException::class, $e->getPrevious());
        }

        $this->assertFalse($reader->hasLostFrameSync(), 'the whole frame was consumed, so the connection is still usable');

        $header = $reader->takeFailedResponseHeader();
        $this->assertNotNull($header, 'the session needs the stream id to finish the request the frame belonged to');
        $this->assertSame(7, $header->stream);
    }

    /**
     * @return array<int, float>
     */
    private function orphanedStreamsOf(Connection $connection): array {
        /** @var array<int, float> $orphaned */
        $orphaned = (new ReflectionProperty(StreamIdPool::class, 'orphanedStreams'))->getValue($this->streamIdPoolOf($connection));

        return $orphaned;
    }

    /**
     * @return array<int>
     */
    private function outstandingStreamsOf(Connection $connection): array {
        /** @var array<int, true> $outstanding */
        $outstanding = (new ReflectionProperty(StreamIdPool::class, 'outstanding'))->getValue($this->streamIdPoolOf($connection));

        return array_keys($outstanding);
    }

    private function streamIdPoolOf(Connection $connection): StreamIdPool {
        /** @var StreamIdPool $pool */
        $pool = (new ReflectionProperty(Session::class, 'streamIds'))->getValue(self::sessionOf($connection));

        return $pool;
    }

    /**
     * A complete v4 RESULT/Rows frame declaring one column whose type is a
     * custom name the type parser will not read — here a vector wider than
     * {@see \Cassandra\TypeInfo\VectorInfo::MAX_DIMENSIONS}. Nothing is wrong
     * with the framing, so the frame is consumed in full and only making sense
     * of it fails.
     */
    private static function unreadableMetadataFrame(int $streamId): string {
        $customType = 'org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 20000)';

        $body = pack('N', 2)                                  // result kind: Rows
            . pack('N', 0x0001)                               // flags: GLOBAL_TABLES_SPEC
            . pack('N', 1)                                    // column count
            . pack('n', 2) . 'ks'
            . pack('n', 2) . 'tb'
            . pack('n', 1) . 'c'
            . pack('n', 0x0000)                               // column type: custom
            . pack('n', strlen($customType)) . $customType
            . pack('N', 0);                                   // row count

        return pack('CCnCN', 0x84, 0, $streamId, Opcode::RESPONSE_RESULT->value, strlen($body)) . $body;
    }
}

/**
 * A node that replays one frame and then reports an idle connection, as the
 * real transports do when they cannot satisfy a read.
 */
final class FakeSingleFrameNode implements Node {
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
