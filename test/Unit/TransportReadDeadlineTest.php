<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\IoNode;
use Cassandra\Connection\Node;
use Cassandra\Connection\Socket;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\NodeException;
use ReflectionProperty;

/**
 * Both transports honour the read deadline they are handed, independently of
 * their own stall window.
 *
 * The two answer different questions and must not be able to mask each other: a
 * deadline says how long the *caller* is prepared to wait and says nothing about
 * the connection, so reaching it comes back empty-handed; a stall window says
 * the connection itself has made no progress for too long, which is a transport
 * failure and raises. These are driven against a real socket that accepts and
 * then says nothing, since what is being tested is the blocking behaviour of
 * select() rather than any logic above it.
 */
final class TransportReadDeadlineTest extends AbstractUnitTestCase {
    /**
     * Upper bound for a read that was meant to end at its 0.3s deadline rather
     * than at the 15s stall window behind it.
     *
     * What is being told apart is 0.3s from 15s, so anything between the two
     * discriminates. It sits well above the deadline because these tests share a
     * machine with whatever else is running: a scheduler that does not come back
     * to the process for a couple of seconds says nothing about the deadline
     * logic, and a bound tight enough to catch that is a bound that fails under
     * load rather than on a bug.
     */
    private const DEADLINE_NOT_STALL_WINDOW = 10.0;

    /** @var ?resource $acceptedClient */
    private $acceptedClient = null;

    private ?IoNode $node = null;

    /** @var ?resource $server */
    private $server = null;

    protected function tearDown(): void {
        $this->node?->close();
        $this->node = null;

        if (is_resource($this->acceptedClient)) {
            fclose($this->acceptedClient);
        }
        $this->acceptedClient = null;

        if (is_resource($this->server)) {
            fclose($this->server);
        }
        $this->server = null;
    }

    public function testSocketDeadlineBeatsALongStallWindow(): void {
        $node = $this->connectSocket(['sec' => 15, 'usec' => 0]);

        [$data, $elapsed] = $this->timedRead($node, microtime(true) + 0.3);

        $this->assertSame('', $data, 'nothing arrived, and that is not a failure');
        $this->assertGreaterThan(0.2, $elapsed, 'it waited the deadline out');
        $this->assertLessThan(self::DEADLINE_NOT_STALL_WINDOW, $elapsed, 'and not the 15s stall window');
    }

    public function testSocketDeadlineIsHonouredWithNoStallWindowAtAll(): void {
        // ['sec' => 0, 'usec' => 0] disables SO_RCVTIMEO, which used to mean an
        // unbounded read. The deadline is now what ends it.
        $node = $this->connectSocket(['sec' => 0, 'usec' => 0]);

        [$data, $elapsed] = $this->timedRead($node, microtime(true) + 0.3);

        $this->assertSame('', $data);
        $this->assertGreaterThan(0.2, $elapsed);
        $this->assertLessThan(self::DEADLINE_NOT_STALL_WINDOW, $elapsed);
    }

    public function testSocketPastDeadlineDoesNotWaitAtAll(): void {
        $node = $this->connectSocket(['sec' => 15, 'usec' => 0]);

        [$data, $elapsed] = $this->timedRead($node, Node::DO_NOT_WAIT);

        $this->assertSame('', $data);
        $this->assertLessThan(0.2, $elapsed, 'a deadline already past buys no wait');
    }

    public function testSocketServesAPastDeadlineReadInBlockingFallbackMode(): void {
        // When the socket cannot be switched to non-blocking mode, a read with a
        // deadline already past cannot simply be skipped: that is what the
        // polling calls make, so skipping it would leave them reporting an idle
        // connection whatever the server sent. Readiness is settled with a
        // zero-timeout select() instead, and the data is served without waiting.
        $node = $this->connectSocket(['sec' => 15, 'usec' => 0]);
        $this->forceBlockingIo($node);

        $this->sendFromServer('123456789');

        [$data, $elapsed] = $this->timedRead($node, Node::DO_NOT_WAIT);

        $this->assertSame('123456789', $data, 'a blocking socket must still yield what has already arrived');
        $this->assertLessThan(1.0, $elapsed, 'and must not have waited for it');
    }

    public function testSocketStallWindowStillRaisesWithoutADeadline(): void {
        // The deadline did not replace the stall window: with no deadline to go
        // by, a connection that stays silent for its whole window is still a
        // transport failure rather than a quiet "nothing arrived".
        $node = $this->connectSocket(['sec' => 0, 'usec' => 300000]);

        $this->expectException(NodeException::class);

        $node->readAvailableDataFromSource(9, 9, readDeadline: null);
    }

    public function testStreamDeadlineBeatsALongStallWindow(): void {
        $node = $this->connectStream(15.0);

        [$data, $elapsed] = $this->timedRead($node, microtime(true) + 0.3);

        $this->assertSame('', $data);
        $this->assertGreaterThan(0.2, $elapsed);
        $this->assertLessThan(self::DEADLINE_NOT_STALL_WINDOW, $elapsed);
    }

    public function testStreamDeadlineIsHonouredWithNoStallWindowAtAll(): void {
        $node = $this->connectStream(0.0);

        [$data, $elapsed] = $this->timedRead($node, microtime(true) + 0.3);

        $this->assertSame('', $data);
        $this->assertGreaterThan(0.2, $elapsed);
        $this->assertLessThan(self::DEADLINE_NOT_STALL_WINDOW, $elapsed);
    }

    public function testStreamPastDeadlineDoesNotWaitAtAll(): void {
        $node = $this->connectStream(15.0);

        [$data, $elapsed] = $this->timedRead($node, Node::DO_NOT_WAIT);

        $this->assertSame('', $data);
        $this->assertLessThan(0.2, $elapsed);
    }

    public function testStreamServesAPastDeadlineReadInBlockingFallbackMode(): void {
        // The stream counterpart of
        // testSocketServesAPastDeadlineReadInBlockingFallbackMode().
        $node = $this->connectStream(15.0);
        $this->forceBlockingIo($node);

        $this->sendFromServer('123456789');

        [$data, $elapsed] = $this->timedRead($node, Node::DO_NOT_WAIT);

        $this->assertSame('123456789', $data, 'a blocking stream must still yield what has already arrived');
        $this->assertLessThan(1.0, $elapsed, 'and must not have waited for it');
    }

    public function testStreamStallWindowStillRaisesWithoutADeadline(): void {
        $node = $this->connectStream(0.3);

        $this->expectException(NodeException::class);

        $node->readAvailableDataFromSource(9, 9, readDeadline: null);
    }

    /**
     * Accept the connection and then say nothing at all, which is what a
     * coordinator still working on an answer looks like at the socket.
     */
    private function acceptSilently(): void {
        if (!is_resource($this->server)) {
            return;
        }

        $client = @stream_socket_accept($this->server, 5);
        if ($client !== false) {
            $this->acceptedClient = $client;
        }
    }

    /**
     * @param array{sec: int, usec: int} $receiveTimeout
     */
    private function connectSocket(array $receiveTimeout): IoNode {
        $port = $this->listen();

        $node = new Socket(new SocketNodeConfig(
            host: '127.0.0.1',
            port: $port,
            socketOptions: [
                SO_RCVTIMEO => $receiveTimeout,
                SO_SNDTIMEO => ['sec' => 5, 'usec' => 0],
            ],
        ));

        $node->connect();
        $this->node = $node;

        $this->acceptSilently();

        return $node;
    }

    private function connectStream(float $timeoutInSeconds): IoNode {
        $port = $this->listen();

        $node = new Stream(new StreamNodeConfig(
            host: '127.0.0.1',
            port: $port,
            timeoutInSeconds: $timeoutInSeconds,
        ));

        $node->connect();
        $this->node = $node;

        $this->acceptSilently();

        return $node;
    }

    /**
     * Pretend the transport could not be switched out of blocking mode, which
     * is the fallback both of them carry and which no local socket takes on its
     * own.
     */
    private function forceBlockingIo(IoNode $node): void {
        (new ReflectionProperty($node::class, 'isBlockingIo'))->setValue($node, true);
    }

    private function listen(): int {
        $server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        if ($server === false) {
            $this->fail('could not listen on a local port: ' . (string) $errorMessage);
        }

        $this->server = $server;

        $localName = stream_socket_get_name($server, false);
        if ($localName === false) {
            $this->fail('could not determine the listening port');
        }

        return (int) substr($localName, (int) strrpos($localName, ':') + 1);
    }

    /**
     * Push data down the accepted connection and give it a moment to arrive, so
     * that a read which refuses to wait still has something to find.
     */
    private function sendFromServer(string $data): void {
        if (!is_resource($this->acceptedClient)) {
            $this->fail('the server never accepted the connection');
        }

        fwrite($this->acceptedClient, $data);
        fflush($this->acceptedClient);

        self::sleepAtLeast(0.2);
    }

    /**
     * @return array{string, float}
     *
     * @throws \Cassandra\Exception\NodeException
     */
    private function timedRead(IoNode $node, ?float $readDeadline): array {
        $start = microtime(true);
        $data = $node->readAvailableDataFromSource(9, 9, $readDeadline);

        return [$data, microtime(true) - $start];
    }
}
