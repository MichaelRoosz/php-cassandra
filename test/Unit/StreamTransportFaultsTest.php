<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\Clock;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\StreamException;
use Cassandra\Request\Query;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Test\Support\Faults;
use Closure;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use RuntimeException;

/**
 * The stream transport against a real local socket, with the PHP stream
 * functions made to fail the way a broken network or PHP build would.
 */
final class StreamTransportFaultsTest extends AbstractUnitTestCase {
    /** @var ?resource */
    private $peer = null;

    /** @var ?resource */
    private $server = null;

    protected function tearDown(): void {
        Faults::reset();
        foreach ([$this->peer, $this->server] as $handle) {
            if (is_resource($handle)) {
                fclose($handle);
            }
        }
        parent::tearDown();
    }

    public function testAClockReadingPastTheStallWindowIsCheckedAgain(): void {
        $node = $this->connected();

        fwrite($this->peerStream(), 'a');
        self::jumpClockInside('selectStreamForRead');
        $this->assertSame('a', $node->readAvailableDataFromSource(1, 1, null));

        self::jumpClockInside('selectStreamForWrite');
        $node->write('x');
        $this->assertSame('x', fread($this->peerStream(), 1));
    }

    public function testAReadTimingOutOnTheStallWindowIsTheTransportsFailure(): void {
        Faults::inject('stream_set_blocking', false);
        $node = $this->connected();

        Faults::inject('fread', false);
        Faults::inject('feof', false);
        Faults::inject('stream_get_meta_data', static fn (mixed $stream): array => ['timed_out' => true] + (is_resource($stream) ? \stream_get_meta_data($stream) : []));

        try {
            $node->readAvailableDataFromSource(1, 1, null);
            $this->fail('expected the read to time out');
        } catch (StreamException $e) {
            $this->assertSame(\Cassandra\Exception\ExceptionCode::STREAM_TIMEOUT_DURING_READ->value, $e->getCode());
        }
    }

    public function testBlockingReadsAtTheEdgeOfTheirDeadline(): void {
        Faults::inject('stream_set_blocking', false);
        $node = $this->connected();

        // Too little of the deadline left to express as a stream timeout, and none at all.
        $deadline = Clock::now() + 1.0;
        self::clockInsideAt('applyReceiveTimeout', $deadline - 0.0000002);
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, $deadline));
        self::clockInsideAt('applyReceiveTimeout', $deadline + 1.0);
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, $deadline));

        // The same timeout twice is applied once.
        fwrite($this->peerStream(), 'ab');
        $this->assertSame('a', $node->readAvailableDataFromSource(1, 1, null));
        $this->assertSame('b', $node->readAvailableDataFromSource(1, 1, null));
    }

    public function testConnectFailures(): void {
        // A bare IPv6 literal is bracketed before the port is appended.
        $node = new Stream(new StreamNodeConfig(host: '::1', port: 1, connectTimeoutInSeconds: 0.5));
        $this->assertRaises(static fn () => $node->connect());

        // A failure that left PHP's error details unset.
        Faults::inject('stream_socket_client', static function (string $address, mixed &$code, mixed &$message): bool {
            $code = null;
            $message = null;

            return false;
        });
        $this->assertRaises(static fn () => (new Stream(new StreamNodeConfig()))->connect());
    }

    public function testConnectingTwiceKeepsTheConnection(): void {
        $node = $this->connected();
        $node->connect();
        $request = new Query('SELECT 1');
        $request->setVersion(ProtocolVersion::V4);
        $request->setStream(1);
        $node->writeRequest($request);
        $this->assertNotSame('', fread($this->peerStream(), 100));
    }

    public function testNativeFailuresAreWrapped(): void {
        $node = $this->connected();

        Faults::inject('stream_select', 1);
        Faults::inject('fread', static fn () => throw new RuntimeException('native'));
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));

        Faults::inject('stream_select', 1);
        Faults::inject('fwrite', static fn () => throw new RuntimeException('native'));
        $this->assertRaises(static fn () => $node->write('x'));
    }

    #[RequiresPhpExtension('sockets')]
    public function testOnlyAStreamConfigIsAccepted(): void {
        $this->expectException(StreamException::class);
        new Stream(new SocketNodeConfig());
    }

    public function testReadFailures(): void {
        $node = $this->connected();
        $read = static fn () => $node->readAvailableDataFromSource(1, 1, null);
        $timedOut = static fn (mixed $stream): array => ['timed_out' => true] + (is_resource($stream) ? \stream_get_meta_data($stream) : []);

        $this->assertSame('', $node->readAvailableDataFromSource(0, 0, null));

        // fread() failing: at end of stream, on a timeout, or for no stated reason.
        Faults::inject('stream_select', 1, 1, 1);
        Faults::inject('fread', false, false, false);
        Faults::inject('feof', true, false, false, false);
        $this->assertRaises($read);
        Faults::inject('stream_get_meta_data', $timedOut);
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, -1.0));
        $this->assertRaises($read);

        // fread() returning nothing at end of stream.
        Faults::reset();
        Faults::inject('stream_select', 1);
        Faults::inject('fread', '');
        Faults::inject('feof', true);
        $this->assertRaises($read);
    }

    public function testSelectFailuresWhileReading(): void {
        $node = $this->connected(timeout: 0.2);

        Faults::inject('stream_select', false);
        Faults::inject('feof', true);
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));

        Faults::inject('stream_select', false);
        Faults::inject('feof', false);
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, -1.0));

        Faults::always('stream_select', false);
        Faults::always('feof', false);
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));
    }

    public function testTheBlockingFallback(): void {
        Faults::inject('stream_set_blocking', false);
        $node = $this->connected(timeout: 0.2);

        // A deadline already behind, one too close to express, and the stall window.
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, \Cassandra\Connection\Clock::now() - 1.0));
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, \Cassandra\Connection\Clock::now() + 0.0000001));
        fwrite($this->peerStream(), 'ab');
        $this->assertSame('a', $node->readAvailableDataFromSource(1, 1, \Cassandra\Connection\Clock::now() + 0.1));
        $this->assertSame('b', $node->readAvailableDataFromSource(1, 1, \Cassandra\Connection\Clock::now() + 0.1));

        // A timed-out read with the stall window armed is the transport's failure.
        Faults::inject('fread', '');
        Faults::inject('feof', false);
        Faults::inject('stream_get_meta_data', static fn (mixed $stream): array => ['timed_out' => true] + (is_resource($stream) ? \stream_get_meta_data($stream) : []));
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));

        Faults::inject('stream_set_timeout', false);
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, \Cassandra\Connection\Clock::now() + 0.05));

        Faults::inject('stream_set_timeout', static function (): bool {
            trigger_error('injected', E_USER_WARNING);

            return true;
        });
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, \Cassandra\Connection\Clock::now() + 0.07));
    }

    public function testUseBeforeConnecting(): void {
        $node = new Stream(new StreamNodeConfig());
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));
        $this->assertRaises(static fn () => $node->write('x'));
    }

    public function testWriteFailures(): void {
        $node = $this->connected(timeout: 0.2);
        $write = static fn () => $node->write('x');
        $node->write('');

        // fwrite() failing at end of stream, on a timeout, or for no stated reason.
        Faults::inject('stream_select', 1, 1, 1);
        Faults::inject('fwrite', false, false, false);
        Faults::inject('feof', true, false, false);
        $this->assertRaises($write);
        Faults::inject('stream_get_meta_data', static fn (mixed $stream): array => ['timed_out' => true] + (is_resource($stream) ? \stream_get_meta_data($stream) : []));
        $this->assertRaises($write);
        $this->assertRaises($write);

        // No progress at all until the send timeout runs out.
        Faults::always('stream_select', 1);
        Faults::always('fwrite', 0);
        $this->assertRaises($write);
        Faults::reset();

        // select() saying no once, then failing, then working.
        Faults::inject('stream_select', 0, false, 1);
        Faults::inject('feof', false);
        $node->write('x');

        Faults::inject('stream_select', false);
        Faults::inject('feof', true);
        $this->assertRaises($write);

        Faults::always('stream_select', false);
        Faults::always('feof', false);
        $this->assertRaises($write);
    }

    /**
     * @param Closure(): mixed $call
     */
    private function assertRaises(Closure $call): void {
        try {
            $call();
            $this->fail('expected a StreamException');
        } catch (StreamException) {
            $this->addToAssertionCount(1);
        }
    }

    /**
     * The clock seen as $at by the next reading taken inside $function, as a
     * float because that is what hrtime() returns on 32-bit PHP.
     */
    private static function clockInsideAt(string $function, float $at): void {
        Faults::injectWhere('hrtime', static fn (): bool => in_array($function, array_column(debug_backtrace(), 'function'), true), $at * 1e9);
    }

    private function connected(float $timeout = 2.0): Stream {
        $server = stream_socket_server('tcp://127.0.0.1:0');
        $this->assertNotFalse($server);
        $this->server = $server;
        $name = (string) stream_socket_get_name($server, false);

        $node = new Stream(new StreamNodeConfig(host: '127.0.0.1', port: (int) substr((string) strrchr($name, ':'), 1), timeoutInSeconds: $timeout));
        $node->connect();

        $peer = stream_socket_accept($server, 2);
        $this->assertNotFalse($peer);
        $this->peer = $peer;

        return $node;
    }

    /**
     * The clock seen once from inside $function only, far past any stall window,
     * so that the checks that read it again find the window still open.
     */
    private static function jumpClockInside(string $function): void {
        Faults::injectWhere('hrtime', static fn (): bool => in_array($function, array_column(debug_backtrace(), 'function'), true), static fn (): float|int => \hrtime(true) + 1_000_000_000_000);
    }

    /**
     * @return resource
     */
    private function peerStream() {
        $this->assertIsResource($this->peer);

        return $this->peer;
    }
}
