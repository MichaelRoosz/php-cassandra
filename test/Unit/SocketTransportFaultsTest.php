<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\Clock;
use Cassandra\Connection\Socket;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\SocketException;
use Cassandra\Test\Support\Faults;
use Closure;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use RuntimeException;

/**
 * The socket transport against a real local listener, with the sockets
 * functions made to fail with the error codes a real network produces.
 */
#[RequiresPhpExtension('sockets')]
final class SocketTransportFaultsTest extends AbstractUnitTestCase {
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

    public function testABracketedIpv6HostIsUnwrapped(): void {
        $node = new Socket(new SocketNodeConfig(host: '[::1]', port: 1, connectTimeoutInSeconds: 0.5));
        $this->assertRaises(static fn () => $node->connect());
    }

    public function testAClockReadingPastTheStallWindowIsCheckedAgain(): void {
        $node = $this->connected();

        fwrite($this->peerStream(), 'a');
        self::jumpClockInside('selectSocketForRead');
        $this->assertSame('a', $node->readAvailableDataFromSource(1, 1, null));

        self::jumpClockInside('selectSocketForWrite');
        $node->write('x');
        $this->assertSame('x', fread($this->peerStream(), 1));
    }

    public function testBlockingReadsAtTheEdgeOfTheirDeadline(): void {
        Faults::inject('socket_set_nonblock', false);
        $node = $this->connected();

        // Too little of the deadline left to express as a receive timeout.
        $deadline = Clock::now() + 1.0;
        self::clockInsideAt('applyReceiveTimeout', $deadline - 0.0000002);
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, $deadline));

        // Interrupted, re-armed, and then the data is there.
        fwrite($this->peerStream(), 'a');
        Faults::inject('socket_read', false);
        Faults::inject('socket_last_error', SOCKET_EINTR);
        $this->assertSame('a', $node->readAvailableDataFromSource(1, 1, null));
    }

    public function testConfigurationIsValidated(): void {
        $this->assertRaises(static fn () => new Socket(new StreamNodeConfig()));
        $this->assertRaises(static fn () => new Socket(new SocketNodeConfig(connectTimeoutInSeconds: 0)));
        $this->assertRaises(static fn () => new Socket(new SocketNodeConfig(socketOptions: [SO_RCVTIMEO => 5])));
        $this->assertRaises(static fn () => new Socket(new SocketNodeConfig(socketOptions: [SO_SNDTIMEO => ['sec' => -1, 'usec' => 0]])));
        $this->assertInstanceOf(Socket::class, new Socket(new SocketNodeConfig(socketOptions: [SO_RCVTIMEO => ['sec' => 0, 'usec' => 0]])));
        $this->assertRaises(fn () => $this->node(socketOptions: [SO_KEEPALIVE => 1.5])->connect());
        $this->assertRaises(static fn () => (new Socket(new SocketNodeConfig(host: 'tls://127.0.0.1')))->connect());
    }

    public function testConnectFailures(): void {
        Faults::inject('socket_create', static fn () => throw new RuntimeException('native'));
        $this->assertRaises(fn () => $this->node()->connect());

        Faults::inject('socket_create', false);
        $this->assertRaises(fn () => $this->node()->connect());

        Faults::inject('socket_set_option', false);
        $this->assertRaises(fn () => $this->node()->connect());

        Faults::inject('socket_set_option', static function (): bool {
            trigger_error('injected', E_USER_WARNING);

            return true;
        });
        $this->assertRaises(fn () => $this->node()->connect());

        foreach ([SOCKET_ETIMEDOUT, SOCKET_ECONNREFUSED] as $code) {
            Faults::inject('socket_connect', false);
            Faults::inject('socket_last_error', $code);
            $this->assertRaises(fn () => $this->node()->connect());
        }

        // Interrupted until the connect timeout, and interrupted once before succeeding.
        Faults::always('socket_connect', false);
        Faults::always('socket_last_error', SOCKET_EINTR);
        $this->assertRaises(fn () => $this->node(connectTimeout: 0.05)->connect());
        Faults::reset();

        Faults::inject('socket_connect', false, false);
        Faults::inject('socket_last_error', SOCKET_EINTR, SOCKET_EISCONN);
        $this->node()->connect();
    }

    public function testNativeFailuresAreWrapped(): void {
        $node = $this->connected();

        Faults::inject('socket_select', 1);
        Faults::inject('socket_read', static fn () => throw new RuntimeException('native'));
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));

        Faults::inject('socket_select', 1);
        Faults::inject('socket_write', static fn () => throw new RuntimeException('native'));
        $this->assertRaises(static fn () => $node->write('x'));
    }

    public function testReadErrorCodes(): void {
        $node = $this->connected(timeout: 0.2);
        $read = static fn (?float $deadline = null) => $node->readAvailableDataFromSource(1, 1, $deadline);
        $failRead = static function (int ...$codes): void {
            Faults::inject('socket_select', ...array_fill(0, count($codes), 1));
            Faults::inject('socket_read', ...array_fill(0, count($codes), false));
            Faults::inject('socket_last_error', ...$codes);
        };

        $failRead(SOCKET_EINTR);
        $this->assertSame('', $read(-1.0));
        $failRead(SOCKET_EWOULDBLOCK);
        $this->assertSame('', $read());
        $failRead(SOCKET_ETIMEDOUT);
        $this->assertSame('', $read());
        foreach ([SOCKET_ECONNRESET, SOCKET_EBADF] as $code) {
            $failRead($code);
            $this->assertRaises($read);
        }

        Faults::inject('socket_select', 1);
        Faults::inject('socket_read', '');
        $this->assertRaises($read);

        // Interrupted while waiting with a deadline that then passes.
        Faults::inject('socket_select', 1);
        Faults::inject('socket_read', static function (): bool {
            usleep(30000);

            return false;
        });
        Faults::inject('socket_last_error', SOCKET_EINTR);
        $this->assertSame('', $read(Clock::now() + 0.02));
    }

    public function testResolutionFailures(): void {
        Faults::inject('socket_addrinfo_lookup', false);
        $this->assertRaises(fn () => $this->node()->connect());

        Faults::inject('socket_addrinfo_explain', ['ai_family' => 'x'], ['ai_family' => AF_INET, 'ai_addr' => ['sin_addr' => '']]);
        Faults::inject('socket_addrinfo_lookup', static fn (): array => [1, 2]);
        $this->assertRaises(fn () => $this->node()->connect());
    }

    public function testSelectFailuresWhileReading(): void {
        $node = $this->connected(timeout: 0.1);

        Faults::inject('socket_select', false, false);
        Faults::inject('socket_last_error', SOCKET_EINTR, SOCKET_EINTR);
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, -1.0));

        Faults::inject('socket_select', false);
        Faults::inject('socket_last_error', SOCKET_EBADF);
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));

        // Interrupted and then nothing ready, until the stall window runs out.
        Faults::inject('socket_select', false);
        Faults::inject('socket_last_error', SOCKET_EINTR);
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));
    }

    public function testTheBlockingFallback(): void {
        Faults::inject('socket_set_nonblock', false);
        $node = $this->connected(timeout: 0.2);

        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, Clock::now() - 1.0));
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, Clock::now() + 0.0000001));
        fwrite($this->peerStream(), 'ab');
        $this->assertSame('a', $node->readAvailableDataFromSource(1, 1, Clock::now() + 0.1));
        $this->assertSame('b', $node->readAvailableDataFromSource(1, 1, Clock::now() + 0.1));

        // Interrupted, then the deadline is gone before a retry could start.
        Faults::inject('socket_read', static function (): bool {
            usleep(30000);

            return false;
        });
        Faults::inject('socket_last_error', SOCKET_EINTR);
        $this->assertSame('', $node->readAvailableDataFromSource(1, 1, Clock::now() + 0.02));

        foreach ([SOCKET_EWOULDBLOCK, SOCKET_ETIMEDOUT] as $code) {
            Faults::inject('socket_read', false);
            Faults::inject('socket_last_error', $code);
            $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));
        }

        $node->write('x');
    }

    public function testUseBeforeConnectingAndConnectingTwice(): void {
        $node = $this->node();
        $this->assertRaises(static fn () => $node->readAvailableDataFromSource(1, 1, null));
        $this->assertRaises(static fn () => $node->write('x'));
        $node->close();

        $node->connect();
        $node->connect();
        $this->assertSame('', $node->readAvailableDataFromSource(0, 0, null));
        $node->write('');
    }

    public function testWaitingForAConnectionInProgress(): void {
        $inProgress = static function (): void {
            Faults::inject('socket_connect', false);
            Faults::inject('socket_last_error', SOCKET_EINPROGRESS);
        };

        // select() interrupted, then nothing ready, then ready with an error pending.
        $inProgress();
        Faults::inject('socket_select', false, 0, 1);
        Faults::inject('socket_last_error', SOCKET_EINTR);
        Faults::inject('socket_get_option', SOCKET_ECONNREFUSED);
        $this->assertRaises(fn () => $this->node()->connect());

        $inProgress();
        Faults::inject('socket_select', 1);
        Faults::inject('socket_get_option', false);
        $this->assertRaises(fn () => $this->node()->connect());

        $inProgress();
        Faults::inject('socket_select', false);
        Faults::inject('socket_last_error', SOCKET_EBADF);
        $this->assertRaises(fn () => $this->node()->connect());

        $inProgress();
        Faults::always('socket_select', 0);
        $this->assertRaises(fn () => $this->node(connectTimeout: 0.05)->connect());
    }

    public function testWriteErrorCodes(): void {
        $node = $this->connected(timeout: 0.2);
        $write = static fn () => $node->write('x');

        foreach ([SOCKET_ECONNRESET, SOCKET_ETIMEDOUT, SOCKET_EBADF] as $code) {
            Faults::inject('socket_select', 1);
            Faults::inject('socket_write', false);
            Faults::inject('socket_last_error', $code);
            $this->assertRaises($write);
        }

        Faults::inject('socket_select', 1, 1);
        Faults::inject('socket_write', false, 1);
        Faults::inject('socket_last_error', SOCKET_EWOULDBLOCK);
        $write();

        Faults::inject('socket_select', 1);
        Faults::inject('socket_write', false, 1);
        Faults::inject('socket_last_error', SOCKET_ENOBUFS);
        $write();

        Faults::inject('socket_select', 1);
        Faults::inject('socket_write', false, false, false);
        Faults::inject('socket_last_error', SOCKET_ENOBUFS, SOCKET_ENOBUFS, SOCKET_ENOBUFS);
        $this->assertRaises($write);

        // select() interrupted, then not ready, then failing outright.
        Faults::inject('socket_select', false, 0, 1);
        Faults::inject('socket_last_error', SOCKET_EINTR);
        $write();

        Faults::inject('socket_select', false);
        Faults::inject('socket_last_error', SOCKET_EBADF);
        $this->assertRaises($write);

        // No progress at all until the send timeout runs out.
        Faults::always('socket_select', 1);
        Faults::always('socket_write', 0);
        $this->assertRaises($write);
    }

    /**
     * @param Closure(): mixed $call
     */
    private function assertRaises(Closure $call): void {
        try {
            $call();
            $this->fail('expected a SocketException');
        } catch (SocketException) {
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

    private function connected(float $timeout = 2.0): Socket {
        $node = $this->node(timeout: $timeout);
        $node->connect();

        $peer = stream_socket_accept($this->listener(), 2);
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
    private function listener() {
        if ($this->server === null) {
            $server = stream_socket_server('tcp://127.0.0.1:0');
            $this->assertNotFalse($server);
            $this->server = $server;
        }

        return $this->server;
    }

    /**
     * @param array<int, mixed> $socketOptions
     */
    private function node(float $timeout = 2.0, float $connectTimeout = 2.0, array $socketOptions = []): Socket {
        $name = (string) stream_socket_get_name($this->listener(), false);
        $window = ['sec' => (int) $timeout, 'usec' => (int) round(($timeout - (int) $timeout) * 1_000_000)];

        // Through reflection, since some tests pass option values the signature rules out.
        $config = (new \ReflectionClass(SocketNodeConfig::class))->newInstanceArgs([
            'host' => '127.0.0.1',
            'port' => (int) substr((string) strrchr($name, ':'), 1),
            'socketOptions' => $socketOptions + [SO_RCVTIMEO => $window, SO_SNDTIMEO => $window],
            'connectTimeoutInSeconds' => $connectTimeout,
        ]);

        return new Socket($config);
    }

    /**
     * @return resource
     */
    private function peerStream() {
        $this->assertIsResource($this->peer);

        return $this->peer;
    }
}
