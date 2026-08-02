<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\IoNode;
use Cassandra\Connection\Socket;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\NodeException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\SocketException;
use ErrorException;
use Socket as PhpSocket;

/**
 * A transport failure is reported once, as an exception — never also as a PHP
 * diagnostic.
 *
 * Both transports read the error themselves and raise a NodeException carrying
 * far more context than PHP's warning does, so the warning is a duplicate. It
 * is not merely noise: these paths run from Connection::disconnect() while the
 * failure that caused it is still propagating, and from the transports'
 * destructors, so an application whose error handler turns warnings into
 * exceptions (as Symfony's and Laravel's do) would get that handler firing in
 * place of the driver's own report, and firing from a destructor.
 *
 * The conditions here are the ordinary ones — a peer that reset the connection,
 * a port nothing is listening on — not edge cases. They are driven against real
 * sockets, reset with SO_LINGER 0, because the whole point is what the operating
 * system does to the other end: a clean FIN takes a different path through both
 * transports and raises nothing either way, so a test that let the reset turn
 * into an end of file would pass without having exercised anything.
 *
 * Not every suppressed call has a case here — an interrupted select() and a
 * write to a vanished peer are not reachable on demand — but they are the same
 * rule applied to the same kind of call.
 */
final class TransportCloseTest extends AbstractUnitTestCase {
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

    public function testAnInvalidSocketOptionIsWrappedAtTheTransportBoundary(): void {
        set_error_handler(static function (int $severity, string $message): never {
            throw new ErrorException($message, 0, $severity);
        });

        try {
            $node = new Socket(new SocketNodeConfig(
                host: '127.0.0.1',
                socketOptions: [999999 => 1],
            ));
            $node->connect();
            $this->fail('Expected the invalid socket option to be rejected');
        } catch (SocketException $e) {
            $this->assertSame(ExceptionCode::SOCKET_SET_OPTION_FAILED->value, $e->getCode());
            $this->assertInstanceOf(ErrorException::class, $e->getPrevious());
        } finally {
            restore_error_handler();
        }
    }

    public function testASocketResetIsReportedOnlyAsAnException(): void {
        $node = $this->connectSocket();

        $this->resetPeer();

        $warnings = $this->collectWarnings(function () use ($node): void {
            $this->readUntilItFails($node);
            $node->close();
            $this->node = null;
        });

        $this->assertSame([], $warnings, 'the SocketException is the report; PHP must not raise one of its own');
    }

    public function testAStreamResetIsReportedOnlyAsAnException(): void {
        $node = $this->connectStream();

        $this->resetPeer();

        $warnings = $this->collectWarnings(function () use ($node): void {
            $this->readUntilItFails($node);
            $node->close();
            $this->node = null;
        });

        $this->assertSame([], $warnings, 'the StreamException is the report; PHP must not raise one of its own');
    }

    public function testConnectingToAClosedPortIsReportedOnlyAsAnException(): void {
        // Both transports report a refused connection through their own
        // exception, so PHP's warning about it is a duplicate that would reach
        // an application's error handler first.
        $port = $this->closedPort();

        $warnings = $this->collectWarnings(function () use ($port): void {
            foreach ([
                new Socket(new SocketNodeConfig(host: '127.0.0.1', port: $port)),
                new Stream(new StreamNodeConfig(host: '127.0.0.1', port: $port, connectTimeoutInSeconds: 1.0)),
            ] as $node) {
                try {
                    $node->connect();
                    $this->fail('expected the connection to be refused');
                } catch (NodeException $e) {
                }
            }
        });

        $this->assertSame([], $warnings);
    }

    /**
     * Accept the connection so that the peer end is ours to reset.
     */
    private function accept(): void {
        if (!is_resource($this->server)) {
            $this->fail('no listening socket');
        }

        $client = @stream_socket_accept($this->server, 5);
        if ($client === false) {
            $this->fail('the connection was never accepted');
        }

        $this->acceptedClient = $client;
    }

    /**
     * A port nothing is listening on: bind one, learn its number, then let it
     * go, so that connecting to it is refused rather than merely slow.
     */
    private function closedPort(): int {
        $port = $this->listen();

        if (is_resource($this->server)) {
            fclose($this->server);
        }
        $this->server = null;

        return $port;
    }

    /**
     * Run $body with an error handler in place and report what reached it.
     *
     * @param callable(): void $body
     *
     * @return array<string> the diagnostics PHP raised
     */
    private function collectWarnings(callable $body): array {
        $warnings = [];

        // Forced on for the duration: PHPUnit runs with E_WARNING masked out of
        // error_reporting(), and the handler below honours that mask exactly as
        // a framework's handler does — so without this every diagnostic would
        // be discarded before it was recorded and the assertions would hold
        // whether or not anything was raised.
        $previousLevel = error_reporting(E_ALL);

        set_error_handler(static function (int $errno, string $message) use (&$warnings): bool {
            // Suppressed diagnostics are handed to the handler all the same, and
            // are told apart only by error_reporting() being narrowed for the
            // duration of the call. Making the same check a framework's handler
            // makes is the point: what is asserted is that nothing reaches an
            // application, not that the underlying calls never fail.
            if (!(error_reporting() & $errno)) {
                return false;
            }

            $warnings[] = $message;

            return true;
        });

        try {
            $body();
        } finally {
            restore_error_handler();
            error_reporting($previousLevel);
        }

        return $warnings;
    }

    private function connectSocket(): IoNode {
        $node = new Socket(new SocketNodeConfig(
            host: '127.0.0.1',
            port: $this->listen(),
            socketOptions: [
                SO_RCVTIMEO => ['sec' => 2, 'usec' => 0],
                SO_SNDTIMEO => ['sec' => 2, 'usec' => 0],
            ],
        ));

        $node->connect();
        $this->node = $node;

        $this->accept();

        return $node;
    }

    private function connectStream(): IoNode {
        $node = new Stream(new StreamNodeConfig(
            host: '127.0.0.1',
            port: $this->listen(),
            timeoutInSeconds: 2.0,
        ));

        $node->connect();
        $this->node = $node;

        $this->accept();

        return $node;
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
     * Read until the reset is delivered, which is what leaves the socket
     * unconnected and so sets up the close being tested. A reset that has not
     * arrived yet reads as "nothing there", so this keeps asking.
     */
    private function readUntilItFails(IoNode $node): void {
        $deadline = microtime(true) + 5.0;

        while (microtime(true) < $deadline) {
            try {
                $node->readAvailableDataFromSource(9, 9, readDeadline: microtime(true) + 0.2);
            } catch (NodeException $e) {
                return;
            }

            usleep(50_000);
        }

        $this->fail('the connection reset never surfaced as a transport failure');
    }

    /**
     * Close the accepted end with SO_LINGER 0, which sends an RST instead of a
     * FIN — the difference that leaves our end unconnected rather than merely
     * at end of file.
     */
    private function resetPeer(): void {
        $client = $this->acceptedClient;
        if (!is_resource($client)) {
            $this->fail('the connection was never accepted');
        }

        $peer = socket_import_stream($client);
        if (!($peer instanceof PhpSocket)) {
            $this->fail('could not take the accepted connection as a socket');
        }

        socket_set_option($peer, SOL_SOCKET, SO_LINGER, ['l_onoff' => 1, 'l_linger' => 0]);

        fclose($client);
        $this->acceptedClient = null;

        // Let the reset arrive before anything reads. Without this the first
        // read can still see a plain end of file, which the transports report
        // just as accurately but by a path that never had a warning to raise —
        // leaving the test passing without having exercised anything.
        usleep(200_000);
    }
}
