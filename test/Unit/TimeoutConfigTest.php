<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\Socket;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\RequestException;
use Cassandra\Exception\SocketException;
use Cassandra\Exception\StreamException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Request\Options\QueryOptions;
use ReflectionProperty;
use ReflectionMethod;

/**
 * The send/receive timeouts of both transports are derived from their node
 * configuration: SO_SNDTIMEO/SO_RCVTIMEO for the socket transport and
 * timeoutInSeconds for the stream transport. Both are handled as fractional
 * seconds, and a zero timeout means "no timeout".
 *
 * The client-side budgets in ConnectionOptions are checked here too, where
 * "no timeout" is spelled null instead and a non-positive value is rejected.
 */
final class TimeoutConfigTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{float}>
     */
    public static function nonFiniteStreamTimeoutProvider(): array {
        return [
            'not a number' => [NAN],
            'positive infinity' => [INF],
            'negative infinity' => [-INF],
        ];
    }
    /**
     * @return array<string, array{float}>
     */
    public static function nonPositiveTimeoutProvider(): array {
        return [
            'zero' => [0.0],
            'negative' => [-1.0],
        ];
    }

    /**
     * @dataProvider nonPositiveTimeoutProvider
     */
    public function testBatchOptionsRejectNonPositiveRequestTimeout(float $timeout): void {
        $this->expectException(RequestException::class);

        new BatchOptions(requestTimeoutInSeconds: $timeout);
    }

    public function testConnectionAcceptsAssociativeAllowedProtocolVersions(): void {
        $options = new ConnectionOptions(
            allowedProtocolVersions: [7 => ProtocolVersion::V3],
        );

        $this->assertSame(ProtocolVersion::V3, $options->initialProtocolVersion);
        $this->assertSame([7 => ProtocolVersion::V3], $options->allowedProtocolVersions);
    }

    public function testConnectionInitialProtocolVersionMustBeAllowed(): void {
        $this->expectException(ConnectionException::class);

        new ConnectionOptions(
            allowedProtocolVersions: [ProtocolVersion::V3],
            initialProtocolVersion: ProtocolVersion::V4,
        );
    }

    public function testConnectionNonPositiveHeartbeatIntervalIsRejected(): void {
        // Heartbeats are turned off with null; zero would mean probing on every
        // single read.
        $this->expectException(ConnectionException::class);

        new ConnectionOptions(heartbeatIntervalInSeconds: 0);
    }

    public function testConnectionNonPositiveHeartbeatTimeoutIsRejected(): void {
        // A probe that is overdue the moment it goes out would declare every
        // healthy node dead.
        $this->expectException(ConnectionException::class);

        new ConnectionOptions(heartbeatTimeoutInSeconds: 0);
    }

    public function testConnectionNonPositiveRequestTimeoutIsRejected(): void {
        // A request that is out of time before it is sent cannot be what the
        // caller meant; waiting indefinitely is spelled null.
        $this->expectException(ConnectionException::class);

        new ConnectionOptions(requestTimeoutInSeconds: 0);
    }

    public function testConnectionPrefersV4WhenInitialVersionIsOmitted(): void {
        $options = new ConnectionOptions(
            allowedProtocolVersions: [ProtocolVersion::V5, ProtocolVersion::V3, ProtocolVersion::V4],
        );

        $this->assertSame(ProtocolVersion::V4, $options->initialProtocolVersion);
    }

    public function testConnectionRejectsInvalidAllowedProtocolVersionTypes(): void {
        $this->expectException(ConnectionException::class);

        /** @phpstan-ignore argument.type */
        new ConnectionOptions(allowedProtocolVersions: ['v4']);
    }

    public function testConnectionRequiresAnAllowedProtocolVersion(): void {
        $this->expectException(ConnectionException::class);

        new ConnectionOptions(allowedProtocolVersions: []);
    }

    public function testConnectionTimeoutsMayBeDisabledWithNull(): void {
        $options = new ConnectionOptions(
            requestTimeoutInSeconds: null,
            heartbeatIntervalInSeconds: null,
        );

        $this->assertNull($options->requestTimeoutInSeconds);
        $this->assertNull($options->heartbeatIntervalInSeconds);

        // With heartbeats off the heartbeat timeout governs nothing, so it is
        // not held to the same rule.
        $this->assertInstanceOf(ConnectionOptions::class, new ConnectionOptions(
            heartbeatIntervalInSeconds: null,
            heartbeatTimeoutInSeconds: 0,
        ));
    }

    public function testConnectionUsesLowestAllowedProtocolVersionWhenV4IsNotAllowed(): void {
        $options = new ConnectionOptions(
            allowedProtocolVersions: [ProtocolVersion::V5, ProtocolVersion::V3],
        );

        $this->assertSame(ProtocolVersion::V3, $options->initialProtocolVersion);
    }

    /**
     * @dataProvider nonPositiveTimeoutProvider
     */
    public function testExecuteOptionsRejectNonPositiveRequestTimeout(float $timeout): void {
        $this->expectException(RequestException::class);

        new ExecuteOptions(requestTimeoutInSeconds: $timeout);
    }

    /**
     * @dataProvider nonPositiveTimeoutProvider
     */
    public function testPrepareOptionsRejectNonPositiveRequestTimeout(float $timeout): void {
        $this->expectException(RequestException::class);

        new PrepareOptions(requestTimeoutInSeconds: $timeout);
    }

    /**
     * The same value ConnectionOptions rejects has to be rejected wherever else
     * a request timeout can be given, or the strictest surface would be the one
     * least likely to be hit: deadlineFor() normalises a non-positive timeout to
     * zero, which silently expires every request it applies to.
     *
     * @dataProvider nonPositiveTimeoutProvider
     */
    public function testRequestOptionsRejectNonPositiveRequestTimeout(float $timeout): void {
        $this->expectException(RequestException::class);

        new QueryOptions(requestTimeoutInSeconds: $timeout);
    }

    public function testRequestTimeoutsMayStillBeOmitted(): void {
        // Null everywhere means "fall back", which is what the rejection above
        // must not get in the way of.
        $this->assertNull((new QueryOptions())->requestTimeoutInSeconds);
        $this->assertNull((new BatchOptions())->requestTimeoutInSeconds);
        $this->assertNull((new ExecuteOptions())->requestTimeoutInSeconds);
        $this->assertNull((new PrepareOptions())->requestTimeoutInSeconds);

        $connection = new Connection([new StreamNodeConfig()]);
        $connection->setRequestTimeout(null);
        $connection->setRequestTimeout(0.5);

        $this->assertFalse($connection->isConnected());
    }

    /**
     * @dataProvider nonPositiveTimeoutProvider
     */
    public function testSetRequestTimeoutRejectsNonPositiveValues(float $timeout): void {
        // Lowering the connection default applies to the requests already in
        // flight, so a bad value here expires them all at once.
        $connection = new Connection([new StreamNodeConfig()]);

        $this->expectException(ConnectionException::class);

        $connection->setRequestTimeout($timeout);
    }

    public function testSocketFractionalSecondsAreCombined(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new SocketNodeConfig(socketOptions: [
            SO_SNDTIMEO => ['sec' => 2, 'usec' => 500000],
            SO_RCVTIMEO => ['sec' => 1, 'usec' => 250000],
        ]));

        $this->assertSame(2.5, $sendTimeout);
        $this->assertSame(1.25, $receiveTimeout);
    }

    public function testSocketInvalidTimeoutIsRejected(): void {
        $this->expectException(SocketException::class);

        $this->getTimeouts(new SocketNodeConfig(socketOptions: [
            SO_RCVTIMEO => ['sec' => 1, 'usec' => 'nope'],
        ]));
    }

    public function testSocketMissingOptionsFallBackToTheConfigDefaults(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new SocketNodeConfig(socketOptions: []));

        $this->assertSame(10.0, $sendTimeout);
        $this->assertSame(15.0, $receiveTimeout);
    }

    public function testSocketNonFiniteConnectTimeoutIsRejected(): void {
        $this->expectException(SocketException::class);

        new Socket(new SocketNodeConfig(connectTimeoutInSeconds: NAN));
    }

    public function testSocketNonPositiveConnectTimeoutIsRejected(): void {
        $this->expectException(SocketException::class);

        // An unbounded connect would let an unreachable host wedge the client,
        // so "no timeout" is not accepted here, unlike for the I/O timeouts.
        new Socket(new SocketNodeConfig(connectTimeoutInSeconds: 0));
    }

    public function testSocketOptionNativeErrorIsWrapped(): void {
        $pair = [];
        if (!socket_create_pair(AF_UNIX, SOCK_STREAM, 0, $pair)) {
            $this->fail('could not create a local socket pair');
        }

        if (!is_array($pair)) {
            $this->fail('socket_create_pair did not return a socket pair');
        }

        [$firstSocket, $secondSocket] = array_values($pair);

        if (!$firstSocket instanceof \Socket || !$secondSocket instanceof \Socket) {
            $this->fail('socket_create_pair did not return a socket pair');
        }

        try {
            $method = new ReflectionMethod(Socket::class, 'setSocketOption');
            $method->invoke(
                new Socket(new SocketNodeConfig()),
                $firstSocket,
                SOL_SOCKET,
                -999,
                1,
            );
            $this->fail('Expected a SocketException');
        } catch (SocketException $e) {
            $this->assertSame(\Cassandra\Exception\ExceptionCode::SOCKET_SET_OPTION_FAILED->value, $e->getCode());
            $this->assertInstanceOf(\ErrorException::class, $e->getPrevious());
        } finally {
            socket_close($firstSocket);
            socket_close($secondSocket);
        }
    }

    public function testSocketSubSecondTimeoutIsNotRoundedAway(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new SocketNodeConfig(socketOptions: [
            SO_SNDTIMEO => ['sec' => 0, 'usec' => 200000],
            SO_RCVTIMEO => ['sec' => 0, 'usec' => 750000],
        ]));

        $this->assertSame(0.2, $sendTimeout);
        $this->assertSame(0.75, $receiveTimeout);
    }

    public function testSocketZeroTimeoutDisablesTheTimeout(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new SocketNodeConfig(socketOptions: [
            SO_SNDTIMEO => ['sec' => 0, 'usec' => 0],
            SO_RCVTIMEO => ['sec' => 0, 'usec' => 0],
        ]));

        $this->assertSame(INF, $sendTimeout);
        $this->assertSame(INF, $receiveTimeout);
    }

    public function testStreamFractionalTimeoutIsNotTruncated(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new StreamNodeConfig(timeoutInSeconds: 0.5));

        $this->assertSame(0.5, $sendTimeout);
        $this->assertSame(0.5, $receiveTimeout);
    }

    public function testStreamInvalidConnectTimeoutIsRejected(): void {
        $this->expectException(\Cassandra\Exception\StreamException::class);

        new Stream(new StreamNodeConfig(connectTimeoutInSeconds: 0));
    }

    public function testStreamNonFiniteConnectTimeoutIsRejected(): void {
        $this->expectException(StreamException::class);

        new Stream(new StreamNodeConfig(connectTimeoutInSeconds: INF));
    }

    /**
     * @dataProvider nonFiniteStreamTimeoutProvider
     */
    public function testStreamNonFiniteTimeoutIsRejected(float $timeout): void {
        $this->expectException(StreamException::class);

        new Stream(new StreamNodeConfig(timeoutInSeconds: $timeout));
    }

    public function testStreamTimeoutIsTakenFromTheConfig(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new StreamNodeConfig(timeoutInSeconds: 12.5));

        $this->assertSame(12.5, $sendTimeout);
        $this->assertSame(12.5, $receiveTimeout);
    }

    public function testStreamUsesTheConfigDefault(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new StreamNodeConfig());

        $this->assertSame(15.0, $sendTimeout);
        $this->assertSame(15.0, $receiveTimeout);
    }

    public function testStreamZeroTimeoutDisablesTheTimeout(): void {
        [$sendTimeout, $receiveTimeout] = $this->getTimeouts(new StreamNodeConfig(timeoutInSeconds: 0));

        $this->assertSame(INF, $sendTimeout);
        $this->assertSame(INF, $receiveTimeout);
    }

    public function testUnsupportedStreamTimeoutIsReported(): void {
        $stream = fopen('php://memory', 'r+');
        if ($stream === false) {
            $this->fail('could not create a memory stream');
        }

        try {
            $method = new ReflectionMethod(Stream::class, 'setStreamTimeout');
            $method->invoke(new Stream(new StreamNodeConfig()), $stream, 1, 0);
            $this->fail('Expected a StreamException');
        } catch (StreamException $e) {
            $this->assertSame(\Cassandra\Exception\ExceptionCode::STREAM_SET_TIMEOUT_FAILED->value, $e->getCode());
        } finally {
            fclose($stream);
        }
    }

    /**
     * @return array{float, float}
     *
     * @throws \Cassandra\Exception\NodeException
     */
    private function getTimeouts(NodeConfig $config): array {
        $node = $config instanceof SocketNodeConfig ? new Socket($config) : new Stream($config);
        $class = $config instanceof SocketNodeConfig ? Socket::class : Stream::class;

        /** @var float $send */
        $send = (new ReflectionProperty($class, 'sendTimeout'))->getValue($node);
        /** @var float $receive */
        $receive = (new ReflectionProperty($class, 'receiveTimeout'))->getValue($node);

        return [$send, $receive];
    }
}
