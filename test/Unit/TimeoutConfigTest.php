<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\Socket;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\SocketException;
use ReflectionProperty;

/**
 * The send/receive timeouts of both transports are derived from their node
 * configuration: SO_SNDTIMEO/SO_RCVTIMEO for the socket transport and
 * timeoutInSeconds for the stream transport. Both are handled as fractional
 * seconds, and a zero timeout means "no timeout".
 */
final class TimeoutConfigTest extends AbstractUnitTestCase {
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

    public function testSocketNonPositiveConnectTimeoutIsRejected(): void {
        $this->expectException(SocketException::class);

        // An unbounded connect would let an unreachable host wedge the client,
        // so "no timeout" is not accepted here, unlike for the I/O timeouts.
        new Socket(new SocketNodeConfig(connectTimeoutInSeconds: 0));
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
