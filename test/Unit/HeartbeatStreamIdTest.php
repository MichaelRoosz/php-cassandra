<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use ReflectionMethod;
use Cassandra\Connection\HeartbeatMonitor;
use Cassandra\Connection\Session;
use Cassandra\Connection\StreamIdPool;
use ReflectionProperty;
use SplQueue;

/**
 * The heartbeat is skipped, not waited for, when every stream id is in use.
 *
 * Claiming an id on an exhausted connection means waiting for one to be
 * released, and waiting means reading — which the non-blocking calls that reach
 * the probe through keepNonBlockingBookkeeping() promised not to do. Nothing is
 * lost by skipping: requests that really are going unanswered run out of time,
 * orphan their ids and take the connection with them at maxOrphanedStreams.
 *
 * The read bound has to agree with that decision. It is what lets a wait with no
 * deadline of its own come up for air in time to send the probe, so a bound that
 * says "now" for a probe that will not be sent turns every wait into a spin over
 * reads that return immediately.
 */
final class HeartbeatStreamIdTest extends AbstractUnitTestCase {
    public function testAnExhaustedIdSpaceDoesNotBoundTheReadForAProbeItWillNotSend(): void {
        $connection = $this->connectionWithHeartbeatDue();

        $this->exhaustStreamIds($connection);

        $this->assertNull(
            $this->nextHeartbeatActionAt($connection),
            'a bound in the past for a probe that is never sent would spin every wait'
        );
    }

    public function testAnExhaustedIdSpaceSkipsTheProbe(): void {
        // Nothing is connected, so sending would have to open a connection and
        // would fail loudly. Skipping is what keeps this quiet — and what keeps
        // it from waiting for an id in the first place.
        $connection = $this->connectionWithHeartbeatDue();

        $this->exhaustStreamIds($connection);

        $checkHeartbeat = new ReflectionMethod(Session::class, 'checkHeartbeat');
        $checkHeartbeat->invoke(self::sessionOf($connection));

        $this->assertNull(
            self::heartbeatOf($connection)->probe(),
            'no probe was sent, so none is outstanding'
        );
        $this->assertFalse($connection->isConnected(), 'and nothing was opened to send one on');
    }
    public function testAProbeThatIsDueBoundsTheRead(): void {
        // The counterpart: with ids to spare the probe will be sent, so the
        // bound must be there for the wait to be cut short by.
        $connection = $this->connectionWithHeartbeatDue();

        $this->assertNotNull(
            $this->nextHeartbeatActionAt($connection),
            'a probe that is going to be sent is worth waking a read up for'
        );
    }

    /**
     * A connection whose handshake is behind it and which has been silent for
     * longer than the heartbeat interval, i.e. one whose next probe is overdue.
     */
    private function connectionWithHeartbeatDue(): Connection {
        // Port 1 so that sending anything is a refused connection rather than a
        // stray hit on whatever the developer has running locally.
        $connection = new Connection(
            [new SocketNodeConfig(host: '127.0.0.1', port: 1)],
            options: new ConnectionOptions(heartbeatIntervalInSeconds: 30.0),
        );

        (new ReflectionProperty(Session::class, 'handshakeComplete'))->setValue(self::sessionOf($connection), true);
        (new ReflectionProperty(HeartbeatMonitor::class, 'lastResponseAt'))->setValue(self::heartbeatOf($connection), microtime(true) - 600.0);

        return $connection;
    }

    private function exhaustStreamIds(Connection $connection): void {
        /** @var SplQueue<int> $empty */
        $empty = new SplQueue();

        $pool = $this->streamIdPoolOf($connection);

        (new ReflectionProperty(StreamIdPool::class, 'nextStreamId'))->setValue($pool, StreamIdPool::MAX_STREAM_ID + 1);
        (new ReflectionProperty(StreamIdPool::class, 'recycledStreams'))->setValue($pool, $empty);
    }

    private function nextHeartbeatActionAt(Connection $connection): ?float {
        /** @var bool $handshakeComplete */
        $handshakeComplete = (new ReflectionProperty(Session::class, 'handshakeComplete'))->getValue(self::sessionOf($connection));

        return self::heartbeatOf($connection)->nextActionAt(
            $handshakeComplete,
            $this->streamIdPoolOf($connection)->hasImmediate(),
        );
    }

    private function streamIdPoolOf(Connection $connection): StreamIdPool {
        /** @var StreamIdPool $pool */
        $pool = (new ReflectionProperty(Session::class, 'streamIds'))->getValue(self::sessionOf($connection));

        return $pool;
    }
}
