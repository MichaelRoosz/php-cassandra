<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\HeartbeatMonitor;
use Cassandra\Connection\Session;
use Cassandra\Connection\StatementRegistry;
use PHPUnit\Framework\TestCase;
use ReflectionProperty;

abstract class AbstractUnitTestCase extends TestCase {
    public function integerHasAtLeast64Bits(): bool {
        return PHP_INT_SIZE >= 8;
    }

    /**
     * The heartbeat state of a connection: whether a probe is outstanding, and
     * when the next one is due.
     */
    protected static function heartbeatOf(Connection $connection): HeartbeatMonitor {
        /** @var HeartbeatMonitor $heartbeat */
        $heartbeat = (new ReflectionProperty(Session::class, 'heartbeat'))->getValue(self::sessionOf($connection));

        return $heartbeat;
    }

    /**
     * Deterministic stand-in for random_bytes(). The output is incompressible
     * for practical purposes but fully reproducible from the seed, so a test
     * failure on generated input can always be replayed.
     */
    protected static function pseudoRandomBytes(int $length, int $seed): string {
        $output = '';
        $counter = 0;

        while (strlen($output) < $length) {
            $output .= hash('sha256', $seed . ':' . $counter, true);
            $counter++;
        }

        return substr($output, 0, $length);
    }

    /**
     * The machinery behind a connection, which is where everything a test wants
     * to look at from the inside lives: the statements in flight, the stream id
     * pool, the heartbeat and the negotiated protocol version.
     */
    protected static function sessionOf(Connection $connection): Session {
        /** @var Session $session */
        $session = (new ReflectionProperty(Connection::class, 'session'))->getValue($connection);

        return $session;
    }

    /**
     * The requests a connection has in flight.
     */
    protected static function statementsOf(Connection $connection): StatementRegistry {
        /** @var StatementRegistry $statements */
        $statements = (new ReflectionProperty(Session::class, 'statements'))->getValue(self::sessionOf($connection));

        return $statements;
    }
}
