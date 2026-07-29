<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\Deadline;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StatementRegistry;
use Cassandra\Connection\StreamIdPool;
use Cassandra\Request\Query;
use Cassandra\Statement;

/**
 * The earliest deadline among the requests in flight, which is what every wait
 * bounds its reads by so that a request going overdue is noticed while somebody
 * is waiting for something else.
 *
 * The answer is remembered between calls, because working it out means walking
 * every statement in flight and every pass of every wait asks for it. That makes
 * the tests here about one thing above all: a remembered deadline that has gone
 * stale is a request that never times out, so each way of moving it has to be
 * shown to move it.
 *
 * Nothing here touches the network. A Connection is built but never connected —
 * a Statement needs one to hand back to whoever waits on it, and none of that is
 * reached.
 */
final class StatementRegistryDeadlineTest extends AbstractUnitTestCase {
    /**
     * When every statement built here was sent. Anchored to the real clock so
     * that a budget of a few seconds lands in the future and one anchored in the
     * past is genuinely overdue — expire() reads the same clock.
     */
    private float $sentAt;

    protected function setUp(): void {
        $this->sentAt = microtime(true);
    }

    public function testAbandoningEverythingLeavesNoDeadline(): void {
        $deadlines = new Deadline(null);
        $registry = new StatementRegistry(new StreamIdPool(), $deadlines);

        $registry->register(1, $this->statement(30.0));
        $this->assertSame($this->sentAt + 30.0, $registry->getEarliestDeadline(null));

        $registry->abandonAll();

        $this->assertNull($registry->getEarliestDeadline(null));
    }

    public function testAChangedConnectionDefaultMovesTheDeadline(): void {
        // The statement asked for no timeout of its own, so it is measured
        // against the connection default as it stands when it is looked at —
        // which setRequestTimeout() can change under a request already in
        // flight.
        $deadlines = new Deadline(30.0);
        $registry = new StatementRegistry(new StreamIdPool(), $deadlines);

        $registry->register(1, $this->statement(null));
        $this->assertSame($this->sentAt + 30.0, $registry->getEarliestDeadline(null));

        $deadlines->setRequestTimeout(5.0);
        $this->assertSame($this->sentAt + 5.0, $registry->getEarliestDeadline(null));

        $deadlines->setRequestTimeout(null);
        $this->assertNull($registry->getEarliestDeadline(null));
    }

    public function testARestartedBudgetMovesTheDeadline(): void {
        // What a chained follow-up request does: the statement keeps its stream
        // id but starts a new wait, so it is registered again with a later send
        // time. The order matters — the registry is what notices the change, so
        // the clock is restarted before the statement goes back in.
        $deadlines = new Deadline(null);
        $registry = new StatementRegistry(new StreamIdPool(), $deadlines);

        $statement = $this->statement(30.0);

        $registry->register(1, $statement);
        $this->assertSame($this->sentAt + 30.0, $registry->getEarliestDeadline(null));

        $registry->forget(1);
        $statement->setSentAt($this->sentAt + 100.0);
        $registry->register(1, $statement);

        $this->assertSame($this->sentAt + 130.0, $registry->getEarliestDeadline(null));
    }

    public function testExpiringAStatementMovesTheDeadlineOn(): void {
        $deadlines = new Deadline(null);
        $registry = new StatementRegistry(new StreamIdPool(), $deadlines);

        // Long past its budget, so expire() gives up on it.
        $overdue = $this->statement(1.0);
        $overdue->setSentAt(microtime(true) - 3600.0);

        $registry->register(1, $overdue);
        $registry->register(2, $this->statement(30.0));

        $this->assertSame($overdue->getSentAt() + 1.0, $registry->getEarliestDeadline(null));

        $this->assertSame([$overdue], $registry->expire(null));

        $this->assertSame($this->sentAt + 30.0, $registry->getEarliestDeadline(null));
    }

    public function testForgettingTheEarliestStatementMovesTheDeadlineOn(): void {
        $deadlines = new Deadline(null);
        $registry = new StatementRegistry(new StreamIdPool(), $deadlines);

        $registry->register(1, $this->statement(5.0));
        $registry->register(2, $this->statement(30.0));

        $this->assertSame($this->sentAt + 5.0, $registry->getEarliestDeadline(null));

        $registry->forget(1);

        $this->assertSame($this->sentAt + 30.0, $registry->getEarliestDeadline(null));
    }

    public function testHasPendingPassesOverTheIgnoredStatement(): void {
        $registry = new StatementRegistry(new StreamIdPool(), new Deadline(null));

        $probe = $this->statement(null);

        $this->assertFalse($registry->hasPending(null));

        $registry->register(1, $probe);
        $this->assertTrue($registry->hasPending(null));
        $this->assertFalse($registry->hasPending($probe));

        $registry->register(2, $this->statement(null));
        $this->assertTrue($registry->hasPending($probe));
    }

    public function testRegisteringAnEarlierStatementMovesTheDeadline(): void {
        $deadlines = new Deadline(null);
        $registry = new StatementRegistry(new StreamIdPool(), $deadlines);

        $registry->register(1, $this->statement(30.0));
        $this->assertSame($this->sentAt + 30.0, $registry->getEarliestDeadline(null));

        $registry->register(2, $this->statement(5.0));
        $this->assertSame($this->sentAt + 5.0, $registry->getEarliestDeadline(null));
    }

    public function testTheHeartbeatProbeIsPassedOverWhicheverWayItIsAsked(): void {
        // The probe is held to a timeout of its own rather than to a request
        // budget, so it must not contribute a deadline — and asking with and
        // without it has to keep giving the two different answers rather than
        // one remembered one.
        $deadlines = new Deadline(null);
        $registry = new StatementRegistry(new StreamIdPool(), $deadlines);

        $probe = $this->statement(5.0);

        $registry->register(1, $probe);
        $registry->register(2, $this->statement(30.0));

        $this->assertSame($this->sentAt + 5.0, $registry->getEarliestDeadline(null));
        $this->assertSame($this->sentAt + 30.0, $registry->getEarliestDeadline($probe));
        $this->assertSame($this->sentAt + 5.0, $registry->getEarliestDeadline(null));
    }

    private function statement(?float $requestTimeoutInSeconds): Statement {
        $statement = new Statement(
            connection: new Connection([new SocketNodeConfig()]),
            streamId: 0,
            streamGeneration: 0,
            request: new Query('SELECT * FROM t'),
            requestTimeoutInSeconds: $requestTimeoutInSeconds,
        );

        $statement->setSentAt($this->sentAt);

        return $statement;
    }
}
