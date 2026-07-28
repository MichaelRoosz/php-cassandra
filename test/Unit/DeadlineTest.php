<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\Deadline;
use Cassandra\Exception\ConnectionException;

/**
 * The rules by which a request timeout becomes the absolute deadline a wait is
 * bounded by.
 *
 * The case that most needs pinning down is INF: it is a legitimate timeout, and
 * it means the same unbounded wait that null does. It has to be normalised to
 * null rather than carried through as a deadline no clock reaches, because
 * everything downstream tests for null to decide whether anything bounds the
 * wait at all — and a wait with nothing bounding it is what leaves the
 * transport's stall window as the last judgement on a connection that died with
 * heartbeats switched off.
 */
final class DeadlineTest extends AbstractUnitTestCase {
    public function testAtCountsAnExplicitTimeoutFromWhenTheRequestWasSent(): void {
        $deadlines = new Deadline(30.0);

        $sentAt = microtime(true) - 100.0;

        $this->assertSame($sentAt + 5.0, $deadlines->at(5.0, $sentAt));
    }

    public function testAtFallsBackToTheConnectionDefault(): void {
        $deadlines = new Deadline(30.0);

        $sentAt = microtime(true);

        $this->assertSame($sentAt + 30.0, $deadlines->at(null, $sentAt));
        $this->assertSame($sentAt + 5.0, $deadlines->at(5.0, $sentAt));
    }

    public function testAtTreatsInfiniteTimeoutsAsUnbounded(): void {
        $sentAt = microtime(true);

        $this->assertNull((new Deadline(30.0))->at(INF, $sentAt));

        // Both ways of spelling an unbounded wait have to produce the same
        // deadline, whether the INF came from the call or from the connection.
        $this->assertNull((new Deadline(INF))->at(null, $sentAt));
        $this->assertNull((new Deadline(null))->at(null, $sentAt));
    }

    public function testAtTreatsNegativeInfinityAsADeadlineAlreadyPassed(): void {
        $deadlines = new Deadline(30.0);

        $sentAt = microtime(true);

        // -INF asks for a wait that is already over, which is the opposite of
        // what INF asks for and must not be normalised away with it.
        $this->assertSame($sentAt, $deadlines->at(-INF, $sentAt));
    }

    public function testAtWithoutConnectionDefaultIsUnbounded(): void {
        $this->assertNull((new Deadline(null))->at(null));
    }

    public function testDescribeSpellsOutInfinitiesForExceptionContexts(): void {
        $deadlines = new Deadline(30.0);

        $this->assertNull($deadlines->describe(null));
        $this->assertSame(5.0, $deadlines->describe(5.0));
        $this->assertSame('INF', $deadlines->describe(INF));
        $this->assertSame('-INF', $deadlines->describe(-INF));
    }

    public function testEarlierTreatsNullAsNoBound(): void {
        $deadlines = new Deadline(30.0);

        $this->assertNull($deadlines->earlier(null, null));
        $this->assertSame(3.0, $deadlines->earlier(null, 3.0));
        $this->assertSame(3.0, $deadlines->earlier(3.0, null));
        $this->assertSame(3.0, $deadlines->earlier(7.0, 3.0));
    }

    public function testInClampsANegativeWaitToNow(): void {
        $deadlines = new Deadline(30.0);

        $before = microtime(true);
        $deadline = $deadlines->in(-5.0);
        $after = microtime(true);

        $this->assertNotNull($deadline);
        $this->assertGreaterThanOrEqual($before, $deadline);
        $this->assertLessThanOrEqual($after, $deadline);
    }

    public function testInIgnoresTheConnectionDefault(): void {
        $deadlines = new Deadline(30.0);

        // Null here means "no bound", not "fall back to the connection
        // default": the waits that use this are bounded by the budgets of the
        // statements they were given.
        $this->assertNull($deadlines->in(null));

        $deadline = $deadlines->in(5.0);
        $this->assertNotNull($deadline);
        $this->assertLessThan(microtime(true) + 30.0, $deadline);
    }

    public function testInTreatsInfinityAsUnbounded(): void {
        $this->assertNull((new Deadline(30.0))->in(INF));
    }

    public function testNonPositiveRequestTimeoutsAreRejected(): void {
        $deadlines = new Deadline(30.0);

        $deadlines->assertValidRequestTimeout(null, 'test');
        $deadlines->assertValidRequestTimeout(INF, 'test');
        $deadlines->assertValidRequestTimeout(0.5, 'test');

        $this->expectException(ConnectionException::class);
        $deadlines->assertValidRequestTimeout(0.0, 'test');
    }
}
