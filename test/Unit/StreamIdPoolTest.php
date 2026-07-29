<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\StreamIdPool;

/**
 * The rules by which a stream id is handed out and given back.
 *
 * The pool exists because a stream id handed to two requests at once is the
 * worst failure this driver has: the node answers on the id, and the answer
 * resolves whichever request happens to hold it, silently and with somebody
 * else's rows. Everything here is about making that impossible even when the
 * code giving an id back is wrong about whether it still owns it — which is the
 * normal case while unwinding out of a connection failure, where the pool has
 * already been started over underneath the code still returning from it.
 *
 * Two things settle ownership. Within one run of the id space it is the
 * outstanding set: an id not handed out cannot be given back. Across runs it is
 * the generation, because the numbers repeat — the id space starts at zero on
 * every connection, so an id from the old connection can otherwise match one the
 * new connection has already handed to somebody else.
 */
final class StreamIdPoolTest extends AbstractUnitTestCase {
    public function testAnIdClaimedBeforeTheSpaceStartedOverIsNotGivenBack(): void {
        $pool = new StreamIdPool();

        $stale = $pool->claim();
        self::assertNotNull($stale);
        $staleGeneration = $pool->generation();

        // The connection was replaced. What follows is the code unwinding out
        // of the failure that replaced it, still holding an id of the pool that
        // is gone.
        $pool->reset();

        $pool->release($stale, $staleGeneration);
        $this->exhaustCounter($pool);
        self::assertNull($pool->claim());
    }

    public function testAnIdClaimedBeforeTheSpaceStartedOverIsNotParked(): void {
        $pool = new StreamIdPool();

        $stale = $pool->claim();
        self::assertNotNull($stale);
        $staleGeneration = $pool->generation();

        $pool->reset();

        $pool->park($stale, $staleGeneration);

        self::assertSame(0, $pool->orphanedCount());
        self::assertFalse($pool->isOrphaned($stale));
    }

    public function testAParkedIdIsNotRecycledUntilItsAnswerArrives(): void {
        $pool = new StreamIdPool();

        $streamId = $pool->claim();
        self::assertNotNull($streamId);

        $pool->park($streamId, $pool->generation());

        self::assertTrue($pool->isOrphaned($streamId));
        self::assertSame(1, $pool->orphanedCount());

        $this->exhaustCounter($pool);

        // Releasing a parked id must not rescue it: what it is waiting for is
        // the node's late answer, which is the only thing that proves the node
        // is done with it.
        $pool->release($streamId, $pool->generation());
        self::assertNull($pool->claim());

        $pool->releaseParked($streamId);
        self::assertSame($streamId, $pool->claim());
        self::assertSame(0, $pool->orphanedCount());
    }

    public function testAReleasedIdIsHandedOutAgainExactlyOnce(): void {
        $pool = new StreamIdPool();

        $streamId = $pool->claim();
        self::assertSame(0, $streamId);
        self::assertSame(1, $pool->claim());

        $this->exhaustCounter($pool);

        $pool->release($streamId, $pool->generation());

        self::assertSame($streamId, $pool->claim());
        self::assertNull($pool->claim());
    }

    /**
     * The case the outstanding set alone cannot see, and the reason the
     * generation exists.
     *
     * An id given back so late that the pool has been started over *and* has
     * handed the same number out again looks, to a check on the number alone,
     * exactly like the request that now holds it. Recycling it there would put
     * a stream id that is in use back into circulation, and the node's answer
     * to the new request would go to whoever claimed it next.
     */
    public function testAStaleDisposalDoesNotRecycleAnIdTheNewSpaceHasHandedOut(): void {
        $pool = new StreamIdPool();

        $stale = $pool->claim();
        self::assertNotNull($stale);
        $staleGeneration = $pool->generation();

        $pool->reset();

        $inUse = $pool->claim();
        self::assertSame($stale, $inUse, 'the id space starts at zero again, so the number repeats');

        $this->exhaustCounter($pool);

        $pool->release($stale, $staleGeneration);

        self::assertNull($pool->claim(), 'an id in use on the new connection must not be handed out again');
    }

    public function testGenerationChangesWheneverTheIdSpaceStartsOver(): void {
        $pool = new StreamIdPool();

        $first = $pool->generation();

        $pool->reset();
        $second = $pool->generation();

        $pool->reset();
        $third = $pool->generation();

        self::assertNotSame($first, $second);
        self::assertNotSame($second, $third);
        self::assertNotSame($first, $third);
    }

    public function testGivingAnIdBackTwiceDoesNotHandItToTwoRequests(): void {
        $pool = new StreamIdPool();

        $streamId = $pool->claim();
        self::assertNotNull($streamId);

        $this->exhaustCounter($pool);

        $generation = $pool->generation();
        $pool->release($streamId, $generation);
        $pool->release($streamId, $generation);
        $pool->release($streamId, $generation);

        self::assertSame($streamId, $pool->claim());

        // The mistake the callers used to have to rule out one by one: a second
        // release would have queued the id again, and the next claim would have
        // handed a stream id to a request while another one was still using it.
        self::assertNull($pool->claim());
    }

    public function testHasImmediateReportsWhetherAClaimWouldHaveToWait(): void {
        $pool = new StreamIdPool();

        self::assertTrue($pool->hasImmediate());

        $streamId = $pool->claim();
        self::assertNotNull($streamId);

        $this->exhaustCounter($pool);
        self::assertFalse($pool->hasImmediate());

        $pool->release($streamId, $pool->generation());
        self::assertTrue($pool->hasImmediate());
    }

    public function testParkingIsIgnoredForAnIdThatWasNeverHandedOut(): void {
        $pool = new StreamIdPool();

        $pool->park(1234, $pool->generation());

        self::assertSame(0, $pool->orphanedCount());
        self::assertFalse($pool->isOrphaned(1234));
    }

    public function testReleaseParkedIsIgnoredForAnIdThatWasNeverParked(): void {
        $pool = new StreamIdPool();

        $this->exhaustCounter($pool);

        $pool->releaseParked(1234);

        self::assertNull($pool->claim());
    }

    public function testTheIdSpaceStopsAtTheHighestIdTheProtocolAllows(): void {
        $pool = new StreamIdPool();

        $claimed = 0;
        while ($pool->claim() !== null) {
            $claimed++;
        }

        self::assertSame(StreamIdPool::MAX_STREAM_ID + 1, $claimed);
    }

    /**
     * Drain the counter so that the next claim can only come from recycling.
     *
     * Note that claim() drains the recycling queue as well, so this has to run
     * before whatever release is under test rather than after it.
     */
    private function exhaustCounter(StreamIdPool $pool): void {

        while ($pool->claim() !== null) {
        }
    }
}
