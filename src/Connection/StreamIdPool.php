<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use SplQueue;

/**
 * The stream ids of one connection: which are still to be handed out, which
 * have come back, and which are being held back.
 *
 * A stream id is only meaningful on the connection that handed it out, so this
 * belongs to the connection and starts over with it, see {@see self::reset()}.
 *
 * Nothing here waits. An id that cannot be had immediately can only come back
 * with an answer, and reading for one is {@see Session::getNewStreamId()}'s
 * business, not this pool's.
 *
 * An id is only ever given back if this pool has it outstanding and the caller
 * names the generation it was claimed under, see {@see self::$outstanding} and
 * {@see self::$generation}. Together those make the callers' bookkeeping safe to
 * get wrong: giving the same id back twice, giving back one claimed before the
 * pool was started over, and giving one back so late that the same number has
 * since been handed to somebody else are all passed over here, rather than left
 * to every caller to rule out on its own.
 * 
 * @internal this is not part of the public API and may change at any time
 */
final class StreamIdPool {
    /**
     * Highest stream id a client may use. The protocol carries it as a signed
     * [short] and reserves the negative half for server-initiated streams
     * (events use -1), leaving 0..32767 for requests.
     */
    public const MAX_STREAM_ID = 32767;

    /**
     * Which run of the id space we are on, bumped every time it is started
     * over. An id is only meaningful together with this: the numbers repeat on
     * every connection, so the generation is what tells an id claimed on the
     * connection that is gone from the same number handed out by the one that
     * replaced it.
     *
     * Callers keep the generation they claimed under and name it when they give
     * the id back, which is what lets a disposal arriving late — from code
     * unwinding out of the failure that replaced the connection — be recognised
     * as belonging to a pool that no longer exists.
     */
    private int $generation = 0;

    /**
     * Next stream id to hand out; the pool runs up to {@see self::MAX_STREAM_ID}
     * and then reuses ids released by answered requests.
     */
    private int $nextStreamId = 0;

    /**
     * @var array<int, float> $orphanedStreams stream ids of statements the
     * client gave up on, mapped to when that happened. They are deliberately
     * kept out of the recycling pool: the server may still answer on them, and
     * handing one to another request would resolve that request with the wrong
     * response. Each is released once its late answer finally arrives.
     */
    private array $orphanedStreams = [];

    /**
     * @var array<int, true> $outstanding the ids handed out and not yet given
     * back, which is what makes this pool the owner of them. Held so that
     * {@see self::release()} and {@see self::park()} can tell an id that is
     * theirs to dispose of from one that is not: a caller unwinding from a
     * failure may well try to give back an id it already gave back, and that
     * would put into circulation an id something else is already using. It also
     * means the recycling queue cannot come to hold the same id twice, since
     * nothing reaches it without being taken out of here first.
     *
     * This is emptied by {@see self::reset()}, so it settles the question
     * within one run of the id space; {@see self::$generation} settles it
     * across them.
     */
    private array $outstanding = [];

    /**
     * @var SplQueue<int> $recycledStreams
     */
    private SplQueue $recycledStreams;

    public function __construct() {
        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->recycledStreams = $recycledStreams;
    }

    /**
     * Take an id that is free right now, or null when none is.
     *
     * Both sources are consulted every time rather than settled once. The
     * recycling queue is the one that fills up while a caller waits, but the
     * counter is checked beside it because the id space starts over whenever
     * the connection is replaced — and then the counter has every id to give
     * again while the queue it was emptied alongside is still empty.
     */
    public function claim(): ?int {

        if ($this->nextStreamId <= self::MAX_STREAM_ID) {
            $streamId = $this->nextStreamId++;
        } elseif (!$this->recycledStreams->isEmpty()) {
            $streamId = $this->recycledStreams->dequeue();
        } else {
            return null;
        }

        $this->outstanding[$streamId] = true;

        return $streamId;
    }

    /**
     * Which run of the id space {@see self::claim()} is currently handing out,
     * to be kept by whoever claims and named again when they give the id back.
     */
    public function getGeneration(): int {

        return $this->generation;
    }

    public function getOrphanedCount(): int {

        return count($this->orphanedStreams);
    }

    /**
     * Whether an id can be had without waiting for one to come free.
     *
     * The heartbeat asks before sending: waiting for an id means reading, and
     * reading is exactly what the probe must not do to get itself sent.
     */
    public function hasImmediate(): bool {

        return $this->nextStreamId <= self::MAX_STREAM_ID || !$this->recycledStreams->isEmpty();
    }

    public function isOrphaned(int $streamId): bool {

        return isset($this->orphanedStreams[$streamId]);
    }

    /**
     * Hold an id back instead of recycling it, because it is unknown whether
     * the node is still going to answer on it.
     *
     * Ignored for an id this pool does not have outstanding, or one claimed
     * under an earlier generation; see {@see self::$outstanding} and
     * {@see self::$generation}.
     */
    public function park(int $streamId, int $generation): void {

        if (!$this->isOurs($streamId, $generation)) {
            return;
        }

        unset($this->outstanding[$streamId]);

        $this->orphanedStreams[$streamId] = microtime(true);
    }

    /**
     * Put an id back into circulation.
     *
     * Ignored for an id this pool does not have outstanding, or one claimed
     * under an earlier generation; see {@see self::$outstanding} and
     * {@see self::$generation}.
     */
    public function release(int $streamId, int $generation): void {

        if (!$this->isOurs($streamId, $generation)) {
            return;
        }

        unset($this->outstanding[$streamId]);

        $this->recycledStreams->enqueue($streamId);
    }

    /**
     * Release a parked id whose late answer has finally arrived, which is what
     * proves the node is done with it.
     *
     * A parked id is not an outstanding one — {@see self::park()} moved it — so
     * being parked here is what stands in for that check.
     */
    public function releaseParked(int $streamId): void {

        if (!isset($this->orphanedStreams[$streamId])) {
            return;
        }

        unset($this->orphanedStreams[$streamId]);

        $this->recycledStreams->enqueue($streamId);
    }

    /**
     * Start the id space over, for a connection that is going away.
     *
     * Nothing handed out by the old connection is outstanding any more, which
     * is what makes a stray release from the unwinding that follows harmless.
     */
    public function reset(): void {

        $this->generation++;
        $this->nextStreamId = 0;
        $this->orphanedStreams = [];
        $this->outstanding = [];

        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->recycledStreams = $recycledStreams;
    }

    /**
     * Whether this pool handed the id out and has not had it back, so that
     * giving it back now is this pool's business.
     *
     * The generation is checked first because it is the one of the two that
     * survives the pool being started over: an id claimed on the connection
     * that is gone may well match a number the new one has since handed to
     * somebody else, and would then look outstanding when it is somebody
     * else's.
     */
    private function isOurs(int $streamId, int $generation): bool {

        return $generation === $this->generation
            && isset($this->outstanding[$streamId]);
    }
}
