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
 */
final class StreamIdPool {
    /**
     * Highest stream id a client may use. The protocol carries it as a signed
     * [short] and reserves the negative half for server-initiated streams
     * (events use -1), leaving 0..32767 for requests.
     */
    public const MAX_STREAM_ID = 32767;

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
            return $this->nextStreamId++;
        }

        if (!$this->recycledStreams->isEmpty()) {
            return $this->recycledStreams->dequeue();
        }

        return null;
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

    public function orphanedCount(): int {

        return count($this->orphanedStreams);
    }

    /**
     * Hold an id back instead of recycling it, because it is unknown whether
     * the node is still going to answer on it.
     */
    public function park(int $streamId): void {

        $this->orphanedStreams[$streamId] = microtime(true);
    }

    /**
     * Put an id back into circulation.
     */
    public function release(int $streamId): void {

        $this->recycledStreams->enqueue($streamId);
    }

    /**
     * Release a parked id whose late answer has finally arrived, which is what
     * proves the node is done with it.
     */
    public function releaseParked(int $streamId): void {

        unset($this->orphanedStreams[$streamId]);
        $this->recycledStreams->enqueue($streamId);
    }

    /**
     * Start the id space over, for a connection that is going away.
     */
    public function reset(): void {

        $this->nextStreamId = 0;
        $this->orphanedStreams = [];

        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->recycledStreams = $recycledStreams;
    }
}
