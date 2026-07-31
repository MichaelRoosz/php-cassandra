<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Statement;

/**
 * When a silent connection needs to be asked whether it is still there, and
 * whether the last such question was answered.
 *
 * Nothing else can tell a dead connection from a quiet one: a transport read
 * timeout cannot distinguish "the node has nothing to say" from "the node is
 * gone", and neither can a request timeout — a coordinator that is still
 * thinking looks exactly like a connection that died. So once the connection
 * has been silent for the heartbeat interval, {@see Session::checkHeartbeat()}
 * sends an OPTIONS request; if its answer does not arrive within the heartbeat
 * timeout, the connection is treated as dead.
 *
 * This class keeps the timing and the state of that probe. Sending it, and
 * failing the connection when it goes unanswered, is the session's — the two
 * are deliberately apart, because the decisions here are made from inside the
 * read loop that the sending must not re-enter.
 *
 * @internal this is not part of the public API and may change at any time
 */
final class HeartbeatMonitor {
    /**
     * When the node last gave us anything at all — a whole response, or merely
     * some of the bytes of one — used to decide whether an outstanding probe
     * has really gone unanswered or is simply queued behind a transfer that is
     * still arriving.
     *
     * Kept apart from {@see self::$lastResponseAt} because the two answer
     * different questions. Whether the connection is idle enough to be worth
     * probing is about completed responses: bytes trickling in mid-frame are
     * not the node saying anything. Whether an unanswered probe means the
     * connection is dead is about bytes: a response larger than the heartbeat
     * timeout is long produces no completed frame for longer than the timeout,
     * and the probe's own answer cannot overtake it — frames are serialised on
     * one socket, so a SUPPORTED queued behind a large page arrives after it.
     * Judged on responses alone, a connection delivering a big result at full
     * speed is indistinguishable from one that died.
     */
    private float $lastProgressAt = 0.0;

    /**
     * When the node last sent us a complete response, used to decide whether an
     * idle connection needs a heartbeat.
     */
    private float $lastResponseAt = 0.0;

    private ConnectionOptions $options;

    /**
     * @var ?Statement $probe the OPTIONS request sent to prove an idle
     * connection is still alive, while its answer is outstanding
     */
    private ?Statement $probe = null;

    private float $probeSentAt = 0.0;

    /**
     * Whether a probe is currently being sent, so that nothing reached from
     * inside {@see Session::checkHeartbeat()} can start a second one before the
     * first has been recorded as pending.
     *
     * Defensive as things stand: sending a probe would only re-enter there by
     * reading, and neither step of it that can read is ever reached. Claiming a
     * stream id is skipped because checkHeartbeat() sends no probe unless
     * {@see StreamIdPool::hasImmediate()} says an id can be had without waiting,
     * and opening a connection is skipped because checkHeartbeat() sends none
     * before the handshake is through, which no connection the session has
     * dropped is. Kept because both of those are properties of other methods
     * agreeing with this one rather than of this one alone, and the cost of
     * being wrong about them is a probe sent on every read.
     */
    private bool $sending = false;

    public function __construct(ConnectionOptions $options) {
        $this->options = $options;
    }

    /**
     * Start the interval from now, for a connection that has just been opened.
     *
     * Anchors it to a fresh connection rather than to whenever this object last
     * heard from a node.
     */
    public function anchor(): void {

        $this->lastResponseAt = microtime(true);
        $this->lastProgressAt = $this->lastResponseAt;
    }

    public function beginSending(): void {

        $this->sending = true;
    }

    public function endSending(): void {

        $this->sending = false;
    }

    /**
     * Forget the outstanding probe, for a connection that is going away or one
     * whose probe has just been answered.
     */
    public function forgetProbe(): void {

        $this->probe = null;
    }

    /**
     * When {@see Session::checkHeartbeat()} next has something to do, or null
     * when heartbeats are off or cannot run yet.
     *
     * Reads are bounded by this as well as by the caller's deadline, so that a
     * wait which is otherwise unbounded — for events, say — still returns in
     * time to send the probe or to declare an unanswered one dead. It mirrors
     * the conditions checkHeartbeat() applies rather than second-guessing them:
     * waking early only costs a read that finds nothing, but waking late would
     * delay the probe by however long the read blocked.
     *
     * @param bool $streamIdAvailable whether an id can be had without waiting;
     * checkHeartbeat() will not send a probe it would have to wait for one to
     * send. Once the probe is due and no id can be had, there is nothing left
     * to come up for air for: reporting a bound anyway would put every read's
     * bound in the past — the probe is due, after all — and turn each wait into
     * a spin over reads that return at once and a probe that is never sent.
     *
     * A probe that is not due yet is a different matter, and is still worth
     * reporting: the bound is in the future, so it costs no spin, and an id may
     * well have come free by the time a read comes back for it. Only where the
     * id space is still exhausted then does the wait go unbounded.
     *
     * Reporting no bound is not free, though, and this is the one place that
     * hands {@see Session::readResponseUntil()} a null it did not get from the
     * caller. A read with nothing bounding it is exactly the state in which that
     * method treats the transport's stall window as the last judgement available
     * and fails the connection over it — so a client that has every stream id in
     * flight, all of them unbounded, and is waiting without a deadline of its
     * own can lose the connection to a quiet moment that is its own backlog
     * rather than the node's fault. It takes all three at once: one bounded
     * request in flight, or a caller wait with a timeout, puts a bound back and
     * the stall window goes back to meaning nothing. At the other end, a
     * connection configured without a stall window at all
     * ({@see SocketNodeConfig} with SO_RCVTIMEO zeroed, {@see StreamNodeConfig}
     * with a non-positive timeout) has, in that same corner, nothing bounding it
     * whatsoever.
     */
    public function getNextActionAt(bool $handshakeComplete, bool $streamIdAvailable): ?float {

        $interval = $this->options->heartbeatIntervalInSeconds;

        if ($this->isDormant($handshakeComplete) || $interval === null) {
            return null;
        }

        if ($this->probe !== null && !$this->probe->isResultReady()) {
            return $this->probeDeadline();
        }

        $now = microtime(true);
        $dueAt = $this->lastResponseAt + $interval;

        if ($dueAt <= $now && !$streamIdAvailable) {
            // The probe is due and cannot be sent, so there is nothing to wake
            // a read for at $dueAt — it is already past, and reporting it would
            // put every read's bound in the past and turn each wait into a spin
            // over reads that return at once and a probe that is never sent.
            //
            // An interval from now rather than no bound at all, though. A bound
            // in the future costs one wake per interval and no spin, and it
            // keeps {@see Session::readResponseUntil()} from reading with
            // nothing bounding it — which is the state in which that method
            // treats the transport's stall window as the last judgement and
            // fails the connection over it. A client with every stream id in
            // flight, all of them unbounded, waiting without a deadline of its
            // own would otherwise lose the connection to a quiet moment that is
            // its own backlog rather than the node's fault.
            return $now + $interval;
        }

        return $dueAt;
    }

    /**
     * The probe whose answer is outstanding, if there is one.
     */
    public function getProbe(): ?Statement {

        return $this->probe;
    }

    public function getTimeoutInSeconds(): float {

        return $this->options->heartbeatTimeoutInSeconds;
    }

    /**
     * Whether there is nothing for the heartbeat to do at all: it is switched
     * off, the connection cannot carry ordinary requests yet, or a probe is
     * being sent right now.
     *
     * The handshake itself waits for responses, but until it is through the
     * node accepts nothing besides the handshake requests.
     */
    public function isDormant(bool $handshakeComplete): bool {

        return $this->options->heartbeatIntervalInSeconds === null
            || !$handshakeComplete
            || $this->sending;
    }

    /**
     * Whether the connection has now been silent for longer than the interval.
     */
    public function isProbeDue(float $now): bool {

        $interval = $this->options->heartbeatIntervalInSeconds;

        return $interval !== null && $now - $this->lastResponseAt >= $interval;
    }

    /**
     * Whether the outstanding probe has now gone unanswered for longer than the
     * heartbeat timeout, i.e. whether the connection is to be treated as dead.
     *
     * Measured from the last sign of life rather than from the probe alone, see
     * {@see self::probeDeadline()}.
     *
     * Inclusive, as {@see self::isProbeDue()} is, so that the instant
     * {@see self::getNextActionAt()} wakes a read for is one this already has
     * an answer for. Strictly greater would send the wait back into a read
     * whose bound has already passed, to come straight back and ask again.
     */
    public function isProbeOverdue(float $now): bool {

        return $now >= $this->probeDeadline();
    }

    /**
     * Take the probe that has just been written as the outstanding one.
     *
     * Anchored to when the OPTIONS was actually written, not to when sending it
     * was decided on: getting that far can wait for a stream id to come free,
     * and that is the client's own backlog rather than time the node spent not
     * answering. Charging it to the heartbeat budget would let a busy client
     * declare a healthy node dead the moment the probe goes out.
     */
    public function recordProbe(Statement $probe): void {

        $this->probeSentAt = $probe->getSentAt();
        $this->probe = $probe;
    }

    /**
     * Bytes arrived from the node, whether or not they completed a response.
     *
     * That is not the node saying anything — the interval is left alone, so a
     * connection dribbling out one large answer is still probed on schedule —
     * but it is proof that the connection is carrying data, which is the whole
     * of what an outstanding probe is asking about; see
     * {@see self::$lastProgressAt}.
     */
    public function recordProgress(): void {

        $this->lastProgressAt = microtime(true);
    }

    /**
     * The node said something, whatever it was, so the interval starts over.
     */
    public function recordResponse(): void {

        $this->lastResponseAt = microtime(true);
        $this->lastProgressAt = $this->lastResponseAt;
    }

    /**
     * When an outstanding probe is to be given up on, and the connection with
     * it.
     *
     * The heartbeat timeout runs from the last sign of life rather than from
     * when the probe went out, so a connection that is demonstrably carrying
     * data is never declared dead. A dead one produces no progress at all, so
     * the deadline stays at $probeSentAt + timeout and fires exactly as before.
     *
     * Progress from before the probe was sent does not count: it says nothing
     * about a question that had not been asked yet.
     */
    private function probeDeadline(): float {

        return max($this->probeSentAt, $this->lastProgressAt) + $this->options->heartbeatTimeoutInSeconds;
    }
}
