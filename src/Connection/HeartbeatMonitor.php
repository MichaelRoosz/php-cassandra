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
     * When the node last sent us anything, used to decide whether an idle
     * connection needs a heartbeat.
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
     * id space is still exhausted then does the wait go unbounded, and that
     * leaves the transport's stall window as the only judgement — so a
     * connection configured without one ({@see SocketNodeConfig} with
     * SO_RCVTIMEO zeroed, {@see StreamNodeConfig} with a non-positive timeout)
     * has, in that one corner, nothing bounding it at all.
     */
    public function getNextActionAt(bool $handshakeComplete, bool $streamIdAvailable): ?float {

        $interval = $this->options->heartbeatIntervalInSeconds;

        if ($this->isDormant($handshakeComplete) || $interval === null) {
            return null;
        }

        if ($this->probe !== null) {
            return $this->probeSentAt + $this->options->heartbeatTimeoutInSeconds;
        }

        $dueAt = $this->lastResponseAt + $interval;

        if ($dueAt <= microtime(true) && !$streamIdAvailable) {
            return null;
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
     * Inclusive, as {@see self::isProbeDue()} is, so that the instant
     * {@see self::getNextActionAt()} wakes a read for is one this already has
     * an answer for. Strictly greater would send the wait back into a read
     * whose bound has already passed, to come straight back and ask again.
     */
    public function isProbeOverdue(float $now): bool {

        return $now - $this->probeSentAt >= $this->options->heartbeatTimeoutInSeconds;
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
     * The node said something, whatever it was, so the interval starts over.
     */
    public function recordResponse(): void {

        $this->lastResponseAt = microtime(true);
    }
}
