<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Statement;

/**
 * Turns the request timeouts a connection works with into the absolute
 * deadlines its waits are bounded by, and keeps the connection default they
 * fall back to.
 *
 * A deadline is an absolute microtime (the scale {@see microtime()} returns for
 * `true`), and null throughout means "no bound", i.e. wait for as long as it
 * takes.
 */
final class Deadline {
    private ?float $requestTimeout;

    public function __construct(?float $requestTimeoutInSeconds) {
        $this->requestTimeout = $requestTimeoutInSeconds;
    }

    /**
     * Reject a request timeout no caller can have meant.
     *
     * Zero or less would put the request out of time before it was sent — the
     * same judgement {@see ConnectionOptions} makes about its own default and
     * {@see \Cassandra\Request\Options\RequestOptions} about the one a request
     * carries. Without this the value would be normalised away by
     * {@see self::at()} and silently expire every request it applies to, which
     * through {@see self::setRequestTimeout()} includes the ones already in
     * flight. "Use the connection default" is spelled null for the per-call
     * argument, and an unbounded wait is null on the connection.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function assertValidRequestTimeout(?float $requestTimeoutInSeconds, string $operation): void {

        if ($requestTimeoutInSeconds === null || $requestTimeoutInSeconds > 0.0) {
            return;
        }

        throw new ConnectionException(
            'Invalid request timeout: it must be greater than zero, or null to fall back to the request options and the connection default',
            ExceptionCode::CONNECTION_INVALID_REQUEST_TIMEOUT->value,
            [
                'operation' => $operation,
                'request_timeout_seconds' => $requestTimeoutInSeconds,
            ]
        );
    }

    /**
     * Absolute microtime at which a wait must give up, or null to wait forever.
     *
     * The budget runs from $sentAt — when the request was handed to the node —
     * rather than from now, so that an async statement gets the same total
     * allowance no matter how long the caller took to start waiting for it.
     * Waits that are not tied to a request (the sync path, which writes
     * immediately before waiting) count from now.
     *
     * INF is normalised to null rather than carried through as a deadline no
     * clock reaches, so that the two ways of spelling an unbounded wait are one
     * and the same downstream. Carried through it would be a bound everywhere
     * that only tests for null — {@see Session::readResponseUntil()} above all,
     * where a wait with nothing bounding it is what makes the transport's stall
     * window the last judgement available, so an INF wait would swallow that
     * timeout and never notice a connection that died with heartbeats off.
     * -INF is a different matter and is left to be clamped below: it asks for a
     * wait that is already over.
     */
    public function at(?float $timeoutInSeconds, ?float $sentAt = null): ?float {

        $timeout = $timeoutInSeconds ?? $this->requestTimeout;

        if ($timeout === null || $timeout === INF) {
            return null;
        }

        return ($sentAt ?? microtime(true)) + max(0.0, $timeout);
    }

    /**
     * A timeout as it goes into an exception's context.
     *
     * INF is a legitimate value — {@see self::assertValidRequestTimeout()}
     * takes it and {@see self::at()} turns it into a wait that never ends — but
     * it has no JSON representation, so an exception carrying it cannot be
     * serialised by whatever the application logs with. It is spelled out
     * instead.
     */
    public function describe(?float $timeoutInSeconds): null|float|string {

        if ($timeoutInSeconds === null || is_finite($timeoutInSeconds)) {
            return $timeoutInSeconds;
        }

        return $timeoutInSeconds > 0.0 ? 'INF' : '-INF';
    }

    /**
     * The earlier of two deadlines, either of which may be null for "no bound".
     */
    public function earlier(?float $a, ?float $b): ?float {

        if ($a === null) {
            return $b;
        }

        if ($b === null) {
            return $a;
        }

        return min($a, $b);
    }

    /**
     * Earliest deadline among the statements still waiting for an answer, so a
     * wait over several statements ends as soon as the first of them has used
     * up its budget. Null when none of them is bounded — nothing is pending, or
     * every pending statement waits indefinitely.
     *
     * Each statement is held to the timeout its own request asked for. One that
     * waits indefinitely is passed over rather than making the whole result
     * unbounded: it has no deadline of its own to contribute, but it must not
     * cost the statements beside it theirs, or a single unbounded request would
     * keep every other one from ever being noticed as overdue. A timeout of INF
     * is passed over by the same test, {@see self::at()} having already
     * normalised it to the unbounded wait it asks for.
     *
     * $ignored is the driver's own heartbeat: it is not one of the caller's
     * requests and is held to
     * {@see ConnectionOptions::$heartbeatTimeoutInSeconds} by
     * {@see Session::checkHeartbeat()} instead of to a request budget.
     *
     * @param array<Statement> $statements
     */
    public function earliestForStatements(array $statements, ?Statement $ignored = null): ?float {

        $earliest = null;

        foreach ($statements as $statement) {
            if ($statement->isResultReady() || $statement === $ignored) {
                continue;
            }

            $deadline = $this->at(
                $statement->getRequestTimeout(),
                $statement->getSentAt(),
            );
            if ($deadline === null) {
                continue;
            }

            if ($earliest === null || $deadline < $earliest) {
                $earliest = $deadline;
            }
        }

        return $earliest;
    }

    /**
     * The connection default, i.e. the timeout applied to every request that
     * has none of its own.
     */
    public function getRequestTimeout(): ?float {

        return $this->requestTimeout;
    }

    /**
     * The counterpart of {@see self::at()} for the waits whose null already
     * means "no bound" rather than "fall back to the connection default" — the
     * ones that take statements or wait for an event, which are bounded by the
     * budgets of the statements they were given rather than by a timeout of
     * their own.
     *
     * INF is normalised to null here as well, so that the two ways of spelling
     * an unbounded wait behave alike for the reason {@see self::at()} gives.
     */
    public function in(?float $timeoutInSeconds): ?float {

        if ($timeoutInSeconds === null || $timeoutInSeconds === INF) {
            return null;
        }

        return microtime(true) + max(0.0, $timeoutInSeconds);
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function setRequestTimeout(?float $requestTimeoutInSeconds): void {

        $this->assertValidRequestTimeout($requestTimeoutInSeconds, 'setRequestTimeout');

        $this->requestTimeout = $requestTimeoutInSeconds;
    }
}
