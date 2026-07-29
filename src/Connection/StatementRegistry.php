<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\StatementException;
use Cassandra\Statement;
use Cassandra\StatementStatus;

/**
 * The requests this connection has in flight, keyed by the stream id each was
 * sent on, together with the budgets they are held to.
 *
 * Registering a statement here is what makes a stream id "in use": the id is
 * only free again once the statement has been answered, given up on, or
 * abandoned with the connection, and each of those goes through this class so
 * that the id ends up in the right place — back in circulation, or parked until
 * a late answer proves the node is done with it.
 *
 * The driver's own heartbeat is a statement like any other, but it is not one
 * of the caller's: every method that reasons about budgets takes it as $ignored
 * and passes over it, because {@see HeartbeatMonitor} holds it to a timeout of
 * its own instead. Timing it out here would report it to a caller who never
 * sent it, and would park a stream id every heartbeat interval whenever the
 * request timeout is the shorter of the two.
 *
 * @internal this is not part of the public API and may change at any time
 */
final class StatementRegistry {
    private Deadline $deadlines;

    /**
     * The last result of {@see self::getEarliestDeadline()}, together with what
     * it was computed from.
     *
     * Every pass of every wait asks for this, and answering means walking all
     * the statements in flight — which on a connection with thousands of async
     * requests outstanding makes draining them quadratic. It is therefore
     * remembered until something it depends on changes, which is what
     * {@see self::$revision} tracks; the deadlines' own revision is part of the
     * key because the connection default they fall back to can be changed under
     * them, see {@see Deadline::getRevision()}.
     *
     * @var ?array{revision: int, deadlineRevision: int, ignored: ?Statement, deadline: ?float}
     */
    private ?array $earliestDeadlineCache = null;

    /**
     * Bumped whenever anything the earliest deadline is computed from changes:
     * which statements are in flight, and when each of them was sent.
     *
     * Deliberately not a matter of the statements telling us: a statement knows
     * nothing about the registry it is in, and a budget that silently went stale
     * would be a request that never times out. Every mutation goes through this
     * class, bar the one where a follow-up request restarts a statement's clock,
     * which {@see self::register()} covers because the follow-up is registered
     * afterwards, see {@see RequestExecutor::chainAsyncRequest()}.
     */
    private int $revision = 0;

    /**
     * @var array<int, Statement> $statements keyed by the stream id each was sent on
     */
    private array $statements = [];

    private StreamIdPool $streamIds;

    public function __construct(StreamIdPool $streamIds, Deadline $deadlines) {
        $this->streamIds = $streamIds;
        $this->deadlines = $deadlines;
    }

    /**
     * Give up on everything in flight, for a connection that is going away.
     *
     * Stream ids are only meaningful on the connection that handed them out, so
     * anything still waiting can never be answered now. Marking them lets a
     * later access fail immediately and accurately, instead of waiting out a
     * request timeout for an answer that cannot come.
     */
    public function abandonAll(): void {

        foreach ($this->statements as $statement) {
            $statement->setStatus(StatementStatus::ABANDONED);
        }

        $this->statements = [];
        $this->revision++;
    }

    /**
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\StatementException
     */
    public function assertResolvable(Statement $statement): void {

        if ($statement->isTimedOut()) {
            throw new RequestTimeoutException(
                'This request timed out and was given up on, so it can no longer be resolved. Send it again.',
                ExceptionCode::REQUESTTIMEOUT_WAITING_FOR_STATEMENTS->value,
                [
                    'stream_id' => $statement->getStreamId(),
                    'request_class' => get_class($statement->getRequest()),
                ],
                timedOutStatements: [$statement],
            );
        }

        if ($statement->isAbandoned()) {
            throw new StatementException(
                'This statement was given up on before it could be answered — the connection it was sent on was closed, a follow-up request of its own never reached the node, or its answer could not be handled — so it can no longer be resolved. Send the request again.',
                ExceptionCode::STATEMENT_ABANDONED->value,
                [
                    'stream_id' => $statement->getStreamId(),
                    'request_class' => get_class($statement->getRequest()),
                    'reason' => 'connection_closed_request_not_sent_or_response_not_handled',
                ]
            );
        }

        // Pending, but not on this connection: a statement from another
        // Connection, or one left over from before this one was replaced.
        // Reading here would never resolve it — no answer for it can arrive on
        // this socket — and it contributes no deadline of its own either, since
        // a wait bounds itself by the requests this connection has in flight.
        // Reporting "not ready yet" would therefore leave a caller waiting on
        // it for as long as they were willing to, every time, for nothing.
        if (($this->statements[$statement->getStreamId()] ?? null) !== $statement) {
            throw new StatementException(
                'This statement was not sent on this connection, so it can never be resolved here. Wait on the connection that sent it.',
                ExceptionCode::STATEMENT_NOT_ON_THIS_CONNECTION->value,
                [
                    'stream_id' => $statement->getStreamId(),
                    'request_class' => get_class($statement->getRequest()),
                ]
            );
        }
    }

    /**
     * Give up on every request whose budget has run out, and return them.
     *
     * This is bookkeeping, not a failure of whatever call happens to be
     * waiting: the statements are marked and their stream ids parked here, but
     * it is left to the caller to decide whether any of this is its business. A
     * wait that was asked about one of these statements raises it; a wait for an
     * event or for the next response simply carries on, and the caller learns
     * about it from the statement itself.
     *
     * All expired statements are handled in one pass, so a set of requests that
     * runs out together is finished together rather than one wait at a time.
     *
     * The ids are parked rather than released for the reason
     * {@see StreamIdPool::park()} gives: the node may still answer on them.
     *
     * @return array<Statement>
     */
    public function expire(?Statement $ignored): array {

        $now = microtime(true);
        $expired = [];

        foreach ($this->statements as $streamId => $statement) {
            if ($statement->isResultReady() || $statement === $ignored) {
                continue;
            }

            $deadline = $this->deadlines->at($statement->getRequestTimeout(), $statement->getSentAt());
            if ($deadline === null || $now < $deadline) {
                continue;
            }

            $statement->setStatus(StatementStatus::TIMED_OUT);
            unset($this->statements[$streamId]);
            $this->revision++;
            $this->streamIds->park($streamId, $statement->getStreamGeneration());

            $expired[] = $statement;
        }

        return $expired;
    }

    public function forget(int $streamId): void {

        if (!isset($this->statements[$streamId])) {
            return;
        }

        unset($this->statements[$streamId]);
        $this->revision++;
    }

    public function get(int $streamId): ?Statement {

        return $this->statements[$streamId] ?? null;
    }

    public function getCount(): int {

        return count($this->statements);
    }

    /**
     * Earliest deadline among the statements still waiting for an answer, so a
     * wait over several of them ends as soon as the first has used up its
     * budget. Null when none of them is bounded.
     *
     * Remembered between calls rather than recomputed on every pass of every
     * wait, see {@see self::$earliestDeadlineCache}. The answer is an absolute
     * deadline computed from each statement's own send time, so it does not go
     * stale merely because time has passed — only a change to which statements
     * are in flight, to when one of them was sent, or to the connection default
     * they fall back to can move it, and each of those bumps a revision.
     */
    public function getEarliestDeadline(?Statement $ignored): ?float {

        $cache = $this->earliestDeadlineCache;
        $deadlineRevision = $this->deadlines->getRevision();

        if (
            $cache !== null
            && $cache['revision'] === $this->revision
            && $cache['deadlineRevision'] === $deadlineRevision
            && $cache['ignored'] === $ignored
        ) {
            return $cache['deadline'];
        }

        $deadline = $this->deadlines->earliestForStatements($this->statements, $ignored);

        $this->earliestDeadlineCache = [
            'revision' => $this->revision,
            'deadlineRevision' => $deadlineRevision,
            'ignored' => $ignored,
            'deadline' => $deadline,
        ];

        return $deadline;
    }

    public function has(int $streamId): bool {

        return isset($this->statements[$streamId]);
    }

    /**
     * Whether anything besides the driver's own heartbeat is waiting for an
     * answer.
     *
     * Asked as a loop condition, so once per pass of a wait, which is why it is
     * this rather than a filtered copy of the pending map: the answer is only
     * ever whether to keep waiting, and the first statement that is not the
     * probe settles it.
     */
    public function hasPending(?Statement $ignored): bool {

        foreach ($this->statements as $statement) {
            if ($statement !== $ignored) {
                return true;
            }
        }

        return false;
    }

    /**
     * The statements of $expired that the caller was actually waiting on.
     *
     * Matched through an identity map rather than with in_array(), which would
     * make this a product of the two sets — and $waitedOn is whatever the caller
     * passed to waitForStatements(), which can be everything they have in
     * flight.
     *
     * @param array<Statement> $expired
     * @param array<Statement> $waitedOn
     *
     * @return array<Statement>
     */
    public function intersect(array $expired, array $waitedOn): array {

        if ($expired === [] || $waitedOn === []) {
            return [];
        }

        $waitedOnIds = [];
        foreach ($waitedOn as $statement) {
            $waitedOnIds[spl_object_id($statement)] = true;
        }

        return array_values(array_filter(
            $expired,
            static fn (Statement $statement): bool => isset($waitedOnIds[spl_object_id($statement)]),
        ));
    }

    public function register(int $streamId, Statement $statement): void {

        $this->statements[$streamId] = $statement;
        $this->revision++;
    }

    /**
     * Give up on a statement whose answer could not be handled.
     *
     * Handling an answer is not only reading it: it can prepare and re-send the
     * statement, cache a prepared result, or run the application's warnings
     * listeners, and any of that can fail — most plainly when the node keeps
     * answering UNPREPARED and {@see ResponseDispatcher::MAX_REPREPARATIONS} is
     * reached. Whatever failed is raised to whoever happened to be reading, but
     * the statement is the connection's to finish: left as it was it would be
     * neither pending nor answered, so its owner would wait on it for good and
     * be told, misleadingly, that it belongs to another connection.
     *
     * The id it was sent on is released rather than parked: the answer that led
     * here has already been read off the wire, so the node is done with it and
     * nothing can arrive on it by surprise. Two cases are left alone —
     * {@see RequestExecutor::chainAsyncRequest()} put a follow-up request on the
     * id, which is therefore in use again, or it gave up on the statement itself
     * and has already put the id wherever it belongs.
     *
     * Where the connection is gone by now it took its whole id space with it,
     * and {@see StreamIdPool::release()} passes the id over rather than handing
     * it to the connection that replaces this one.
     */
    public function releaseAfterFailedResponseHandling(Statement $statement, int $streamId): void {

        if (($this->statements[$streamId] ?? null) === $statement || $statement->isAbandoned()) {
            return;
        }

        $statement->setStatus(StatementStatus::ABANDONED);

        $this->streamIds->release($streamId, $statement->getStreamGeneration());
    }

    /**
     * Report statements the caller was waiting on that have been given up on.
     *
     * Only the connection's bookkeeping has happened by now ({@see self::expire()});
     * this turns it into the failure the caller sees, naming every statement of
     * theirs that ran out rather than just the first.
     *
     * @param array<Statement> $expired
     *
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function reportTimedOut(array $expired, string $operation): never {

        $streamIds = array_map(static fn (Statement $statement): int => $statement->getStreamId(), $expired);

        throw new RequestTimeoutException(
            count($expired) === 1
                ? 'Timed out waiting for the server to answer the request'
                : 'Timed out waiting for the server to answer ' . count($expired) . ' requests',
            ExceptionCode::REQUESTTIMEOUT_WAITING_FOR_STATEMENTS->value,
            [
                'operation' => $operation,
                'stream_ids' => $streamIds,
                'timed_out_statements' => count($expired),
                'orphaned_streams' => $this->streamIds->getOrphanedCount(),
            ],
            timedOutStatements: $expired,
        );
    }
}
