<?php

declare(strict_types=1);

namespace Cassandra\Exception;

use Cassandra\Statement;
use Throwable;

/**
 * The server did not answer within the client-side request timeout.
 *
 * This is deliberately not a {@see NodeException}: nothing is known to be wrong
 * with the node or the connection, the coordinator was simply slower than the
 * client was willing to wait. Only the requests that ran out are finished — the
 * connection stays open, keeps its prepared statements, and its other requests
 * carry on — and the node is not counted as failed, so one expensive query
 * cannot push a healthy node out of rotation.
 *
 * Raise the request timeout (`ConnectionOptions::$requestTimeoutInSeconds`,
 * `Connection::setRequestTimeout()`, the `requestTimeoutInSeconds` option of the
 * request itself, or the per-call argument of `Connection::syncRequest()`) for
 * operations that are legitimately slow, such as TRUNCATE, which Cassandra
 * allows 60s for by default.
 */
final class RequestTimeoutException extends CassandraException {
    /**
     * @var array<Statement> $timedOutStatements
     */
    private array $timedOutStatements;

    /**
     * @param array<mixed> $context
     * @param array<Statement> $timedOutStatements the statements that ran out of
     * time, so that a caller waiting on several of them can tell which ones to
     * send again without having to match stream ids up by hand. Empty for a
     * synchronous request, which has no statement of its own — there the failing
     * request is simply the one that was called.
     */
    public function __construct(
        string $message,
        int $code,
        array $context = [],
        ?Throwable $previous = null,
        array $timedOutStatements = [],
    ) {
        parent::__construct($message, $code, $context, $previous);

        $this->timedOutStatements = $timedOutStatements;
    }

    /**
     * The statements that ran out of time, in the order they were given up on.
     *
     * @return array<Statement>
     */
    public function getTimedOutStatements(): array {
        return $this->timedOutStatements;
    }
}
