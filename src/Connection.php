<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\Session;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Response\Result;
use Cassandra\Value\ValueEncodeConfig;

/**
 * A client connection to a Cassandra or ScyllaDB cluster.
 *
 * This is the whole public API. It builds requests out of what a caller passes
 * — filling in the connection's keyspace and consistency where a call leaves
 * them out — and hands everything that touches the node to its
 * {@see \Cassandra\Connection\Session}, which opens the connection, sends the
 * request, matches the answer to it and keeps its budget.
 */
final class Connection {
    private Consistency $consistency = Consistency::ONE;

    /**
     * The connection to the node and the machinery around it. Owned rather than
     * inherited from or handed out, so that what a caller can reach stays
     * exactly what is documented here.
     */
    private Session $session;

    /**
     * @param array<\Cassandra\Connection\NodeConfig> $nodes
     */
    public function __construct(
        array $nodes,
        string $keyspace = '',
        ConnectionOptions $options = new ConnectionOptions(),
    ) {

        $this->session = new Session($this, $nodes, $keyspace, $options);
    }

    /**
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this statement only. The counterpart of the argument
     * {@see self::syncRequest()} takes; the budget runs from now, when the
     * request is written, not from whenever the caller starts waiting for it.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function asyncRequest(Request\Request $request, ?float $requestTimeoutInSeconds = null): Statement {
        return $this->session->sendAsyncRequest($request, requestTimeoutInSeconds: $requestTimeoutInSeconds);
    }

    /**
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this call only; see {@see self::syncRequest()}, whose argument this is the
     * counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function batch(Request\Batch $batchRequest, ?float $requestTimeoutInSeconds = null): Response\Result {
        $response = $this->syncRequest($batchRequest, $requestTimeoutInSeconds);

        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during batch', ExceptionCode::CONNECTION_UNEXPECTED_RESPONSE_BATCH_SYNC->value, [
                'operation' => 'batch',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this statement only; see {@see self::asyncRequest()}, whose argument this
     * is the counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function batchAsync(Request\Batch $batchRequest, ?float $requestTimeoutInSeconds = null): Statement {
        return $this->asyncRequest($batchRequest, $requestTimeoutInSeconds);
    }

    public function configureValueEncoding(ValueEncodeConfig $config): void {
        $this->session->configureValueEncoding($config);
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function connect(): void {
        $this->session->connect();
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    public function createBatchRequest(BatchType $type = BatchType::LOGGED, ?Consistency $consistency = null, BatchOptions $options = new BatchOptions()): Request\Batch {

        $consistency = $consistency ?? $this->consistency;

        if (
            $options->keyspace === null
            && $this->session->getKeyspace() !== ''
            && $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->session->getKeyspace());
        }

        return new Request\Batch($type, $consistency, $options);
    }

    public function disconnect(): void {
        $this->session->disconnect();
    }

    /**
     * Non-blocking: read up to $max available responses, returning how many were processed.
     *
     * NOTE: This method will not block; it processes any currently available responses
     * and returns when the receive buffer is drained or the provided limit is reached.
     *
     * "Does not block" is about waiting for the node to answer. Two things
     * around that still can, and they are the same for
     * {@see self::tryReadNextEvent()}, {@see self::tryReadNextResponse()},
     * {@see self::tryResolveStatement()} and
     * {@see self::tryResolveStatements()}:
     *
     * Getting a connection at all. Called before anything has been sent, this
     * opens one and takes it through the handshake first, as every other method
     * that touches the transport does. The one exception is a $max of zero
     * here, which reads nothing and so never gets as far as needing a
     * connection.
     *
     * Writing the heartbeat. Having read, these calls owe the connection the
     * probe a wait would have sent, and a write blocks until the transport
     * accepts it — bounded by the send stall window, not by anything here.
     * Waiting for its answer is what these calls do not do; that is left to
     * whichever call reads next.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function drainAvailableResponses(int $max = PHP_INT_MAX): int {
        return $this->session->drainAvailableResponses($max);
    }

    /**
     * @param array<mixed> $values
     *
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this call only; see {@see self::syncRequest()}, whose argument this is the
     * counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function execute(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions(), ?float $requestTimeoutInSeconds = null): Response\Result {

        $consistency = $consistency ?? $this->consistency;

        if (
            $options->keyspace === null
            && $this->session->getKeyspace() !== ''
            && $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->session->getKeyspace());
        }

        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $response = $this->syncRequest($request, $requestTimeoutInSeconds);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during execute', ExceptionCode::CONNECTION_EXECUTE_UNEXPECTED_RESPONSE->value, [
                'operation' => 'execute',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @param array<mixed> $values
     * @return array<\Cassandra\Response\Result\RowsResult>
     *
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this call only. It bounds each page request, not the call as a whole, so
     * fetching a paged result can take a multiple of it before giving up.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function executeAll(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions(), ?float $requestTimeoutInSeconds = null): array {

        $responses = [];

        $response = $this->execute($previousResult, $values, $consistency, $options, $requestTimeoutInSeconds)->asRowsResult();

        $responses[] = $response;

        $pagingState = $response->getRowsMetadata()->pagingState;
        while ($pagingState !== null) {
            $response = $this->execute(
                previousResult: $previousResult,
                values: $values,
                consistency: $consistency,
                options: $options->withPagingState($pagingState),
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
            )->asRowsResult();

            $responses[] = $response;

            $pagingState = $response->getRowsMetadata()->pagingState;
        }

        return $responses;
    }

    /**
     * @param array<mixed> $values
     *
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this statement only; see {@see self::asyncRequest()}, whose argument this
     * is the counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function executeAsync(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions(), ?float $requestTimeoutInSeconds = null): Statement {

        $consistency = $consistency ?? $this->consistency;

        if (
            $options->keyspace === null
            && $this->session->getKeyspace() !== ''
            && $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->session->getKeyspace());
        }

        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $statement = $this->asyncRequest($request, $requestTimeoutInSeconds);

        return $statement;
    }

    public function getNode(): ?Connection\Node {
        return $this->session->getNode();
    }

    /**
     * Returns the protocol version used by this connection.
     * Before connecting, it will return the initial protocol version,
     * as set in the connection options.
     */
    public function getProtocolVersion(): ProtocolVersion {
        return $this->session->getProtocolVersion();
    }

    /**
     * Wait for this statement's answer and return it.
     *
     * The wait is bounded by the statement's own request timeout, or by the
     * connection's where it has none. Where neither is set the wait is
     * unbounded, and then — exactly as in {@see self::waitForNextEvent()} — the
     * heartbeat is the only thing that can tell a dead connection from a slow
     * one. With heartbeats disabled as well
     * ({@see ConnectionOptions::$heartbeatIntervalInSeconds} set to null), the
     * transport's stall window is the last judgement left and this fails the
     * connection with a NodeException once it elapses.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     */
    public function getResponseForStatement(Statement $statement): Response\Response {
        return $this->session->getResponseForStatement($statement);
    }

    /**
     * @deprecated Use getProtocolVersion() instead.
     */
    public function getVersion(): int {
        return $this->session->getProtocolVersion()->value;
    }

    public function isConnected(): bool {
        return $this->session->isConnected();
    }

    /**
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this call only; see {@see self::syncRequest()}, whose argument this is the
     * counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function prepare(string $query, PrepareOptions $options = new PrepareOptions(), ?float $requestTimeoutInSeconds = null): Response\Result\PreparedResult {

        if (
            $options->keyspace === null
            && $this->session->getKeyspace() !== ''
            && $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->session->getKeyspace());
        }

        $response = $this->syncRequest(new Request\Prepare($query, $options), $requestTimeoutInSeconds);
        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new ConnectionException('Unexpected response type during prepare', ExceptionCode::CONNECTION_PREPARE_UNEXPECTED_RESPONSE->value, [
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this statement only; see {@see self::asyncRequest()}, whose argument this
     * is the counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function prepareAsync(string $query, PrepareOptions $options = new PrepareOptions(), ?float $requestTimeoutInSeconds = null): Statement {

        if (
            $options->keyspace === null
            && $this->session->getKeyspace() !== ''
            && $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->session->getKeyspace());
        }

        $request = new Request\Prepare($query, $options);

        return $this->asyncRequest($request, $requestTimeoutInSeconds);
    }

    /**
     * @param array<mixed> $values
     *
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this call only; see {@see self::syncRequest()}, whose argument this is the
     * counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function query(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions(), ?float $requestTimeoutInSeconds = null): Response\Result {

        $consistency = $consistency ?? $this->consistency;

        if (
            $options->keyspace === null
            && $this->session->getKeyspace() !== ''
            && $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->session->getKeyspace());
        }

        $request = new Request\Query($query, $values, $consistency, $options);

        $response = $this->syncRequest($request, $requestTimeoutInSeconds);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during query', ExceptionCode::CONNECTION_QUERY_UNEXPECTED_RESPONSE->value, [
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @param array<mixed> $values
     * @return array<\Cassandra\Response\Result\RowsResult>
     *
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this call only. It bounds each page request, not the call as a whole, so
     * fetching a paged result can take a multiple of it before giving up.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function queryAll(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions(), ?float $requestTimeoutInSeconds = null): array {

        $responses = [];

        $response = $this->query($query, $values, $consistency, $options, $requestTimeoutInSeconds)->asRowsResult();

        $responses[] = $response;

        $pagingState = $response->getRowsMetadata()->pagingState;
        while ($pagingState !== null) {
            $response = $this->query(
                query: $query,
                values: $values,
                consistency: $consistency,
                options: $options->withPagingState(
                    $pagingState
                ),
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
            )->asRowsResult();

            $responses[] = $response;

            $pagingState = $response->getRowsMetadata()->pagingState;
        }

        return $responses;
    }

    /**
     * @param array<mixed> $values
     *
     * @param ?float $requestTimeoutInSeconds how long the server may take to
     * answer, overriding the request's and the connection's request timeout for
     * this statement only; see {@see self::asyncRequest()}, whose argument this
     * is the counterpart of.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
    */
    public function queryAsync(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions(), ?float $requestTimeoutInSeconds = null): Statement {

        $consistency = $consistency ?? $this->consistency;

        if (
            $options->keyspace === null
            && $this->session->getKeyspace() !== ''
            && $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->session->getKeyspace());
        }

        $request = new Request\Query($query, $values, $consistency, $options);

        return $this->asyncRequest($request, $requestTimeoutInSeconds);
    }

    public function registerEventListener(EventListener $eventListener): void {
        $this->session->listeners()->registerEventListener($eventListener);
    }

    public function registerWarningsListener(WarningsListener $warningsListener): void {
        $this->session->listeners()->registerWarningsListener($warningsListener);
    }

    public function setConsistency(Consistency $consistency): void {
        $this->consistency = $consistency;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function setKeyspace(string $keyspace): void {
        $this->session->setKeyspace($keyspace);
    }

    /**
     * How long to wait for the server's answer to a request before giving up
     * with a {@see \Cassandra\Exception\RequestTimeoutException}, in seconds.
     * Null waits indefinitely.
     *
     * Applies to every request that has no explicit timeout of its own. That
     * includes the ones already in flight, whose budgets are measured against
     * this value as it stands when they are looked at rather than as it stood
     * when they were sent: lowering it can therefore expire outstanding
     * requests at once — and since giving up on a request parks its stream id,
     * doing so to enough of them at once can reach
     * {@see ConnectionOptions::$maxOrphanedStreams} and replace the connection.
     * Requests sent with a timeout of their own — from their options, or from
     * the argument {@see self::syncRequest()} and {@see self::asyncRequest()}
     * take — are unaffected.
     *
     * Raise it around operations Cassandra allows more time for, such as
     * TRUNCATE (60s server-side by default), or pass the timeout directly to
     * {@see self::syncRequest()} for a single request.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function setRequestTimeout(?float $requestTimeoutInSeconds): void {

        $this->session->setRequestTimeout($requestTimeoutInSeconds);
    }

    public function supportsKeyspaceRequestOption(): bool {
        return $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value;
    }

    public function supportsNowInSecondsRequestOption(): bool {
        return $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value;
    }

    /**
     * @param ?float $requestTimeoutInSeconds how long to wait for the server's
     * answer, overriding the request's and the connection's request timeout for this call only.
     * Pass a larger value for operations Cassandra itself allows more time for,
     * such as TRUNCATE.
     *
     * It bounds each request this call sends, not the call as a whole: when the
     * driver has to prepare or reprepare the statement first, the PREPARE and
     * the request it precedes each get the full budget, so the call can take a
     * multiple of it before giving up — a multiple bounded in turn by
     * {@see \Cassandra\Connection\ResponseDispatcher::MAX_REPREPARATIONS}, since a node can answer the re-executed
     * statement with UNPREPARED all over again. The wait for a free stream id,
     * on a connection that has handed out every one of them, is held to it
     * separately as well.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function syncRequest(Request\Request $request, ?float $requestTimeoutInSeconds = null): Response\Response {

        return $this->session->sendSyncRequest($request, $requestTimeoutInSeconds);
    }

    /**
     * Non-blocking: attempt to read and return the next event, or null if none is available.
     *
     * Opens the connection first if there is none yet; see
     * {@see self::drainAvailableResponses()}.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function tryReadNextEvent(): ?Response\Event {
        return $this->session->tryReadNextEvent();
    }

    /**
     * Non-blocking: attempt to read and return the next response, or null if none is available.
     *
     * Opens the connection first if there is none yet; see
     * {@see self::drainAvailableResponses()}.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function tryReadNextResponse(): ?Response\Response {
        return $this->session->tryReadNextResponse();
    }

    /**
     * Non-blocking: try to resolve a specific statement; returns true if it is ready.
     *
     * Opens the connection first if there is none yet; see
     * {@see self::drainAvailableResponses()}.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     */
    public function tryResolveStatement(Statement $statement): bool {
        return $this->session->tryResolveStatement($statement);
    }

    /**
     * Non-blocking: try to resolve from a set of statements, up to $max responses processed.
     * Returns the number of newly resolved statements from the provided set.
     *
     * Opens the connection first if there is none yet; see
     * {@see self::drainAvailableResponses()}.
     *
     * @param array<Statement> $statements
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     */
    public function tryResolveStatements(array $statements, int $max = PHP_INT_MAX): int {
        return $this->session->tryResolveStatements($statements, $max);
    }

    public function unregisterEventListener(EventListener $eventListener): void {
        $this->session->listeners()->unregisterEventListener($eventListener);
    }

    public function unregisterWarningsListener(WarningsListener $warningsListener): void {
        $this->session->listeners()->unregisterWarningsListener($warningsListener);
    }

    /**
     * Wait until every statement in flight has been answered.
     *
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  let the statements' own budgets bound it (the default)
     *   0     do not wait: make one non-blocking read attempt and return
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * It returns once every statement has been answered or the time is up. A
     * statement that runs out of its own budget is given up on and raises a
     * RequestTimeoutException instead.
     *
     * A timeout of 0 still costs a read while anything is outstanding, but a
     * non-blocking one, so it does not wait on the transport either;
     * {@see self::tryResolveStatements()} is the equivalent that never touches
     * a deadline at all.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function waitForAllPendingStatements(?float $timeoutInSeconds = null): void {
        $this->session->waitForAllPendingStatements($timeoutInSeconds);
    }

    /**
     * Wait until any of the given statements becomes ready and return it.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  let the statements' own budgets bound it (the default)
     *   0     do not wait: make one non-blocking read attempt and return
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * Returns null when the time is up with none of them ready; the statements
     * are untouched and can still be waited on. A statement that runs out of
     * its own budget is given up on and raises a RequestTimeoutException, which
     * takes precedence over handing one back: where one of them is answered in
     * the same pass that another runs out, the exception is what the caller
     * sees. Nothing is lost by it — the answer stays on its statement and the
     * exception names the ones that ran out, so calling again returns the ready
     * one, as long as those are dropped from $statements first. They are not
     * resolvable any more, and a statement that can never resolve is reported
     * rather than waited on.
     *
     * A timeout of 0 still costs a read while anything is outstanding, but a
     * non-blocking one, so it does not wait on the transport either;
     * {@see self::tryResolveStatements()} is the equivalent that never touches
     * a deadline at all.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     */
    public function waitForAnyStatement(array $statements, ?float $timeoutInSeconds = null): ?Statement {
        return $this->session->waitForAnyStatement($statements, $timeoutInSeconds);
    }

    /**
     * Wait for the next server event.
     *
     * An idle event stream is not an error, so this keeps waiting across
     * transport read timeouts instead of tearing the connection down: the node
     * simply had nothing to report. While waiting, an OPTIONS heartbeat is sent
     * whenever the connection has been silent for longer than the configured
     * heartbeat interval, so a connection that died quietly is still noticed —
     * which is the job a read timeout cannot do here.
     *
     * That rests on the heartbeat, and only on it. Turn it off
     * ({@see ConnectionOptions::$heartbeatIntervalInSeconds} set to null) and
     * an unbounded wait here has nothing bounding its reads at all, at which
     * point the transport's stall window is the only judgement left and this
     * fails the connection with a NodeException once it elapses, as every other
     * unbounded wait does. Waiting for events with heartbeats disabled
     * therefore means passing a finite $timeoutInSeconds and calling again,
     * rather than waiting indefinitely.
     *
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  wait for as long as it takes (the default), since an event can
     *         arrive at any time
     *   0     do not wait: make one non-blocking read attempt and return
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * Returns null when the time is up without an event, leaving the connection
     * usable. A timeout of 0 still costs one read, but a non-blocking one, so
     * it does not wait on the transport either; {@see self::tryReadNextEvent()}
     * is the equivalent that never touches a deadline at all.
     *
     * Requests already in flight keep their own deadlines while this waits, and
     * one that runs out is given up on here. It is not raised here, though: an
     * event listener did not ask about it, so its loop is not interrupted for
     * it — the caller finds out from the statement, which then raises
     * RequestTimeoutException. Only that request is affected and the connection
     * stays open.
     *
     * The one exception is the connection itself giving out: enough requests
     * running out without their answers ever arriving reaches
     * {@see ConnectionOptions::$maxOrphanedStreams}, and that replaces the
     * connection and raises a ConnectionException here. An event loop is
     * interrupted for that, because the connection it was reading from is gone.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function waitForNextEvent(?float $timeoutInSeconds = null): ?Response\Event {
        return $this->session->waitForNextEvent($timeoutInSeconds);
    }

    /**
     * Wait for the next response of any kind, the counterpart of
     * {@see self::waitForNextEvent()}.
     *
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  use the connection's request timeout (the default)
     *   0     do not wait: make one non-blocking read attempt and return
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * Null means the connection's request timeout here, rather than "no bound"
     * as it does in the waits that take statements: those are bounded by the
     * budgets of the statements they were given, and this call has none to go
     * by. Where the connection's request timeout is itself null there is
     * nothing to fall back on and the wait is unbounded, exactly as INF is —
     * the two are the same wait, INF being the way to ask for it without
     * changing what the connection default means for everything else. Neither
     * ends of its own accord: what ends an unbounded wait is a response
     * arriving, or the connection being found dead, as
     * {@see self::waitForNextEvent()} describes.
     *
     * Returns null when the time is up with nothing having arrived, whether or
     * not anything went overdue in the meantime — see below. A timeout of 0
     * still costs one read, but a non-blocking one, so it does not wait on the
     * transport either; {@see self::tryReadNextResponse()} is the equivalent
     * that never touches a deadline at all.
     *
     * Requests already in flight keep their own deadlines while this waits, and
     * one that runs out is given up on here. It is not raised here, though: the
     * caller asked for the next response, not about any request in particular —
     * they find out from the statement, which then raises
     * RequestTimeoutException. Only that request is affected and the connection
     * stays open.
     *
     * The one exception is the connection itself giving out: enough requests
     * running out without their answers ever arriving reaches
     * {@see ConnectionOptions::$maxOrphanedStreams}, and that replaces the
     * connection and raises a ConnectionException here. The wait cannot simply
     * be repeated then, because the connection it was reading from is gone.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function waitForNextResponse(?float $timeoutInSeconds = null): ?Response\Response {
        return $this->session->waitForNextResponse($timeoutInSeconds);
    }

    /**
     * Wait until the given async statements have been answered.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  let the statements' own budgets bound it (the default)
     *   0     do not wait: make one non-blocking read attempt and return
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * It returns once they have all been answered or the time is up, so check
     * isResultReady() when passing a timeout. A statement that runs out of its
     * own budget is given up on and raises a RequestTimeoutException instead.
     *
     * A timeout of 0 still costs a read while anything is outstanding, but a
     * non-blocking one, so it does not wait on the transport either;
     * {@see self::tryResolveStatements()} is the equivalent that never touches
     * a deadline at all.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     */
    public function waitForStatements(array $statements, ?float $timeoutInSeconds = null): void {
        $this->session->waitForStatements($statements, $timeoutInSeconds);
    }

    public function withConsistency(Consistency $consistency): self {
        $this->setConsistency($consistency);

        return $this;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function withKeyspace(string $keyspace): self {
        $this->setKeyspace($keyspace);

        return $this;
    }
}
