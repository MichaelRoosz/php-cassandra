<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\Session;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Response\Result;
use Cassandra\Value\ValueEncodeConfig;

/**
 * A client connection to a Cassandra or ScyllaDB cluster: the public API for
 * sending requests and waiting for answers. Its work is done by
 * {@see \Cassandra\Connection\Session}.
 */
final class Connection {
    /**
     * For the `$timeoutInSeconds` of a `waitFor…()` method: use that method's default.
     */
    public const WAIT_DEFAULT = null;

    /**
     * For the `$timeoutInSeconds` of a `waitFor…()` method: wait for as long as it takes.
     */
    public const WAIT_FOREVER = INF;

    /**
     * For the `$timeoutInSeconds` of a `waitFor…()` method: make one non-blocking
     * read attempt and return.
     */
    public const WAIT_NONE = 0.0;

    private Consistency $consistency = Consistency::ONE;

    private Session $session;

    /**
     * @param array<\Cassandra\Connection\NodeConfig> $nodes
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function __construct(
        array $nodes,
        string $keyspace = '',
        ConnectionOptions $options = new ConnectionOptions(),
    ) {

        if ($nodes === []) {
            throw new ConnectionException(
                'No nodes were configured; a connection needs at least one NodeConfig to connect to',
                ExceptionCode::CONNECTION_NO_NODES_CONFIGURED->value,
                [
                    'node_count' => 0,
                ]
            );
        }

        foreach ($nodes as $index => $node) {
            if (!$node instanceof Connection\NodeConfig) {
                throw new ConnectionException(
                    'Invalid node configuration; every node must be a NodeConfig',
                    ExceptionCode::CONNECTION_INVALID_NODE_CONFIG->value,
                    [
                        'index' => $index,
                        'actual_type' => get_debug_type($node),
                    ]
                );
            }
        }

        $this->session = new Session($this, $nodes, $keyspace, $options);
    }

    /**
     * Send a request the caller built without waiting for its answer; see
     * {@see self::syncRequest()}. Its budget runs from now.
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
    public function asyncRequest(Request\Request $request): Statement {
        return $this->session->sendAsyncRequest($request);
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
    public function batch(Request\Batch $batchRequest): Response\Result {
        $response = $this->syncRequest($batchRequest);

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
    public function batchAsync(Request\Batch $batchRequest): Statement {
        return $this->asyncRequest($batchRequest);
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

    public function createBatchRequest(BatchType $type = BatchType::LOGGED, ?Consistency $consistency = null, BatchOptions $options = new BatchOptions()): Request\Batch {

        $consistency = $consistency ?? $this->consistency;

        return new Request\Batch($type, $consistency, $options);
    }

    public function disconnect(): void {
        $this->session->disconnect();
    }

    /**
     * Non-blocking: read up to $max available frames and return how many were
     * processed, including late answers and the driver's own heartbeat;
     * {@see self::tryResolveStatements()} counts a caller's own statements instead.
     *
     * Like the other try…() calls this never waits for the node, but it still
     * enforces request budgets and sends a due heartbeat, whose write can block up
     * to the send stall window. Enforcing them can fail the connection with a
     * ConnectionException: an unanswered heartbeat or too many orphaned streams.
     *
     * Opens a connection if there is none, as tryReadNextEvent() and
     * tryReadNextResponse() do; a $max of zero or less reads nothing and opens none.
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
    public function execute(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions()): Response\Result {

        $consistency = $consistency ?? $this->consistency;

        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $response = $this->syncRequest($request);
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
    public function executeAll(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions()): array {

        $responses = [];

        $response = $this->execute($previousResult, $values, $consistency, $options)->asRowsResult();

        $responses[] = $response;

        $pagingState = $response->getRowsMetadata()->pagingState;
        $seenPagingStates = [];
        self::rememberPagingState($pagingState, $seenPagingStates, 'executeAll', 1);

        while ($pagingState !== null) {
            $response = $this->execute(
                // The page just read, because from v5 an ALTER can change the
                // result metadata id mid-paging, and naming the old one makes
                // the node resend the metadata with every remaining page.
                previousResult: $response,
                values: $values,
                consistency: $consistency,

                // Explicit, because Execute prefers the options' paging state to
                // the previous result's: a caller's own would repeat the first page.
                options: $options->withPagingState($pagingState),
            )->asRowsResult();

            $pagingState = $response->getRowsMetadata()->pagingState;
            self::rememberPagingState($pagingState, $seenPagingStates, 'executeAll', count($responses) + 1);

            $responses[] = $response;
        }

        return $responses;
    }

    /**
     * @param array<mixed> $values
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
    public function executeAsync(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions()): Statement {

        $consistency = $consistency ?? $this->consistency;

        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $statement = $this->asyncRequest($request);

        return $statement;
    }

    /**
     * The default for requests built by this connection; a request built by hand
     * keeps the consistency it was constructed with.
     */
    public function getConsistency(): Consistency {
        return $this->consistency;
    }

    /**
     * The keyspace this connection's requests run against, or an empty string when
     * there is none.
     */
    public function getKeyspace(): string {
        return $this->session->getKeyspace();
    }

    /**
     * The current transport, possibly wrapped in a framing or compression decorator,
     * or null when there is none.
     *
     * @deprecated The transport belongs to the session: writing to it or closing it
     * strands every request in flight. Use {@see self::isConnected()}.
     */
    public function getNode(): ?Connection\Node {
        return $this->session->getNode();
    }

    /**
     * The negotiated protocol version while connected, otherwise the initial one from
     * the connection options; a later connection negotiates afresh.
     */
    public function getProtocolVersion(): ProtocolVersion {
        return $this->session->getProtocolVersion();
    }

    /**
     * The connection's default request timeout in seconds; null and INF both wait
     * indefinitely. Requests with a timeout in their own options are unaffected.
     */
    public function getRequestTimeout(): ?float {
        return $this->session->getRequestTimeout();
    }

    /**
     * Wait for this statement's answer and return it.
     *
     * The wait is bounded by the statement's request timeout, or the connection's.
     * Where neither is set, the heartbeat is what detects a dead connection; with the
     * heartbeat off too, the transport's stall window fails the connection with a
     * NodeException. A statement whose connection was replaced during the wait raises
     * a ConnectionException, as its answer can no longer arrive.
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
    public function prepare(string $query, PrepareOptions $options = new PrepareOptions()): Response\Result\PreparedResult {

        $response = $this->syncRequest(new Request\Prepare($query, $options));
        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new ConnectionException('Unexpected response type during prepare', ExceptionCode::CONNECTION_PREPARE_UNEXPECTED_RESPONSE->value, [
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
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
    public function prepareAsync(string $query, PrepareOptions $options = new PrepareOptions()): Statement {

        $request = new Request\Prepare($query, $options);

        return $this->asyncRequest($request);
    }

    /**
     * @param array<mixed> $values
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
    public function query(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions()): Response\Result {

        $consistency = $consistency ?? $this->consistency;

        $request = new Request\Query($query, $values, $consistency, $options);

        $response = $this->syncRequest($request);
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
    public function queryAll(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions()): array {

        $responses = [];

        $response = $this->query($query, $values, $consistency, $options)->asRowsResult();

        $responses[] = $response;

        $pagingState = $response->getRowsMetadata()->pagingState;
        $seenPagingStates = [];
        self::rememberPagingState($pagingState, $seenPagingStates, 'queryAll', 1);

        while ($pagingState !== null) {
            $response = $this->query(
                query: $query,
                values: $values,
                consistency: $consistency,
                options: $options->withPagingState(
                    $pagingState
                ),
            )->asRowsResult();

            $pagingState = $response->getRowsMetadata()->pagingState;
            self::rememberPagingState($pagingState, $seenPagingStates, 'queryAll', count($responses) + 1);

            $responses[] = $response;
        }

        return $responses;
    }

    /**
     * @param array<mixed> $values
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
    public function queryAsync(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions()): Statement {

        $consistency = $consistency ?? $this->consistency;

        $request = new Request\Query($query, $values, $consistency, $options);

        return $this->asyncRequest($request);
    }

    /**
     * Be told about the events this connection receives.
     *
     * This does not subscribe to any: send a {@see \Cassandra\Request\Register} through
     * {@see self::syncRequest()}, and again after a reconnect, since the node's
     * registration ends with the socket.
     *
     * Listeners run inside the read that received the event, so one that blocks holds
     * up the call it was invoked from. A listener may issue requests of its own.
     */
    public function registerEventListener(EventListener $eventListener): void {
        $this->session->getListeners()->registerEventListener($eventListener);
    }

    /**
     * Be told about the warnings a response carries. Listeners run inside the read
     * that received the response, as in {@see self::registerEventListener()}.
     */
    public function registerWarningsListener(WarningsListener $warningsListener): void {
        $this->session->getListeners()->registerWarningsListener($warningsListener);
    }

    public function setConsistency(Consistency $consistency): void {
        $this->consistency = $consistency;
    }

    /**
     * Set the keyspace this connection's requests run against. The name is taken
     * exactly as given, so a keyspace created as `MyKs` is not reached by `myks`.
     *
     * Up to v4 this sends a USE: a keyspace that does not exist fails here and leaves
     * the connection where it was, and switching empties the prepared-statement cache.
     * There is no CQL to leave a keyspace, so an empty one is refused while connected
     * to a keyspace; {@see self::disconnect()} first.
     *
     * From v5 the keyspace travels with each request: this only records it, and a
     * keyspace that does not exist is refused by the next request. A keyspace in a
     * request's own options takes precedence; before v5 such a request is refused.
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
    public function setKeyspace(string $keyspace): void {
        $this->session->setKeyspace($keyspace);
    }

    /**
     * Set the default time the server may take to answer, in seconds, after which a
     * {@see \Cassandra\Exception\RequestTimeoutException} is raised. Null and INF wait
     * indefinitely; zero or less is refused.
     *
     * It applies to every request without a timeout in its own options, including
     * those in flight: lowering it can leave them overdue, and giving up on enough of
     * them at once reaches {@see ConnectionOptions::$maxOrphanedStreams} and replaces
     * the connection.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function setRequestTimeout(?float $requestTimeoutInSeconds): void {

        $this->session->setRequestTimeout($requestTimeoutInSeconds);
    }

    /**
     * Whether requests can carry their own keyspace. Before connecting this reports
     * the initial protocol version, not the one that will be negotiated.
     */
    public function supportsKeyspaceRequestOption(): bool {
        return $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value;
    }

    /**
     * Whether requests can carry their own `now_in_seconds`. Before connecting this
     * reports the initial protocol version, not the one that will be negotiated.
     */
    public function supportsNowInSecondsRequestOption(): bool {
        return $this->session->getProtocolVersion()->value >= ProtocolVersion::V5->value;
    }

    /**
     * Send a request the caller built and wait for its answer.
     *
     * The connection fills in the stream id, the protocol version and, where the
     * request names none, its keyspace. The timeout comes from the request's options,
     * then {@see self::setRequestTimeout()}. It bounds each request sent, so a
     * statement that has to be prepared first can take a multiple of it.
     *
     * A connection replaced during the wait raises a ConnectionException; whether the
     * node acted on the request is then unknown.
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
    public function syncRequest(Request\Request $request): Response\Response {

        return $this->session->sendSyncRequest($request);
    }

    /**
     * Non-blocking: return the next event, or null if none is available. See
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
     * Non-blocking: return the next response, or null if none is available. See
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
     * Non-blocking: try to resolve a statement; returns true if it is ready.
     *
     * Never opens a connection. A statement that cannot resolve raises instead: a
     * StatementException if it was never this connection's or was abandoned, a
     * RequestTimeoutException if it was given up on. See also
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
     * Non-blocking: process up to $max responses and return how many of $statements
     * newly resolved. Raises as {@see self::tryResolveStatement()} does.
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
        $this->session->getListeners()->unregisterEventListener($eventListener);
    }

    public function unregisterWarningsListener(WarningsListener $warningsListener): void {
        $this->session->getListeners()->unregisterWarningsListener($warningsListener);
    }

    /**
     * Wait until every statement in flight has been answered or the time is up.
     * A statement that runs out of its budget raises a RequestTimeoutException.
     *
     * @param ?float $timeoutInSeconds how long this call may block, in seconds:
     *   null  let the statements' own budgets bound it (the default)
     *   {@see self::WAIT_NONE}     one non-blocking read attempt
     *   {@see self::WAIT_FOREVER}  for as long as it takes
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
     * Wait until any of the given statements is ready and return it. Returns null when
     * the time is up, or at once when $statements is empty.
     *
     * Remove a returned statement from the list before calling again, as a ready
     * statement is returned every time. A statement that runs out of its budget raises
     * a RequestTimeoutException, even if another became ready in the same pass; that
     * one stays ready and is returned by the next call.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds how long this call may block, in seconds:
     *   null  let the statements' own budgets bound it (the default)
     *   {@see self::WAIT_NONE}     one non-blocking read attempt
     *   {@see self::WAIT_FOREVER}  for as long as it takes
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
     * Wait for the next server event, opening the connection if needed. Returns null
     * when the time is up. A quiet cluster is not an error, and the heartbeat is what
     * detects a dead connection: with the heartbeat off, pass a finite timeout, since
     * the transport's stall window otherwise fails the connection.
     *
     * Requests in flight keep their budgets, but one that expires here only raises when
     * its statement is next used, so an event loop is not interrupted for it. Reaching
     * {@see ConnectionOptions::$maxOrphanedStreams} does raise a ConnectionException,
     * because the connection is gone.
     *
     * @param ?float $timeoutInSeconds how long this call may block, in seconds:
     *   null  for as long as it takes (the default)
     *   {@see self::WAIT_NONE}     one non-blocking read attempt
     *   {@see self::WAIT_FOREVER}  for as long as it takes
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
     * Wait for the next response of any kind, opening the connection if needed. Returns
     * null when the time is up. Requests in flight behave as in
     * {@see self::waitForNextEvent()}.
     *
     * @param ?float $timeoutInSeconds how long this call may block, in seconds:
     *   null  the connection's request timeout (the default), or for as long as it
     *         takes where that is null too
     *   {@see self::WAIT_NONE}     one non-blocking read attempt
     *   {@see self::WAIT_FOREVER}  for as long as it takes
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
     * Wait until the given statements have been answered or the time is up, so check
     * {@see Statement::isResultReady()} when passing a timeout. A statement that runs
     * out of its budget raises a RequestTimeoutException.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds how long this call may block, in seconds:
     *   null  let the statements' own budgets bound it (the default)
     *   {@see self::WAIT_NONE}     one non-blocking read attempt
     *   {@see self::WAIT_FOREVER}  for as long as it takes
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

    /**
     * {@see self::setConsistency()} for chaining. This changes and returns this
     * connection rather than a copy, unlike the `with…()` methods of request options.
     */
    public function withConsistency(Consistency $consistency): self {
        $this->setConsistency($consistency);

        return $this;
    }

    /**
     * {@see self::setKeyspace()} for chaining. This changes and returns this connection
     * rather than a copy.
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
    public function withKeyspace(string $keyspace): self {
        $this->setKeyspace($keyspace);

        return $this;
    }

    /**
     * @param array<string, true> $seenPagingStates
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    private static function rememberPagingState(
        ?string $pagingState,
        array &$seenPagingStates,
        string $operation,
        int $pagesReceived,
    ): void {
        if ($pagingState === null) {
            return;
        }

        if (isset($seenPagingStates[$pagingState])) {
            throw new ResponseException(
                'Server returned a paging state that was already used while fetching this result set',
                ExceptionCode::RESPONSE_ROWS_PAGING_STATE_CYCLE->value,
                [
                    'operation' => $operation,
                    'pages_received' => $pagesReceived,
                    'paging_state_length' => strlen($pagingState),
                ]
            );
        }

        $seenPagingStates[$pagingState] = true;
    }

}
