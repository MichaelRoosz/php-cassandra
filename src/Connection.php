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
 * A client connection to a Cassandra or ScyllaDB cluster.
 *
 * This is the whole of what a caller does to a connection; the rest of the
 * public API is the things they hand it and get back — {@see ConnectionOptions}
 * and {@see \Cassandra\Connection\NodeConfig} on the way in,
 * {@see \Cassandra\Statement} and the {@see \Cassandra\Response\Response} tree
 * on the way out. It builds requests out of what a caller passes — filling in
 * this connection's consistency where a call leaves it out — and hands
 * everything that touches the node to its
 * {@see \Cassandra\Connection\Session}, which opens the connection, sends the
 * request, matches the answer to it and keeps its budget.
 *
 * The keyspace is not filled in here but on the way to the wire, by
 * {@see \Cassandra\Connection\RequestExecutor}, because whether a request can
 * carry one at all depends on the negotiated protocol version and that is only
 * settled once the connection is up. Which is why it reaches a request built by
 * hand and handed to {@see self::syncRequest()} just as it reaches the ones
 * built here; see {@see self::setKeyspace()}.
 */
final class Connection {
    private Consistency $consistency = Consistency::ONE;

    /**
     * The connection to the node and the machinery around it. Owned rather than
     * inherited from, so that what a caller can reach stays exactly what is
     * documented here. {@see self::getNode()} is the one thing that reaches past
     * it, and is deprecated for that reason.
     */
    private Session $session;

    /**
     * @param array<\Cassandra\Connection\NodeConfig> $nodes
     *
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
     * Non-blocking: read up to $max available responses, returning how many were processed.
     *
     * NOTE: This method will not block; it processes any currently available responses
     * and returns when the receive buffer is drained or the provided limit is reached.
     *
     * The count is of frames taken off the wire and dealt with, not of requests
     * of the caller's that were answered — it also covers a late answer to a
     * request already given up on, and the driver's own heartbeat.
     * {@see self::tryResolveStatements()} is what counts a caller's own
     * statements.
     *
     * @param int $max most frames to take off the wire, which zero or less
     * makes none: such a call reads nothing and so opens no connection, but it
     * still keeps the request budgets described below.
     *
     * "Does not block" is about waiting for the node to answer. Two things
     * around that still can, and they are the same for
     * {@see self::tryReadNextEvent()}, {@see self::tryReadNextResponse()},
     * {@see self::tryResolveStatement()} and
     * {@see self::tryResolveStatements()}:
     *
     * Getting a connection at all. Only the calls that read off the wire on
     * their own account open one: called before anything has been sent, this
     * method and {@see self::tryReadNextEvent()}/{@see self::tryReadNextResponse()}
     * open a connection and take it through the handshake first, as every other
     * method that touches the transport does — and even they do not where they
     * never get as far as reading, which a $max of zero here does not. The two
     * calls that take statements never open one at all: a statement is only
     * resolvable here if this connection is the one that sent it, so where
     * there is no connection there is nothing they could be asked about, and
     * they raise instead of opening one to read on — a StatementException for
     * one that was never this connection's or was abandoned with it, a
     * RequestTimeoutException for one already given up on.
     *
     * Writing the heartbeat. Having read, these calls owe the connection the
     * probe a wait would have sent, and a write blocks until the transport
     * accepts it — bounded by the send stall window, not by anything here.
     * Waiting for its answer is what these calls do not do; that is left to
     * whichever call reads next. That call may well be this one on a later pass,
     * and finding a probe of an earlier pass still unanswered past the heartbeat
     * timeout is one of the two ways a call that never waits can nonetheless
     * fail the connection with a ConnectionException — the other being the
     * orphaned-stream limit, which the request budgets kept here can reach; see
     * {@see self::waitForNextResponse()}. The probe's write failing takes the
     * connection down as well, but is reported as the transport failure it is
     * rather than as either of those.
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
     * The wait is bounded by the statement's own request timeout, or by the
     * connection's where it has none. Where neither is set the wait is
     * unbounded, and then — exactly as in {@see self::waitForNextEvent()} — the
     * heartbeat is the only thing that can tell a dead connection from a slow
     * one. With heartbeats disabled as well
     * ({@see ConnectionOptions::$heartbeatIntervalInSeconds} set to null), the
     * transport's stall window is the last judgement left and this fails the
     * connection with a NodeException once it elapses — unless some other
     * request in flight is bounded, since each read is held to the earliest
     * budget on the connection rather than only to this statement's, and a read
     * that something still bounds treats the stall window as the quiet moment it
     * is instead of as a failure.
     *
     * An answer that arrives is the answer, whichever read took it off the wire.
     * A listener invoked from inside this wait can issue a request of its own,
     * and the read that serves it may well be the one that brings this
     * statement's answer; it resolves the statement all the same, and is handed
     * back here rather than being overtaken by the budget that ran out while
     * the nested request was still being waited for.
     *
     * What such a listener cannot do is have this wait carry on across a
     * connection it replaced: a statement is only resolvable on the connection
     * that sent it, so one whose connection went away while its answer was
     * outstanding is reported with a ConnectionException — as
     * {@see self::syncRequest()} describes — instead of being waited on for an
     * answer that can no longer reach it.
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
     * The keyspace the requests this connection builds run against.
     *
     * How it takes effect depends on the negotiated protocol version, and so
     * does when a keyspace that does not exist is refused. Up to v4 it is the
     * node's session that is switched over, with a USE sent from here, so a bad
     * keyspace fails this call — and leaves the connection on the keyspace it
     * was already on, which is where the node's session stayed. From v5 the
     * keyspace travels with each request — USE is deprecated there — so this
     * call only records it, and a bad one is refused by the next request
     * instead.
     *
     * That difference is also why an empty keyspace — moving this connection off
     * a keyspace rather than onto another one — is only accepted from v5, where
     * it means the absence of a keyspace on each request. Up to v4 there is no
     * CQL that un-sets a node session's keyspace, so on a connected v4
     * connection that is on one this raises rather than recording a keyspace the
     * requests would not actually run against; before connecting, and from v5,
     * it is recorded like any other value. A connection that is on no keyspace
     * already is not being moved off anything, so an empty keyspace is recorded
     * there too, on every version — there is nothing for it to contradict.
     *
     * {@see self::disconnect()} is therefore what makes a v4 connection
     * clearable: with no connection there is no node session to contradict, so
     * an empty keyspace is recorded like any other value and the next connect
     * opens on none. Disconnecting does not clear it by itself — what this
     * connection is on outlives the socket, which is why reconnecting sends the
     * USE again — so moving a v4 connection off its keyspace is disconnect,
     * then setKeyspace(''), and not either of them alone.
     *
     * Switching keyspace also empties the prepared-statement cache up to v4,
     * where a prepared statement cannot be told apart by the keyspace it was
     * prepared in. From v5 it is kept: the keyspace is part of what each entry
     * is filed under, so both keyspaces keep their own.
     *
     * Either way it applies to every request sent on this connection, including
     * one built by hand and handed to {@see self::syncRequest()} or
     * {@see self::asyncRequest()} — except where that request names a keyspace
     * of its own, which wins from v5. A request sent more than once takes
     * whatever this connection is on at each send; it is only a keyspace the
     * caller put on the request that survives a change here.
     *
     * Up to v4 a keyspace on the request has nothing to win with: the request
     * option does not exist before v5, so a request carrying one is refused when
     * it is encoded, with a RequestException, rather than being sent against the
     * keyspace it names. Only the connection's own keyspace reaches a v4 node,
     * and only through the USE described above — which is why the driver takes
     * its own default back off a request before a v4 send, and why a keyspace
     * the caller named is left there to be refused rather than quietly dropped.
     *
     * The name is taken exactly as given, on every protocol version: a keyspace
     * created as `MyKs` is reached by that spelling and not by `myks`.
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
     * How long to wait for the server's answer to a request before giving up
     * with a {@see \Cassandra\Exception\RequestTimeoutException}, in seconds.
     * Null waits indefinitely, and so does INF — the same wait, spelled the way
     * a single request spells it, so that one value can be passed around
     * without having to be translated on the way in; see
     * {@see self::getRequestTimeout()}. Zero or less is refused, being a
     * request that would be out of time before it was sent.
     *
     * Applies to every request that has no explicit timeout of its own. That
     * includes the ones already in flight, whose budgets are measured against
     * this value as it stands when they are looked at rather than as it stood
     * when they were sent: lowering it can therefore leave outstanding requests
     * already overdue. This call does not act on that — nothing is given up on
     * here — but the next wait or poll that examines them does, all in one pass,
     * and since giving up on a request parks its stream id, doing so to enough
     * of them at once can reach {@see ConnectionOptions::$maxOrphanedStreams}
     * and replace the connection.
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
     * Unlike the reads, this never opens a connection: a statement can only be
     * resolved on the connection that sent it, so one that cannot be raises
     * here rather than being waited on — a StatementException where it was
     * never this connection's or was abandoned with it, a
     * RequestTimeoutException where it was already given up on. What the
     * non-blocking calls do have in common is described at
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
     * As with {@see self::tryResolveStatement()}, this never opens a
     * connection, and it reads nothing at all where every statement handed in
     * has been answered already; see {@see self::drainAvailableResponses()}.
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
     * are untouched and can still be waited on. One that is answered in the very
     * pass the time runs out is handed back rather than lost, so a null return
     * really does mean that none of them arrived.
     *
     * Passing no statements at all returns null at once: there is nothing that
     * could become ready, so there is nothing to wait for.
     *
     * A statement that runs out of its own budget is given up on and raises a
     * RequestTimeoutException, which takes precedence over handing one back:
     * where one of them is answered in the same pass that another runs out, the
     * exception is what the caller sees. Nothing is lost by it — the answer
     * stays on its statement and the exception names the ones that ran out, so
     * calling again hands back the one that is ready: a statement already
     * holding its answer is looked for before any of them is judged resolvable,
     * so the ones that ran out do not have to be dropped from $statements for
     * that. They do once nothing is ready any more, though — a statement that
     * can never resolve is reported rather than waited on, so a call with only
     * those left raises instead of waiting.
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
     * Opens the connection first if there is none yet, as every call that reads
     * off the wire on its own account does; see
     * {@see self::drainAvailableResponses()}.
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
     * Opens the connection first if there is none yet, exactly as that call
     * does.
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

    /**
     * {@see self::setConsistency()}, returning the connection so that it can be
     * set in the same expression the connection is used in.
     *
     * Fluent, not immutable: this changes the connection it is called on and
     * hands that same object back, rather than producing a copy that differs in
     * the consistency. There is nothing here a copy could be made of — a
     * connection is a socket and the requests in flight on it — so
     * `$connection->withConsistency(...)` is `setConsistency()` written to
     * chain, and every other reference to the connection sees the change. The
     * `with` prefix on the request options means the other thing: those really
     * do return a new object.
     */
    public function withConsistency(Consistency $consistency): self {
        $this->setConsistency($consistency);

        return $this;
    }

    /**
     * {@see self::setKeyspace()}, returning the connection so that it can be
     * set in the same expression the connection is used in.
     *
     * Fluent, not immutable, for the reason {@see self::withConsistency()}
     * gives — and more pointedly here, since up to v4 the keyspace is the
     * node's session's: there is only one of those per connection, so a copy
     * on another keyspace is not something this could hand back even in
     * principle.
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
