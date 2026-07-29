<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Connection;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request;
use Cassandra\Response;
use Cassandra\Response\Result;
use Cassandra\Statement;
use Cassandra\StatementStatus;
use Cassandra\Value\ValueEncodeConfig;
use Throwable;

/**
 * Everything {@see \Cassandra\Connection} does once a request has been built:
 * opening a connection and taking it through the handshake, sending requests
 * and matching answers to them, keeping request budgets, and proving with a
 * heartbeat that a silent connection is still alive.
 *
 * Kept apart from Connection so that the public API — which is mostly about
 * building requests out of the arguments a caller passes, and is documented for
 * the people who use it — is not read alongside the machinery that carries them
 * out. Connection owns exactly one of these and hands every call that touches
 * the node to it.
 *
 * @internal this is not part of the public API and may change at any time
 */
final class Session {
    /**
     * The connection this session belongs to. Held for one purpose: every
     * {@see Statement} is given it, so that waiting on one goes back through
     * the object the caller actually holds.
     */
    private Connection $connection;

    /**
     * The request timeouts of this connection, and the deadlines its waits are
     * bounded by.
     */
    private Deadline $deadlines;

    /**
     * What becomes of an answer once it has been read.
     */
    private ResponseDispatcher $dispatcher;

    /**
     * How a request gets onto the wire.
     */
    private RequestExecutor $executor;

    private Handshake $handshake;

    /**
     * Whether the connection got past STARTUP, i.e. whether the node accepts
     * ordinary requests such as the heartbeat.
     */
    private bool $handshakeComplete = false;

    /**
     * When a silent connection is next to be probed, and whether the last probe
     * was answered.
     */
    private HeartbeatMonitor $heartbeat;

    private string $keyspace;

    /**
     * The application's event and warnings callbacks.
     */
    private ListenerRegistry $listeners;

    private ?Node $node = null;

    /**
     * Which node to open a connection to, and how each of them has behaved.
     */
    private NodeConnector $nodeConnector;

    private ConnectionOptions $options;

    /**
     * The prepared statements this connection already knows the id of.
     */
    private PreparedResultCache $preparedResultCache;

    private ResponseReader $responseReader;

    /**
     * The requests in flight on this connection, and the budgets they are held
     * to.
     */
    private StatementRegistry $statements;

    /**
     * The stream ids of the current connection.
     */
    private StreamIdPool $streamIds;

    private ?ValueEncodeConfig $valueEncodeConfig = null;

    private ProtocolVersion $version;

    /**
     * @param array<NodeConfig> $nodes
     */
    public function __construct(
        Connection $connection,
        array $nodes,
        string $keyspace,
        ConnectionOptions $options,
    ) {

        $this->connection = $connection;
        $this->keyspace = $keyspace;
        $this->options = $options;
        $this->version = $options->initialProtocolVersion;
        $this->responseReader = new ResponseReader();
        $this->handshake = new Handshake($options);
        $this->heartbeat = new HeartbeatMonitor($options);
        $this->listeners = new ListenerRegistry();
        $this->nodeConnector = new NodeConnector($nodes, $options->nodeSelectionStrategy->createSelector());
        $this->streamIds = new StreamIdPool();
        $this->deadlines = new Deadline($options->requestTimeoutInSeconds);
        $this->statements = new StatementRegistry($this->streamIds, $this->deadlines);
        $this->preparedResultCache = new PreparedResultCache($options->preparedResultCacheSize);
        $this->dispatcher = new ResponseDispatcher($this, $this->listeners, $this->preparedResultCache);
        $this->executor = new RequestExecutor(
            $this,
            $this->dispatcher,
            $this->statements,
            $this->streamIds,
            $this->deadlines,
            $this->preparedResultCache,
        );
    }

    /**
     * Send a follow-up request on the stream id a statement already holds; see
     * {@see RequestExecutor::chainAsyncRequest()}.
     *
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     */
    public function chainAsyncRequest(Request\Request $request, Statement $statement): void {

        $this->executor->chainAsyncRequest($request, $statement);
    }

    public function configureValueEncoding(ValueEncodeConfig $config): void {
        $this->valueEncodeConfig = $config;
    }

    /**
     * See {@see \Cassandra\Connection::connect()}.
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
    public function connect(): void {
        if ($this->node !== null) {
            return;
        }

        $this->preparedResultCache->clear();
        $this->handshakeComplete = false;
        $this->streamIds->reset();

        $node = $this->node = $this->nodeConnector->open();

        $this->heartbeat->anchor();

        try {
            $this->completeHandshake($node);
        } catch (Throwable $e) {
            $this->disconnect();

            throw $e;
        }
    }

    public function connection(): Connection {

        return $this->connection;
    }

    public function disconnect(): void {

        $this->preparedResultCache->clear();
        $this->statements->abandonAll();
        $this->heartbeat->forgetProbe();
        $this->handshakeComplete = false;
        $this->streamIds->reset();
        $this->version = $this->options->initialProtocolVersion;
        $this->responseReader->reset();

        if ($this->node === null) {
            return;
        }

        $node = $this->node;
        $this->node = null;
        $node->close();
    }

    /**
     * See {@see \Cassandra\Connection::drainAvailableResponses()}.
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
        $count = 0;
        $readAttempted = false;
        $drainedResponses = false;
        while ($count < $max) {
            $readAttempted = true;
            $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                break;
            }
            $count++;
        }

        $this->keepNonBlockingBookkeeping($readAttempted);

        return $count;
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
    public function getConnectedNode(): Node {

        $node = $this->node;
        if ($node === null) {
            $this->connect();

            $node = $this->node;
            if ($node === null) {
                throw new ConnectionException('Client is not connected to any node. This should never happen.', ExceptionCode::CONNECTION_NOT_CONNECTED->value, [
                    'operation' => 'getConnectedNode',
                ]);
            }
        }

        return $node;
    }

    public function getKeyspace(): string {
        return $this->keyspace;
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
    public function getNewStreamId(?float $requestTimeoutInSeconds = null): int {

        $waitDeadline = null;
        $waitStarted = false;
        $deadlineExceeded = false;

        while (true) {
            $streamId = $this->streamIds->claim();
            if ($streamId !== null) {
                return $streamId;
            }

            if (!$waitStarted) {
                // Every id has been handed out, so one can only come back with
                // an answer. This is an ordinary bounded wait like any other: a
                // transport read timeout here only means the connection was
                // quiet, so it must not tear the connection down, and the
                // heartbeat is what proves the node is still there while we
                // wait.
                //
                // The wait is part of the request that is asking for the id, so
                // it is held to that request's own budget where it has one, and
                // only otherwise to the connection default. With neither it
                // waits indefinitely, as every other wait in this class does —
                // an id can still come free at any time. It is anchored on the
                // first pass that finds nothing, so that the passes which found
                // an id straight away cost no clock reading at all.
                $waitStarted = true;
                $waitDeadline = $this->deadlines->at($requestTimeoutInSeconds);
            }

            $deadline = $this->deadlines->earlier($waitDeadline, $this->pendingStatementsDeadline());

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Giving up on a request parks its id rather than releasing it,
                // so this does not by itself make one available; it is done here
                // so that overdue requests are still finished on time.
                $this->timeOutExpiredStatements();

                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    throw new ConnectionException(
                        'Every stream id the protocol allows is already in use and none was released in time, so this request could not be sent',
                        ExceptionCode::CONNECTION_STREAM_IDS_EXHAUSTED->value,
                        [
                            'operation' => 'getNewStreamId',
                            'max_stream_id' => StreamIdPool::MAX_STREAM_ID,
                            'statements_in_flight' => $this->statements->count(),
                            'orphaned_streams' => $this->streamIds->orphanedCount(),
                            'request_timeout_seconds' => $this->deadlines->describe($requestTimeoutInSeconds ?? $this->deadlines->getRequestTimeout()),
                        ]
                    );
                }
            }
        }
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
    public function getNextResponseForStream(int $streamId, ?float $requestTimeoutInSeconds = null, ?Statement $statement = null, ?string $requestClass = null): Response\Response {

        $deadlineExceeded = false;

        // The sync path has no statement to carry an anchor for it, and it
        // wrote its request immediately before getting here, so its budget is
        // anchored once, now. A statement's own anchor is read per pass below.
        $sentAt = microtime(true);

        while (true) {
            if ($statement !== null) {
                // Checked every pass, not just before the loop: once the
                // statement is finished the stream id stops being ours to give
                // up on, and parking it below would hold back an id that has
                // already gone back into circulation — or, after the connection
                // was replaced, one the next connection may hand to somebody
                // else, whose answer would then be discarded as a late one.
                $alreadyAnswered = $statement->peekResponse();
                if ($alreadyAnswered !== null) {
                    return $alreadyAnswered;
                }

                // A dead end reached while we were reading, which today only a
                // disconnect can do — and every path that disconnects raises on
                // its own, so this is the braces to that belt. Waiting on
                // further passes would be waiting for an answer that cannot
                // come.
                //
                // A statement given up on is reported as the timeout it is,
                // matching {@see StatementRegistry::assertResolvable()} when it
                // finds the same statement in the same state before the wait
                // begins: which side of a read the budget ran out on is timing,
                // not a difference the caller should have to tell apart.
                if ($statement->isTimedOut()) {
                    throw new RequestTimeoutException(
                        'This request timed out and was given up on while its answer was outstanding, so it can no longer be resolved. Send it again.',
                        ExceptionCode::REQUESTTIMEOUT_WAITING_FOR_STATEMENTS->value,
                        [
                            'operation' => 'getResponseForStatement',
                            'stream_id' => $streamId,
                            'request_class' => get_class($statement->getRequest()),
                        ],
                        timedOutStatements: [$statement],
                    );
                }

                // Abandoned, on the other hand, means the statement was given up
                // on without ever running out of time — the connection it was
                // sent on went away, or a follow-up request of its own never
                // reached the node — so it is reported as a connection failure
                // rather than a timeout. It is deliberately not the StatementException
                // StatementRegistry::assertResolvable() raises: the sync path shares
                // this loop and has no statement at all, so raising that here
                // would put StatementException on the @throws list of every
                // method that ends up in syncRequest(), for a failure none of
                // them can produce.
                if ($statement->isAbandoned()) {
                    throw new ConnectionException(
                        'The statement being waited on was given up on while its answer was outstanding, so this connection can no longer resolve it',
                        ExceptionCode::CONNECTION_STATEMENT_NO_LONGER_RESOLVABLE->value,
                        [
                            'operation' => 'getResponseForStatement',
                            'stream_id' => $streamId,
                            'request_class' => get_class($statement->getRequest()),
                            'abandoned' => true,
                        ]
                    );
                }
            }

            // The deadline is recomputed every pass rather than taken once: a
            // chained follow-up request (repreparation, auto-prepare) restarts
            // the statement's budget, and a deadline captured before the loop
            // would hold that new request to the budget of the one it replaced.
            $ownDeadline = $this->deadlines->at($requestTimeoutInSeconds, $statement?->getSentAt() ?? $sentAt);

            // The other requests in flight keep their own budgets while this
            // one waits, so one of them going overdue is noticed here too
            // rather than only whenever its caller next waits on it.
            $deadline = $this->deadlines->earlier($ownDeadline, $this->pendingStatementsDeadline());

            $response = $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($response !== null && $response->getStream() === $streamId) {
                return $response;
            }

            if ($deadlineExceeded) {
                // The deadline that fired may well belong to another request,
                // so everything overdue is given up on first and only then is
                // it decided whether this request is among them.
                $expired = $this->timeOutExpiredStatements();

                $statementStillPending = false;

                if ($statement !== null) {
                    $mine = $this->statements->intersect($expired, [$statement]);
                    if ($mine !== []) {
                        $this->statements->reportTimedOut($mine, 'getResponseForStatement');
                    }

                    // Not among the expired, so it is still waiting — its budget
                    // has not run out, or a chained follow-up restarted it while
                    // we were reading. Either way the connection's own
                    // bookkeeping is authoritative for a statement, and the
                    // fallback below must not second-guess it; carry on against
                    // the deadline the next pass computes.
                    $statementStillPending = $this->statements->has($streamId);
                }

                if (
                    !$statementStillPending
                    && $ownDeadline !== null
                    && microtime(true) >= $ownDeadline
                ) {
                    $this->timeOutStream(
                        $streamId,
                        $statement === null ? 'syncRequest' : 'getResponseForStatement',
                        $requestTimeoutInSeconds,
                        $requestClass,
                        $statement,
                    );
                }
            }
        }
    }

    public function getNode(): ?Node {
        return $this->node;
    }

    /**
     * Returns the protocol version used by this connection.
     * Before connecting, it will return the initial protocol version,
     * as set in the connection options.
     */
    public function getProtocolVersion(): ProtocolVersion {
        return $this->version;
    }

    /**
     * See {@see \Cassandra\Connection::getResponseForStatement()}.
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

        if ($statement->isResultReady()) {
            return $statement->getResponse();
        }

        $this->statements->assertResolvable($statement);

        $response = $this->getNextResponseForStream(
            streamId: $statement->getStreamId(),
            requestTimeoutInSeconds: $statement->getRequestTimeout(),
            statement: $statement,
        );

        if ($response instanceof Response\Error) {
            throw $response->getException();
        }

        return $response;
    }

    public function handleNodeException(Node $node): void {
        $this->nodeConnector->recordFailure($node->getConfig());
        $this->disconnect();
    }

    public function isConnected(): bool {
        return $this->node !== null;
    }

    /**
     * The application's event and warnings callbacks.
     */
    public function listeners(): ListenerRegistry {
        return $this->listeners;
    }

    /**
     * Park a stream id whose fate a failure left undecided.
     *
     * Reached when a request ends in something that is neither a node failure —
     * which takes the whole pool with it — nor a timeout, which parks the id
     * itself: a frame the reader could not make sense of, above all. Whether
     * the answer was consumed is exactly what such a failure leaves unknown, so
     * the id is parked rather than recycled, for the reason
     * {@see StatementRegistry::expire()} gives, and released if and when
     * an answer for it does turn up.
     *
     * Not enforcing the orphan limit here is deliberate: this runs while
     * another failure is on its way out, and the connection being replaced is
     * not what the caller should be told about. The next piece of bookkeeping
     * enforces it.
     */
    public function parkUnresolvedStream(int $streamId): void {

        if ($this->node === null || $this->streamIds->isOrphaned($streamId)) {
            return;
        }

        $this->statements->forget($streamId);
        $this->streamIds->park($streamId);
    }

    /**
     * The node took a request, which is enough to clear a failure recorded
     * against it. Whether it answers is a separate question, recorded
     * separately.
     */
    public function recordNodeSuccess(NodeConfig $config): void {

        $this->nodeConnector->recordSuccess($config);
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
    public function sendAsyncRequest(Request\Request $request, ?float $requestTimeoutInSeconds = null): Statement {

        return $this->executor->sendAsyncRequest($request, $requestTimeoutInSeconds);
    }

    /**
     * @param int $repreparationDepth see {@see RequestExecutor::sendSyncRequest()}
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
    public function sendSyncRequest(Request\Request $request, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): Response\Response {

        return $this->executor->sendSyncRequest($request, $requestTimeoutInSeconds, $repreparationDepth);
    }

    /**
     * See {@see \Cassandra\Connection::setKeyspace()}.
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

        $this->keyspace = $keyspace;

        if (!$this->isConnected()) {
            return;
        }

        if ($this->version->value < ProtocolVersion::V5->value) {
            $response = $this->sendSyncRequest(new Request\Query("USE {$this->keyspace};"));
            if (!($response instanceof Response\Result)) {
                throw new ConnectionException('Unexpected response type during setKeyspace', ExceptionCode::CONNECTION_SET_KEYSPACE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result::class,
                    'received' => get_class($response),
                    'operation' => 'setKeyspace',
                    'keyspace' => $this->keyspace,
                ]);
            }
        }
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function setRequestTimeout(?float $requestTimeoutInSeconds): void {
        $this->deadlines->setRequestTimeout($requestTimeoutInSeconds);
    }

    /**
     * See {@see \Cassandra\Connection::tryReadNextEvent()}.
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
        $drainedResponses = false;
        while (true) {
            $event = $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                $this->keepNonBlockingBookkeeping(readAttempted: true);

                return null;
            }
            if ($event instanceof Response\Event) {
                $this->keepNonBlockingBookkeeping(readAttempted: true);

                return $event;
            }
        }
    }

    /**
     * See {@see \Cassandra\Connection::tryReadNextResponse()}.
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
        $drainedResponses = false;
        while (true) {
            $response = $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                $this->keepNonBlockingBookkeeping(readAttempted: true);

                return null;
            }
            if ($response !== null) {
                $this->keepNonBlockingBookkeeping(readAttempted: true);

                return $response;
            }
        }
    }

    /**
     * See {@see \Cassandra\Connection::tryResolveStatement()}.
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
        if ($statement->isResultReady()) {
            return true;
        }

        $this->timeOutExpiredStatements();

        $this->statements->assertResolvable($statement);

        $drainedResponses = false;
        do {
            $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                break;
            }
            if ($statement->isResultReady()) {
                $this->keepNonBlockingBookkeeping(readAttempted: true);

                return true;
            }
        } while (true);

        $this->keepNonBlockingBookkeeping(readAttempted: true);

        return $statement->isResultReady();
    }

    /**
     * See {@see \Cassandra\Connection::tryResolveStatements()}.
     *
     * @param array<Statement> $statements
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

        $this->timeOutExpiredStatements();

        $initialReady = 0;
        foreach ($statements as $s) {
            if ($s->isResultReady()) {
                $initialReady++;

                continue;
            }

            $this->statements->assertResolvable($s);
        }

        $processed = 0;
        $readAttempted = false;
        $drainedResponses = false;
        while (
            $processed < $max
        ) {
            $hasUnresolvedStatements = false;
            foreach ($statements as $s) {
                if (!$s->isResultReady()) {
                    $hasUnresolvedStatements = true;

                    break;
                }
            }
            if (!$hasUnresolvedStatements) {
                break;
            }

            $readAttempted = true;
            $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                break;
            }
            $processed++;
        }

        $this->keepNonBlockingBookkeeping($readAttempted, statementsAlreadyExpired: !$readAttempted);

        $ready = 0;
        foreach ($statements as $s) {
            if ($s->isResultReady()) {
                $ready++;
            }
        }

        return $ready - $initialReady;
    }

    /**
     * See {@see \Cassandra\Connection::waitForAllPendingStatements()}.
     *
     * @param ?float $timeoutInSeconds
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

        $waitDeadline = $this->deadlines->in($timeoutInSeconds);
        $deadlineExceeded = false;

        while ($this->statements->pending($this->heartbeat->probe())) {
            // Recomputed per pass: each statement carries its own budget from
            // when it was sent, and resolved ones drop out of the reckoning.
            $deadline = $this->deadlines->earlier($waitDeadline, $this->pendingStatementsDeadline());

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                $expired = $this->timeOutExpiredStatements();
                if ($expired !== []) {
                    $this->statements->reportTimedOut($expired, 'waitForAllPendingStatements');
                }

                // As in waitForStatements(): only the caller's own bound ends
                // the wait. A statement deadline firing without anything having
                // expired means that statement was answered in the same pass,
                // which is no reason to stop waiting for the rest.
                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    return;
                }
            }
        }
    }

    /**
     * See {@see \Cassandra\Connection::waitForAnyStatement()}.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds
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

        $waitDeadline = $this->deadlines->in($timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            foreach ($statements as $s) {
                if ($s->isResultReady()) {
                    return $s;
                }

                $this->statements->assertResolvable($s);
            }

            // Bounded by every request in flight, not just the ones asked
            // about: each keeps its own budget while this waits, so one going
            // overdue is noticed here rather than only whenever its own caller
            // next waits on it. The bound only decides when to come up for air;
            // which statement is answered, and which of them is the caller's
            // business, is decided below rather than here.
            $deadline = $this->deadlines->earlier($waitDeadline, $this->pendingStatementsDeadline());

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Everything overdue is given up on, but only the statements
                // the caller handed in are reported as this call's failure.
                $expired = $this->statements->intersect($this->timeOutExpiredStatements(), $statements);
                if ($expired !== []) {
                    $this->statements->reportTimedOut($expired, 'waitForAnyStatement');
                }

                // Only the caller's wait bound is left, or a statement that is
                // none of their business ran out; either way there is nothing
                // ready to hand back.
                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    return null;
                }
            }
        }
    }

    /**
     * See {@see \Cassandra\Connection::waitForNextEvent()}.
     *
     * @param ?float $timeoutInSeconds
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

        $waitDeadline = $this->deadlines->in($timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            // Requests sent on this connection keep their deadlines while it is
            // being pumped for events, so one going overdue is noticed here too
            // rather than only whenever the caller next waits on it.
            $deadline = $this->deadlines->earlier($waitDeadline, $this->pendingStatementsDeadline());

            $event = $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Requests that ran out are finished here, but an event listener
                // is not the one who asked about them, so its loop is not
                // interrupted for them; the caller finds out from the statement.
                // This runs before the event is handed back so that an overdue
                // request is still given up on in the pass that brought one.
                //
                // It can still raise: giving up on enough requests reaches the
                // orphaned-stream limit, which replaces the connection. That is
                // this loop's business — the connection it reads from is gone.
                $this->timeOutExpiredStatements();
            }

            if ($event instanceof Response\Event) {
                return $event;
            }

            if ($deadlineExceeded && $waitDeadline !== null && microtime(true) >= $waitDeadline) {
                return null;
            }
        }
    }

    /**
     * See {@see \Cassandra\Connection::waitForNextResponse()}.
     *
     * @param ?float $timeoutInSeconds
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

        $waitDeadline = $this->deadlines->at($timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            // Bounded by whichever comes first: how long the caller is willing
            // to wait, or the budget of the request that expires soonest.
            $deadline = $this->deadlines->earlier($waitDeadline, $this->pendingStatementsDeadline());

            $response = $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Requests that ran out are finished here, but the caller asked
                // for the next response, not about any request in particular,
                // so that is not this call's failure to report: they find out
                // from the statement. Nothing came, which is not a failure at
                // all — the connection and its other requests carry on and the
                // wait can simply be repeated. This runs before the response is
                // handed back so that an overdue request is still given up on in
                // the pass that brought one.
                //
                // It can still raise: giving up on enough requests reaches the
                // orphaned-stream limit, which replaces the connection. That is
                // every caller's business, this one included.
                $this->timeOutExpiredStatements();
            }

            if ($response !== null) {
                return $response;
            }

            if ($deadlineExceeded && $waitDeadline !== null && microtime(true) >= $waitDeadline) {
                return null;
            }
        }
    }

    /**
     * See {@see \Cassandra\Connection::waitForStatements()}.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds
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

        $waitDeadline = $this->deadlines->in($timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            $hasUnresolvedStatements = false;
            foreach ($statements as $s) {
                if (!$s->isResultReady()) {
                    $this->statements->assertResolvable($s);

                    $hasUnresolvedStatements = true;

                    break;
                }
            }

            if (!$hasUnresolvedStatements) {
                break;
            }

            // As in waitForAnyStatement(): bounded by every request in flight,
            // so none of them overshoots its budget while this call waits on a
            // statement with a longer one. Widening the bound this way is safe
            // whatever $statements holds — it only decides when to come up for
            // air, never which statement is answered or reported.
            $deadline = $this->deadlines->earlier($waitDeadline, $this->pendingStatementsDeadline());

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Everything overdue is given up on, but only the statements
                // the caller handed in are reported as this call's failure.
                $expired = $this->statements->intersect($this->timeOutExpiredStatements(), $statements);
                if ($expired !== []) {
                    $this->statements->reportTimedOut($expired, 'waitForStatements');
                }

                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    return;
                }
            }
        }
    }

    /**
     * Prove that a silent connection is still alive.
     *
     * The timing and the state of the probe belong to
     * {@see HeartbeatMonitor}; what is here is the part that cannot live there:
     * sending the OPTIONS request, and failing the connection when the answer
     * does not come.
     *
     * This runs while waiting for a response as well as while waiting for
     * events, because the protocol multiplexes stream ids: the heartbeat is
     * answered on its own stream while a slow request is still being computed,
     * so a dead connection is caught in interval + timeout no matter how
     * generous the request timeout is.
     *
     * A read that could outlast the probe's schedule would delay it. Neither
     * kind does. {@see self::readResponseUntil()}, which is what the waits use,
     * bounds every read by {@see HeartbeatMonitor::nextActionAt()} as well as by
     * the caller's deadline — that is what lets the transport's stall window be
     * long, or absent. {@see self::readResponse()}, which is what the
     * non-blocking calls use, needs no such bound: it never waits at all.
     *
     * Callers must read before calling this, so that an answer already sitting
     * in the socket buffer is never mistaken for an unanswered heartbeat. The
     * waits all read on their way here; the non-blocking calls, which can
     * return before they get that far, are held to it by
     * {@see self::keepNonBlockingBookkeeping()}.
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
    private function checkHeartbeat(): void {

        if ($this->heartbeat->isDormant($this->handshakeComplete)) {
            return;
        }

        $now = microtime(true);

        $probe = $this->heartbeat->probe();
        if ($probe !== null) {

            if ($probe->isResultReady()) {
                $this->heartbeat->forgetProbe();

                return;
            }

            if (!$this->heartbeat->probeIsOverdue($now)) {
                return;
            }

            $node = $this->node;
            $context = [
                'operation' => 'heartbeat',
                'heartbeat_timeout_seconds' => $this->heartbeat->timeoutInSeconds(),
                'host' => $node?->getConfig()->host,
                'port' => $node?->getConfig()->port,
            ];

            if ($node !== null) {
                $this->handleNodeException($node);
            } else {
                $this->disconnect();
            }

            throw new ConnectionException(
                'Node did not answer the connection heartbeat in time',
                ExceptionCode::CONNECTION_HEARTBEAT_TIMEOUT->value,
                $context
            );
        }

        if (!$this->heartbeat->isProbeDue($now)) {
            return;
        }

        // Skipped rather than waited for when every stream id is in use.
        // Claiming one would read, and reading here can block for a whole
        // request timeout — which the non-blocking calls that reach this
        // through {@see self::keepNonBlockingBookkeeping()} promised not to do.
        // Nothing is lost by it: an id space with all 32768 in flight is not a
        // connection anyone needs a probe to have an opinion about, and if
        // those requests really are going unanswered they run out of time,
        // orphan their ids and take the connection with them at
        // {@see ConnectionOptions::$maxOrphanedStreams}.
        if (!$this->streamIds->hasImmediate()) {
            return;
        }

        $this->heartbeat->beginSending();

        try {
            $this->heartbeat->recordProbe($this->sendAsyncRequest(new Request\Options()));
        } finally {
            $this->heartbeat->endSending();
        }
    }

    /**
     * Take a freshly opened connection through OPTIONS, STARTUP and, if the
     * node asks for it, authentication.
     *
     * Kept apart from {@see self::connect()} so that everything it does can be
     * unwound in one place when any of it fails.
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
    private function completeHandshake(Node $node): void {

        $response = $this->sendSyncRequest(new Request\Options());
        if (!($response instanceof Response\Supported)) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('OPTIONS handshake failed: unexpected response type', ExceptionCode::CONNECTION_OPTIONS_UNEXPECTED_RESPONSE->value, [
                'operation' => 'connect/options',
                'expected' => Response\Supported::class,
                'received' => get_class($response),
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
            ]);
        }

        $negotiated = $this->handshake->negotiate($response, $node, $this->version);
        $this->version = $negotiated['version'];
        $startupOptions = $negotiated['startupOptions'];

        $response = $this->sendSyncRequest(new Request\Startup($startupOptions));

        if ($response instanceof Response\Authenticate) {
            $nodeConfig = $node->getConfig();

            if (!$nodeConfig->username || !$nodeConfig->password) {
                throw new ConnectionException('Username and password must not be empty.', ExceptionCode::CONNECTION_AUTH_MISSING_CREDENTIALS->value, [
                    'operation' => 'connect/authenticate',
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'auth_required' => true,
                ]);
            }

            // From here every frame is framed and compressed as the node has
            // just agreed, the AUTH_RESPONSE below included.
            $node = $this->node = $this->handshake->wrapNode($node, $this->version, $startupOptions);

            $authResult = $this->sendSyncRequest(new Request\AuthResponse($nodeConfig->username, $nodeConfig->password));
            if (!($authResult instanceof Response\AuthSuccess)) {
                throw new ConnectionException('Authentication failed.', ExceptionCode::CONNECTION_AUTH_FAILED->value, [
                    'operation' => 'connect/authenticate',
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'username' => $nodeConfig->username,
                ]);
            }
        } elseif ($response instanceof Response\Ready) {
            $node = $this->node = $this->handshake->wrapNode($node, $this->version, $startupOptions);
        } else {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('Connection startup failed: unexpected response type', ExceptionCode::CONNECTION_STARTUP_UNEXPECTED_RESPONSE->value, [
                'operation' => 'connect/startup',
                'expected' => [Response\Authenticate::class, Response\Ready::class],
                'received' => get_class($response),
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
            ]);
        }

        $this->handshakeComplete = true;

        if ($this->keyspace && $this->version->value < ProtocolVersion::V5->value) {
            $this->sendSyncRequest(new Request\Query("USE {$this->keyspace};"));
        }
    }

    /**
     * Replace the connection once it is holding back too many stream ids.
     *
     * A parked id is only released when its late answer arrives, so a node that
     * keeps leaving requests unanswered would tie up more and more of the id
     * space. Past the configured limit the connection is closed and started
     * over — and unlike a single request running out, that is every caller's
     * business: the connection they were using is gone and the requests still
     * in flight on it were abandoned with it, so it is raised rather than done
     * quietly, whatever the caller happened to be waiting for.
     *
     * The node is not recorded as failed: it may simply be slow, and this only
     * says that this particular connection has accumulated too much debris.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function enforceOrphanedStreamLimit(): void {

        $orphanedCount = $this->streamIds->orphanedCount();

        if ($orphanedCount <= max(0, $this->options->maxOrphanedStreams)) {
            return;
        }

        $node = $this->node;
        $context = [
            'operation' => 'enforceOrphanedStreamLimit',
            'orphaned_streams' => $orphanedCount,
            'max_orphaned_streams' => $this->options->maxOrphanedStreams,
            'abandoned_statements' => $this->statements->count(),
            'host' => $node?->getConfig()->host,
            'port' => $node?->getConfig()->port,
        ];

        $this->disconnect();

        throw new ConnectionException(
            'Too many requests were given up on without their answers ever arriving, so the connection was replaced',
            ExceptionCode::CONNECTION_TOO_MANY_ORPHANED_STREAMS->value,
            $context
        );
    }

    /**
     * What the non-blocking calls owe the connection once they have read.
     *
     * They never wait, so nothing that runs out of time is a failure of theirs
     * to report — but the two things a wait does besides waiting still have to
     * happen, or an application that only ever polls would get neither. Request
     * budgets are kept, so that a statement nobody blocks on is still given up
     * on and its stream id eventually released; and the connection is probed,
     * so that one which died quietly is noticed at all. A caller learns about
     * an expired statement from the statement itself, as it does from the waits
     * that were not asked about it ({@see self::waitForNextResponse()}).
     *
     * $readAttempted says whether the caller got as far as reading from the
     * transport. It usually has, but a poll can return before it does — over
     * statements that were all resolved already, or with a limit of zero — and
     * the probe must not run then, for the reason {@see self::checkHeartbeat()}
     * gives: an answer still sitting in the receive buffer would be taken for
     * an unanswered probe and would cost a healthy connection. Such a call has
     * learned nothing about the connection either, so there is nothing for the
     * probe to decide.
     *
     * $statementsAlreadyExpired is for the callers that keep the budgets on
     * their way in — the ones that have to, so that
     * {@see StatementRegistry::assertResolvable()} sees a statement which ran
     * out as the timeout it is. Where such a call then returns without reading,
     * nothing has happened in between for a second pass over the pending
     * statements to find: no wall clock was spent on the transport, and the
     * connection was not touched.
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
    private function keepNonBlockingBookkeeping(bool $readAttempted, bool $statementsAlreadyExpired = false): void {

        if (!$statementsAlreadyExpired) {
            $this->timeOutExpiredStatements();
        }

        if ($readAttempted) {
            $this->checkHeartbeat();
        }
    }

    /**
     * When the first of the requests in flight will have used up its budget, so
     * that a wait comes up for air in time to give up on it. Null when none of
     * them is bounded.
     */
    private function pendingStatementsDeadline(): ?float {

        return $this->statements->earliestDeadline($this->heartbeat->probe());
    }

    /**
     * Dispatch a freshly read response: resolve the statement it belongs to,
     * notify event listeners and record that the node is answering.
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
    private function processResponse(Response\Response $response, Node $node): ?Response\Response {

        $orphanedStreamId = $response->getStream();
        if ($this->streamIds->isOrphaned($orphanedStreamId)) {
            // The late answer to a statement that was already given up on. It
            // has nowhere to go, but its arrival proves the stream id is free
            // again, so it goes back into circulation here.
            $this->streamIds->releaseParked($orphanedStreamId);

            $this->heartbeat->recordResponse();
            $this->nodeConnector->recordSuccess($node->getConfig());

            return null;
        }

        if ($this->valueEncodeConfig !== null && ($response instanceof Result\RowsResult)) {
            $response->configureValueEncoding($this->valueEncodeConfig);
        }

        $streamId = $response->getStream();
        $statement = $this->statements->get($streamId);
        if ($statement !== null) {
            $this->statements->forget($streamId);

            // Taken out of the pending map before its answer is handled, so
            // handling that ends in a failure rather than a result — the
            // repreparation limit above all, see
            // {@see ResponseDispatcher::MAX_REPREPARATIONS} — must not leave it
            // neither pending nor finished. It is given up on
            // instead, or a caller would be left waiting on a statement this
            // connection has forgotten, holding a stream id nothing releases.
            $handled = false;

            try {
                $response = $this->dispatcher->handleResponse($statement->getRequest(), $response, $statement);
                $handled = true;
            } finally {
                if (!$handled) {
                    $this->statements->releaseAfterFailedResponseHandling($statement, $streamId, $this->node !== null);
                }
            }

            if ($response !== null) {
                $statement->setResponse($response);
                $this->streamIds->release($streamId);
            }
        }

        if ($response instanceof Response\Event) {
            $this->listeners->notifyEvent($response);
        }

        $this->heartbeat->recordResponse();
        $this->nodeConnector->recordSuccess($node->getConfig());

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
    private function readResponse(bool &$drainedResponses = false): ?Response\Response {
        $node = $this->getConnectedNode();

        try {
            $response = $this->responseReader->readResponse($node, $this->version, Node::DO_NOT_WAIT);
        } catch (NodeException $e) {
            if (!$e->isReadTimeout()) {
                $this->handleNodeException($node);

                throw $e;
            }

            // A read timeout says nothing about the connection, exactly as in
            // readResponseUntil(). A read that was told not to wait should not
            // be able to produce one — both transports settle readiness with a
            // zero-timeout select() and never arm their stall window for it —
            // so this is the braces to that belt: tearing the connection down
            // over a quiet moment would be the worst possible reading of a
            // timeout that no wait was even attempted for.
            $response = null;
        }

        if ($response === null) {
            $drainedResponses = true;

            return null;
        }

        $drainedResponses = false;

        return $this->processResponse($response, $node);
    }

    /**
     * One read, with $deadline rather than the transport timeout deciding when
     * the wait is over.
     *
     * A transport read timeout only means that the connection was silent for
     * its stall window. The response reader keeps any partially consumed frame,
     * so resuming the read is always safe — which makes the transport timeout
     * the wrong place to decide that a slow server, or an event stream with
     * nothing to report, has waited long enough. That decision belongs to
     * $deadline, an absolute microtime; null waits indefinitely.
     *
     * $deadline is handed to the read rather than merely consulted before it,
     * so a read never outlives it: a budget of half a second is reported after
     * half a second even where the transport would have sat in the same read
     * for fifteen, and one that has already passed — an expired statement, or a
     * caller waiting with a timeout of 0 — costs no wait at all. Whatever has
     * already arrived is consumed either way, so an answer sitting in the
     * buffer resolves the wait instead of being reported as overdue.
     *
     * The read is bounded by {@see HeartbeatMonitor::nextActionAt()} as well,
     * because a wait with no deadline of its own still has to come up for air
     * often enough to probe a connection that has gone quiet. Between the two
     * of them the transport's stall window is no longer what decides when
     * anything happens, so it is free to be long, or absent altogether — and
     * where neither of them bounds the read, the stall window is what is left
     * to end it, so its elapsing is reported as the transport failure it is
     * rather than swallowed like the timeouts that a bounded read produces.
     *
     * Returns null when no complete response was available, with
     * $deadlineExceeded telling the caller whether the wait may continue. The
     * two are independent: a response for some other request may well arrive
     * after this one's deadline has passed, and the caller has to act on both.
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
    private function readResponseUntil(?float $deadline, bool &$deadlineExceeded = false): ?Response\Response {
        $deadlineExceeded = false;

        $node = $this->getConnectedNode();

        // The read itself is bounded, not just the decision whether to start
        // one. Beside the caller's deadline it is held to when the heartbeat
        // next needs attention, which is the one thing that still has to happen
        // on a connection nobody has set a deadline on.
        $readDeadline = $this->deadlines->earlier($deadline, $this->heartbeat->nextActionAt($this->handshakeComplete, $this->streamIds->hasImmediate()));

        try {
            $response = $this->responseReader->readResponse($node, $this->version, $readDeadline);
        } catch (NodeException $e) {
            if (!$e->isReadTimeout()) {
                $this->handleNodeException($node);

                throw $e;
            }

            // A read timeout only says the connection was silent for its stall
            // window, which decides nothing while something else still bounds
            // this wait: the caller's deadline or the next heartbeat will end
            // it, and a coordinator that is still thinking looks exactly like
            // this. With neither — $readDeadline is what the two of them
            // produced — the stall window is the only judgement available, and
            // it is the transport's own: a connection that has made no progress
            // for its whole window, with nothing left to notice it, is treated
            // as failed rather than waited on forever.
            if ($readDeadline === null) {
                $this->handleNodeException($node);

                throw $e;
            }

            $response = null;
        }

        // Checked after the read, not before it: the read is bounded by the
        // deadline rather than merely started on the strength of it, so it is
        // only on the way out that the deadline can have passed. Reaching it is
        // not exclusive with having read something either — a response for
        // another request may well arrive in the same pass — so both are
        // reported and it is left to the caller to act on each.
        if ($deadline !== null && microtime(true) >= $deadline) {
            $deadlineExceeded = true;
        }

        if ($response === null) {
            return null;
        }

        return $this->processResponse($response, $node);
    }

    /**
     * Give up on every request whose budget has run out, and report them.
     *
     * The bookkeeping is {@see StatementRegistry::expire()}'s; what is added
     * here is the one consequence that is the connection's rather than the
     * statements': enough ids held back replaces the connection.
     *
     * @return array<Statement>
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function timeOutExpiredStatements(): array {

        $expired = $this->statements->expire($this->heartbeat->probe());

        $this->enforceOrphanedStreamLimit();

        return $expired;
    }

    /**
     * Give up on whatever was waiting on a stream id, keeping the connection.
     *
     * See {@see StatementRegistry::expire()} for why parking the id rather
     * than recycling it is what makes this safe.
     *
     * A statement is marked as timed out here as well, so that one this call
     * gives up on can never be left reporting neither a result nor a timeout —
     * which would leave a caller retrying a statement that can never resolve.
     *
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    private function timeOutStream(int $streamId, string $operation, ?float $requestTimeoutInSeconds, ?string $requestClass = null, ?Statement $statement = null): never {

        $this->statements->forget($streamId);
        $this->streamIds->park($streamId);

        if ($statement !== null) {
            $statement->setStatus(StatementStatus::TIMED_OUT);
        }

        $this->enforceOrphanedStreamLimit();

        throw new RequestTimeoutException(
            'Timed out waiting for the server to answer the request',
            ExceptionCode::REQUESTTIMEOUT_WAITING_FOR_RESPONSE->value,
            [
                'operation' => $operation,
                'stream_id' => $streamId,
                'request_class' => $requestClass ?? ($statement === null ? null : get_class($statement->getRequest())),
                'request_timeout_seconds' => $this->deadlines->describe($requestTimeoutInSeconds ?? $this->deadlines->getRequestTimeout()),
                'orphaned_streams' => $this->streamIds->orphanedCount(),
            ],
            timedOutStatements: $statement === null ? [] : [$statement],
        );
    }
}
