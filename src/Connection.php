<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Connection\FrameCodec;
use Cassandra\Protocol\Opcode;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Connection\RequestCompressor;
use Cassandra\Connection\ResponseReader;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\StatementException;
use Cassandra\Protocol\Header;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Response\Result;
use Cassandra\Response\StreamReader;
use Cassandra\Value\NotSet;
use Cassandra\Value\ValueBase;
use Cassandra\Value\ValueEncodeConfig;
use SplQueue;

final class Connection {
    /**
     * Highest stream id a client may use. The protocol carries it as a signed
     * [short] and reserves the negative half for server-initiated streams
     * (events use -1), leaving 0..32767 for requests.
     */
    private const MAX_STREAM_ID = 32767;

    private Consistency $consistency = Consistency::ONE;

    /**
     * @var array<EventListener> $eventListeners
     */
    private array $eventListeners = [];

    /**
     * Whether the connection got past STARTUP, i.e. whether the node accepts
     * ordinary requests such as the heartbeat.
     */
    private bool $handshakeComplete = false;

    private string $keyspace;

    /**
     * When the node last sent us anything, used to decide whether an idle
     * connection needs a heartbeat.
     */
    private float $lastResponseAt = 0.0;

    /**
     * Next stream id to hand out; the pool runs up to {@see self::MAX_STREAM_ID}
     * and then reuses ids released by answered requests.
     */
    private int $nextStreamId = 0;

    private ?Connection\Node $node = null;

    private Connection\NodeHealth $nodeHealth;

    /**
     * @var array<\Cassandra\Connection\NodeConfig> $nodes
     */
    private array $nodes;

    private Connection\NodeSelector $nodeSelector;

    private ConnectionOptions $options;

    /**
     * @var array<int, float> $orphanedStreams stream ids of statements the
     * client gave up on, mapped to when that happened. They are deliberately
     * kept out of the recycling pool: the server may still answer on them, and
     * handing one to another request would resolve that request with the wrong
     * response. Each is released once its late answer finally arrives.
     */
    private array $orphanedStreams = [];

    /**
     * @var ?Statement $pendingHeartbeat the OPTIONS request sent to prove an
     * idle connection is still alive, while its answer is outstanding
     */
    private ?Statement $pendingHeartbeat = null;

    private float $pendingHeartbeatSentAt = 0.0;

    /**
     * @var array<string, \Cassandra\Response\Result\CachedPreparedResult> $preparedResultCache
     */
    private array $preparedResultCache = [];

    private int $preparedResultCacheSize;
    private int $preparedResultCacheSizeToTrim;

    /**
     * @var SplQueue<int> $recycledStreams
     */
    private SplQueue $recycledStreams;

    private ?float $requestTimeout;

    private ResponseReader $responseReader;

    /**
     * Whether a heartbeat is currently being sent. Sending one can wait for a
     * stream id to come free, which reads responses, which checks the heartbeat
     * again — this keeps that from starting a second probe before the first one
     * has been recorded as pending.
     */
    private bool $sendingHeartbeat = false;

    /**
     * @var array<int, Statement> $statements keyed by the stream id each was sent on
     */
    private array $statements = [];

    private ?ValueEncodeConfig $valueEncodeConfig = null;

    private ProtocolVersion $version;

    /**
     * @var array<WarningsListener> $warningsListeners
     */
    private array $warningsListeners = [];

    /**
     * @param array<\Cassandra\Connection\NodeConfig> $nodes
     */
    public function __construct(
        array $nodes,
        string $keyspace = '',
        ConnectionOptions $options = new ConnectionOptions(),
    ) {

        $this->nodes = $nodes;
        $this->keyspace = $keyspace;
        $this->options = $options;
        $this->version = $options->initialProtocolVersion;
        $this->nodeSelector = $options->nodeSelectionStrategy->createSelector();
        $this->nodeHealth = new Connection\NodeHealth();
        $this->responseReader = new ResponseReader();

        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->recycledStreams = $recycledStreams;

        $this->requestTimeout = $options->requestTimeoutInSeconds;
        $this->preparedResultCacheSize = max(0, $options->preparedResultCacheSize);
        $this->preparedResultCacheSizeToTrim = (int) ceil((float) $this->preparedResultCacheSize * 0.25);
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
        return $this->sendAsyncRequest($request, requestTimeoutInSeconds: $requestTimeoutInSeconds);
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
        $this->valueEncodeConfig = $config;
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
        if ($this->node !== null) {
            return;
        }

        $this->preparedResultCache = [];
        $this->handshakeComplete = false;

        $node = $this->node = $this->selectNodeAndOpenConnection();

        // Anchors the heartbeat interval to a fresh connection rather than to
        // whenever this Connection object last heard from a node.
        $this->lastResponseAt = microtime(true);

        try {
            $this->completeHandshake($node);
        } catch (\Throwable $e) {
            // A handshake that fails part way through leaves a socket the node
            // will not accept ordinary requests on, and only a NodeException
            // closes it on its own: a request timeout deliberately keeps the
            // connection, and a rejected or unexpected response never touches
            // it. Without this the client would go on reporting isConnected(),
            // hand that socket to every later request, and — because the
            // heartbeat is gated on a completed handshake — never probe it
            // either.
            $this->disconnect();

            throw $e;
        }
    }

    public function createBatchRequest(BatchType $type = BatchType::LOGGED, ?Consistency $consistency = null, BatchOptions $options = new BatchOptions()): Request\Batch {

        $consistency = $consistency ?? $this->consistency;

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        return new Request\Batch($type, $consistency, $options);
    }

    public function disconnect(): void {

        $this->preparedResultCache = [];

        // Stream ids are only meaningful on the connection that handed them
        // out, so anything still waiting can never be answered now. Marking
        // them lets a later access fail immediately and accurately, instead of
        // waiting out a request timeout for an answer that cannot come.
        foreach ($this->statements as $statement) {
            $statement->setStatus(StatementStatus::ABANDONED);
        }

        $this->statements = [];
        $this->nextStreamId = 0;
        $this->orphanedStreams = [];
        $this->pendingHeartbeat = null;
        $this->handshakeComplete = false;

        // A bounded read can come back with a header whose body has not arrived
        // yet, and the reader keeps it so the next read resumes the same frame.
        // Those remaining bytes belong to the transport being dropped here, so
        // the half-read frame has to go with it: kept, it would be finished off
        // with the first bytes the next connection sends, leaving every response
        // after it parsed at the wrong offset.
        $this->responseReader->reset();

        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->recycledStreams = $recycledStreams;

        if ($this->node === null) {
            return;
        }

        $node = $this->node;
        $this->node = null;
        $node->close();
    }

    /**
     * Non-blocking: read up to $max available responses, returning how many were processed.
     *
     * NOTE: This method will not block; it processes any currently available responses
     * and returns when the receive buffer is drained or the provided limit is reached.
     *
     * The one thing it can block on is getting a connection at all: called
     * before anything has been sent, it opens one and takes it through the
     * handshake first, as every other method that touches the transport does.
     * The same goes for {@see self::tryReadNextEvent()},
     * {@see self::tryReadNextResponse()}, {@see self::tryResolveStatement()}
     * and {@see self::tryResolveStatements()}.
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
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
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
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $statement = $this->asyncRequest($request, $requestTimeoutInSeconds);

        return $statement;
    }

    public function getNode(): ?Connection\Node {
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
     * Wait for this statement's answer and return it.
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

        $this->assertStatementIsResolvable($statement);

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

    /**
     * @deprecated Use getProtocolVersion() instead.
     */
    public function getVersion(): int {
        return $this->version->value;
    }

    public function isConnected(): bool {
        return $this->node !== null;
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
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
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
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
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
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
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
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        $request = new Request\Query($query, $values, $consistency, $options);

        return $this->asyncRequest($request, $requestTimeoutInSeconds);
    }

    public function registerEventListener(EventListener $eventListener): void {
        $this->eventListeners[] = $eventListener;
    }

    public function registerWarningsListener(WarningsListener $warningsListener): void {
        $this->warningsListeners[] = $warningsListener;
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

        $this->keyspace = $keyspace;

        if (!$this->isConnected()) {
            return;
        }

        if ($this->version->value < ProtocolVersion::V5->value) {
            $response = $this->syncRequest(new Request\Query("USE {$this->keyspace};"));
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
     */
    public function setRequestTimeout(?float $requestTimeoutInSeconds): void {
        $this->requestTimeout = $requestTimeoutInSeconds;
    }

    public function supportsKeyspaceRequestOption(): bool {
        return $this->version->value >= ProtocolVersion::V5->value;
    }

    public function supportsNowInSecondsRequestOption(): bool {
        return $this->version->value >= ProtocolVersion::V5->value;
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
     * multiple of it before giving up. The wait for a free stream id, on a
     * connection that has handed out every one of them, is held to it
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

        $node = $this->getConnectedNode();

        // An explicit argument wins over what the request's options asked for,
        // which in turn wins over the connection default.
        $requestTimeoutInSeconds ??= $request->getRequestTimeout();

        $request->setVersion($this->version);

        if ($request instanceof Request\Prepare) {
            $cachedResult = $this->getCachedPrepareResult($request);
            if ($cachedResult !== null) {
                return $cachedResult;
            }
        }

        $autoPrepareRequest = $this->getAutoPrepareRequestIfNeeded($request);
        if ($autoPrepareRequest !== null) {

            $prepareResponse = $this->syncRequest($autoPrepareRequest, $requestTimeoutInSeconds);
            if (!($prepareResponse instanceof Response\Result\PreparedResult)) {
                throw new ConnectionException('Unexpected response type during prepare', ExceptionCode::CONNECTION_PREPARE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result::class,
                    'received' => get_class($prepareResponse),
                ]);
            }

            $response = $this->handleAutoPrepareResult($prepareResponse, originalRequest: $request, requestTimeoutInSeconds: $requestTimeoutInSeconds);
            if ($response === null) {
                throw new ConnectionException('Unexpected null response during autoPrepare', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result::class,
                    'received' => 'null',
                ]);
            }

            return $response;
        }

        $streamId = $this->getNewStreamId($requestTimeoutInSeconds);
        $request->setStream($streamId);

        $writeSucceeded = false;
        $nodeFailed = false;

        try {
            $node->writeRequest($request);
            $writeSucceeded = true;
        } catch (NodeException $e) {
            $nodeFailed = true;

            $this->handleNodeException($node);

            throw $e;
        } finally {
            if (!$writeSucceeded && !$nodeFailed) {
                // Nothing reached the node — an unencodable request, say — so
                // the stream id was never in use and goes straight back into
                // circulation; leaving it behind would burn one id of the pool
                // per failure for the lifetime of the connection. A node
                // failure needs no such care, because it takes the whole pool
                // with it.
                $this->recycledStreams->enqueue($streamId);
            }
        }

        $responseArrived = false;

        try {
            $response = $this->getNextResponseForStream(
                streamId: $streamId,
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
                requestClass: get_class($request),
            );

            $responseArrived = true;

            $this->recycledStreams->enqueue($streamId);

            $response = $this->handleResponse($request, $response, requestTimeoutInSeconds: $requestTimeoutInSeconds);
            $this->nodeHealth->recordSuccess($node->getConfig());
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
        } finally {
            if (!$responseArrived) {
                // Nothing came back for this id and, unlike an async request,
                // there is no statement carrying it: without this it would be
                // lost for the life of the connection. A timeout has parked it
                // already and a node failure took the whole pool with it, so
                // this is for the rest — a malformed frame, say, which leaves
                // it undecidable whether the answer was consumed.
                $this->parkUnresolvedStream($streamId);
            }
        }

        if ($response === null) {
            throw new ConnectionException('Received unexpected null response from server.', ExceptionCode::CONNECTION_SYNC_NULL_RESPONSE->value, [
                'operation' => 'syncRequest',
                'request_class' => get_class($request),
            ]);
        }

        if ($response instanceof Response\Error) {
            throw $response->getException();
        }

        return $response;
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
        $drainedResponses = false;
        while (true) {
            $event = $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                $this->keepNonBlockingBookkeeping(readAttempted: true);

                return null;
            }
            if ($event instanceof Response\Event) {
                return $event;
            }
        }
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
        $drainedResponses = false;
        while (true) {
            $response = $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                $this->keepNonBlockingBookkeeping(readAttempted: true);

                return null;
            }
            if ($response !== null) {
                return $response;
            }
        }
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
        if ($statement->isResultReady()) {
            return true;
        }

        // Budgets are kept here as well as in the waits: polling never blocks,
        // but a statement that is only ever polled must still run out of time
        // rather than stay pending — and holding its stream id — for good.
        $this->timeOutExpiredStatements();

        // Never resolvable, so reporting "not ready yet" would send a polling
        // caller round a loop that can never end.
        $this->assertStatementIsResolvable($statement);

        $drainedResponses = false;
        do {
            $this->readResponse(drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                break;
            }
            if ($statement->isResultReady()) {
                return true;
            }
        } while (true);

        $this->keepNonBlockingBookkeeping(readAttempted: true);

        return $statement->isResultReady();
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

        // As in tryResolveStatement(): polling does not wait, but it does not
        // excuse a statement from its budget either.
        $this->timeOutExpiredStatements();

        $initialReady = 0;
        foreach ($statements as $s) {
            if ($s->isResultReady()) {
                $initialReady++;

                continue;
            }

            $this->assertStatementIsResolvable($s);
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

        $this->keepNonBlockingBookkeeping($readAttempted);

        $ready = 0;
        foreach ($statements as $s) {
            if ($s->isResultReady()) {
                $ready++;
            }
        }

        return $ready - $initialReady;
    }

    public function unregisterEventListener(EventListener $eventListener): void {
        $this->eventListeners = array_filter($this->eventListeners, fn (EventListener $listener) => $listener !== $eventListener);
    }

    public function unregisterWarningsListener(WarningsListener $warningsListener): void {
        $this->warningsListeners = array_filter($this->warningsListeners, fn (WarningsListener $listener) => $listener !== $warningsListener);
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

        $waitDeadline = $timeoutInSeconds === null ? null : microtime(true) + max(0.0, $timeoutInSeconds);
        $deadlineExceeded = false;

        while ($this->pendingStatements()) {
            // Recomputed per pass: each statement carries its own budget from
            // when it was sent, and resolved ones drop out of the reckoning.
            $deadline = $this->earlierDeadline($waitDeadline, $this->deadlineForStatements($this->statements));

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                $expired = $this->timeOutExpiredStatements();
                if ($expired !== []) {
                    $this->reportTimedOutStatements($expired, 'waitForAllPendingStatements');
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
     * its own budget is given up on and raises a RequestTimeoutException.
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

        $waitDeadline = $timeoutInSeconds === null ? null : microtime(true) + max(0.0, $timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            foreach ($statements as $s) {
                if ($s->isResultReady()) {
                    return $s;
                }

                $this->assertStatementIsResolvable($s);
            }

            $deadline = $this->earlierDeadline($waitDeadline, $this->deadlineForStatements($statements));

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                $expired = $this->intersectStatements($this->timeOutExpiredStatements(), $statements);
                if ($expired !== []) {
                    $this->reportTimedOutStatements($expired, 'waitForAnyStatement');
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
     * unbounded wait does — see {@see self::readResponseUntil()}. Waiting for
     * events with heartbeats disabled therefore means passing a
     * $timeoutInSeconds and calling again, rather than waiting indefinitely.
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
     * RequestTimeoutException. Only that request is affected; the connection
     * stays open.
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

        $waitDeadline = $timeoutInSeconds === null ? null : microtime(true) + max(0.0, $timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            // Requests sent on this connection keep their deadlines while it is
            // being pumped for events, so one going overdue is noticed here too
            // rather than only whenever the caller next waits on it.
            $deadline = $this->earlierDeadline($waitDeadline, $this->deadlineForStatements($this->statements));

            $event = $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Requests that ran out are finished here, but an event listener
                // is not the one who asked about them, so its loop is not
                // interrupted for them; the caller finds out from the statement.
                // This runs before the event is handed back so that an overdue
                // request is still given up on in the pass that brought one.
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
     * nothing to fall back on and the wait is unbounded, exactly as INF is.
     * Pass INF for a wait that only ends when something arrives.
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
     * RequestTimeoutException. Only that request is affected; the connection
     * stays open.
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

        $waitDeadline = $this->deadlineFor($timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            // Bounded by whichever comes first: how long the caller is willing
            // to wait, or the budget of the request that expires soonest.
            $deadline = $this->earlierDeadline($waitDeadline, $this->deadlineForStatements($this->statements));

            $response = $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Requests that ran out are finished here, but the caller asked
                // for the next response, not about any request in particular,
                // so that is not this call's failure to report: they find out
                // from the statement. Nothing came, which is not a failure at
                // all — the connection and its other requests are untouched and
                // the wait can simply be repeated. This runs before the response
                // is handed back so that an overdue request is still given up on
                // in the pass that brought one.
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

        $waitDeadline = $timeoutInSeconds === null ? null : microtime(true) + max(0.0, $timeoutInSeconds);
        $deadlineExceeded = false;

        while (true) {
            $hasUnresolvedStatements = false;
            foreach ($statements as $s) {
                if (!$s->isResultReady()) {
                    $this->assertStatementIsResolvable($s);

                    $hasUnresolvedStatements = true;

                    break;
                }
            }

            if (!$hasUnresolvedStatements) {
                break;
            }

            $deadline = $this->earlierDeadline($waitDeadline, $this->deadlineForStatements($statements));

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                $expired = $this->intersectStatements($this->timeOutExpiredStatements(), $statements);
                if ($expired !== []) {
                    $this->reportTimedOutStatements($expired, 'waitForStatements');
                }

                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    return;
                }
            }
        }
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

    /**
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\StatementException
     */
    private function assertStatementIsResolvable(Statement $statement): void {

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
                'This statement was given up on before the answer arrived — the connection it was sent on was closed, or a follow-up request of its own never reached the node — so it can no longer be resolved. Send the request again.',
                ExceptionCode::STATEMENT_ABANDONED->value,
                [
                    'stream_id' => $statement->getStreamId(),
                    'request_class' => get_class($statement->getRequest()),
                    'reason' => 'connection_closed_or_request_not_sent',
                ]
            );
        }

        // Pending, but not on this connection: a statement from another
        // Connection, or one left over from before this one was replaced.
        // Reading here would never resolve it — no answer for it can arrive on
        // this socket — and, because a wait bounds itself by the deadlines of
        // the statements it was given, an unbounded wait would spin on a
        // deadline that nothing on this connection can ever retire.
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
     * @throws \Cassandra\Exception\ResponseException
     */
    private function cachePrepareResult(Request\Prepare $request, Response\Result\PreparedResult $result): void {

        if ($this->preparedResultCacheSize < 1) {
            return;
        }

        $cachedResult = new Response\Result\CachedPreparedResult(
            new Header(version: $this->version, flags: 0, stream: 0, opcode: Opcode::RESPONSE_RESULT, length: 0),
            new StreamReader(''),
            $result->getPreparedData(),
        );

        $cachedResult->setRequest($request);

        if (count($this->preparedResultCache) >= $this->preparedResultCacheSize) {
            $this->preparedResultCache = array_slice(
                $this->preparedResultCache,
                $this->preparedResultCacheSizeToTrim
            );
        }

        $this->preparedResultCache[$request->getHash()] = $cachedResult;
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     */
    private function chainAsyncRequest(Request\Request $request, Statement $statement): void {

        $streamId = $statement->getStreamId();

        // The statement is not among the pending ones at this point: the
        // response that triggered the follow-up took it out of the map. Nothing
        // else would mark it if the follow-up never goes out, so it is done
        // here — a statement left neither pending nor finished would wait on a
        // stream id that the next connection may well hand to somebody else,
        // and would be resolved by that request's answer. This covers every way
        // of failing, not just the write: reaching a node and claiming the
        // stream id can fail too, and leave the statement just as stranded.
        $requestWasSent = false;

        try {
            // Deliberately not getConnectedNode(): that would open a fresh
            // connection, and this request cannot go on one. The stream id it
            // carries was handed out by the connection that is gone, whose id
            // space a new connection starts over — sending on it would register
            // a statement at an id the new connection is free to hand to
            // somebody else, and the statement was marked as abandoned along
            // with its connection anyway, so it could never be resolved.
            if ($this->node === null) {
                throw new ConnectionException(
                    'The connection this statement was sent on was closed before its follow-up request could be sent, so the request was given up on. Send it again.',
                    ExceptionCode::CONNECTION_CHAINED_REQUEST_CONNECTION_GONE->value,
                    [
                        'operation' => 'chainAsyncRequest',
                        'stream_id' => $streamId,
                        'request_class' => get_class($request),
                    ]
                );
            }

            $node = $this->node;

            $request->setVersion($this->version);
            $request->setStream($streamId);

            if (isset($this->statements[$streamId])) {
                throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                    'operation' => 'chainAsyncRequest',
                    'stream_id' => $streamId,
                ]);
            }

            $writeSucceeded = false;
            $nodeFailed = false;

            try {
                $node->writeRequest($request);
                $writeSucceeded = true;

                $this->nodeHealth->recordSuccess($node->getConfig());
            } catch (NodeException $e) {
                $nodeFailed = true;

                $this->handleNodeException($node);

                throw $e;
            } finally {
                if (!$writeSucceeded && !$nodeFailed) {
                    // Nothing reached the node, so the connection is fine and
                    // the stream id was never in use: it goes back into
                    // circulation instead of being burned. A node failure needs
                    // no such care, because it takes the whole pool with it.
                    $this->recycledStreams->enqueue($streamId);
                }
            }

            $requestWasSent = true;
        } finally {
            if (!$requestWasSent) {
                $statement->setStatus(StatementStatus::ABANDONED);
            }
        }

        $this->statements[$streamId] = $statement;

        $statement->setRequest($request);
        $statement->setResponse(null);

        // A follow-up request (repreparation, auto-prepare) is a new wait, so
        // it gets its own budget rather than inheriting the original one.
        $statement->setSentAt(microtime(true));
    }

    /**
     * Prove that a silent connection is still alive.
     *
     * Nothing else can: a transport read timeout cannot distinguish "the node
     * has nothing to say" from "the node is gone", and neither can a request
     * timeout — a coordinator that is still thinking looks exactly like a
     * connection that died. So once the connection has been silent for the
     * heartbeat interval, an OPTIONS request is sent; if its answer does not
     * arrive within the heartbeat timeout, the connection is treated as dead.
     *
     * This runs while waiting for a response as well as while waiting for
     * events, because the protocol multiplexes stream ids: the heartbeat is
     * answered on its own stream while a slow request is still being computed,
     * so a dead connection is caught in interval + timeout no matter how
     * generous the request timeout is.
     *
     * A read that could outlast the probe's schedule would delay it. None does:
     * {@see self::readResponseUntil()} bounds every read by
     * {@see self::nextHeartbeatActionAt()} as well as by the caller's deadline,
     * which is what lets the transport's stall window be long, or absent.
     *
     * The probe is the driver's own request, not the caller's, so it is held to
     * the heartbeat timeout alone: it is deliberately left out of the request
     * timeout bookkeeping ({@see self::deadlineForStatements()},
     * {@see self::timeOutExpiredStatements()}), which would otherwise report it
     * to a caller who never sent it, and would park a stream id every interval
     * whenever the request timeout is the shorter of the two.
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

        $interval = $this->options->heartbeatIntervalInSeconds;

        // The handshake itself waits for responses, but until it is through
        // the node accepts nothing besides the handshake requests.
        if ($interval === null || !$this->handshakeComplete || $this->sendingHeartbeat) {
            return;
        }

        $now = microtime(true);

        if ($this->pendingHeartbeat !== null) {

            if ($this->pendingHeartbeat->isResultReady()) {
                $this->pendingHeartbeat = null;

                return;
            }

            if ($now - $this->pendingHeartbeatSentAt <= $this->options->heartbeatTimeoutInSeconds) {
                return;
            }

            $node = $this->node;
            $context = [
                'operation' => 'heartbeat',
                'heartbeat_timeout_seconds' => $this->options->heartbeatTimeoutInSeconds,
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

        if ($now - $this->lastResponseAt < $interval) {
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
        if (!$this->hasImmediateStreamId()) {
            return;
        }

        $this->sendingHeartbeat = true;

        try {
            $heartbeat = $this->sendAsyncRequest(new Request\Options());

            // Anchored to when the OPTIONS was actually written, not to when
            // sending it was decided on: getting that far can wait for a stream
            // id to come free, and that is the client's own backlog rather than
            // time the node spent not answering. Charging it to the heartbeat
            // budget would let a busy client declare a healthy node dead the
            // moment the probe goes out.
            $this->pendingHeartbeatSentAt = $heartbeat->getSentAt();
            $this->pendingHeartbeat = $heartbeat;
        } finally {
            $this->sendingHeartbeat = false;
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
    private function completeHandshake(Connection\Node $node): void {

        $response = $this->syncRequest(new Request\Options());
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

        $startupOptions = $this->configureStartupOptions($response, $node);
        $response = $this->syncRequest(new Request\Startup($startupOptions));

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

            if ($this->version->value >= ProtocolVersion::V5->value) {
                $node = $this->node = new FrameCodec($node, $startupOptions['COMPRESSION'] ?? '');
            } elseif (isset($startupOptions['COMPRESSION']) && $startupOptions['COMPRESSION'] !== '') {
                $node = $this->node = new RequestCompressor($node, $startupOptions['COMPRESSION']);
            }

            $authResult = $this->syncRequest(new Request\AuthResponse($nodeConfig->username, $nodeConfig->password));
            if (!($authResult instanceof Response\AuthSuccess)) {
                throw new ConnectionException('Authentication failed.', ExceptionCode::CONNECTION_AUTH_FAILED->value, [
                    'operation' => 'connect/authenticate',
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'username' => $nodeConfig->username,
                ]);
            }
        } elseif ($response instanceof Response\Ready) {
            if ($this->version->value >= ProtocolVersion::V5->value) {
                $node = $this->node = new FrameCodec($node, $startupOptions['COMPRESSION'] ?? '');
            } elseif (isset($startupOptions['COMPRESSION']) && $startupOptions['COMPRESSION'] !== '') {
                $node = $this->node = new RequestCompressor($node, $startupOptions['COMPRESSION']);
            }
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
            $this->syncRequest(new Request\Query("USE {$this->keyspace};"));
        }
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     *
     * @return array<string,string>
     */
    private function configureStartupOptions(Response\Supported $supportedResponse, Connection\Node $node): array {
        $serverOptions = $supportedResponse->getData();

        // configure protocol version
        if (!isset($serverOptions['PROTOCOL_VERSIONS'])) {
            $versionsSupportedByServer = [$this->version];
        } else {
            $versionsSupportedByServer = [];

            foreach ($serverOptions['PROTOCOL_VERSIONS'] as $versionString) {
                $version = ProtocolVersion::fromOptionFormat($versionString);
                if ($version !== null) {
                    $versionsSupportedByServer[] = $version;
                }
            }
        }

        $protocolVersion = ProtocolVersion::getHighestSupportedVersion($versionsSupportedByServer, $this->options->allowedProtocolVersions);
        if ($protocolVersion === null) {

            $versionsSupportedByServerInOptionFormat = array_map(
                fn (ProtocolVersion $v) => $v->inOptionFormat(),
                $versionsSupportedByServer
            );

            $allowedProtocolVersionsInOptionFormat = array_map(
                fn (ProtocolVersion $v) => $v->inOptionFormat(),
                $this->options->allowedProtocolVersions
            );

            throw new ConnectionException('Server does not support a compatible protocol version.', ExceptionCode::CONNECTION_SERVER_PROTOCOL_UNSUPPORTED->value, [
                'protocol_versions_supported_by_server' => $versionsSupportedByServerInOptionFormat,
                'protocol_versions_supported_by_client' => ProtocolVersion::CASES_IN_OPTION_FORMAT,
                'protocol_versions_allowed_by_connection_options' => $allowedProtocolVersionsInOptionFormat,
            ]);
        }

        $this->version = $protocolVersion;

        // configure startup options
        $startupOptions = $this->options->asStartupOptions();

        if (isset($startupOptions['COMPRESSION']) && $startupOptions['COMPRESSION']
            && isset($serverOptions['COMPRESSION']) && $serverOptions['COMPRESSION']
        ) {
            $compressionAlgo = strtolower($startupOptions['COMPRESSION']);

            if (!in_array($compressionAlgo, $serverOptions['COMPRESSION'])) {
                $nodeConfig = $node->getConfig();

                throw new ConnectionException('Compression "' . $compressionAlgo . '" not supported by server.', ExceptionCode::CONNECTION_COMPRESSION_NOT_SUPPORTED->value, [
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'compression' => $compressionAlgo,
                    'server_supported' => $serverOptions['COMPRESSION'],
                ]);
            }

            $startupOptions['COMPRESSION'] = $compressionAlgo;
        } else {
            unset($startupOptions['COMPRESSION']);
        }

        if ($this->version->value >= ProtocolVersion::V4->value) {
            if ($this->options->throwOnOverload) {
                $startupOptions['THROW_ON_OVERLOAD'] = '1';
            } else {
                $startupOptions['THROW_ON_OVERLOAD'] = '0';
            }
        } else {
            unset($startupOptions['THROW_ON_OVERLOAD']);
        }

        if ($this->version->value < ProtocolVersion::V5->value) {
            unset($startupOptions['DRIVER_NAME']);
            unset($startupOptions['DRIVER_VERSION']);
        }

        return $startupOptions;
    }

    /**
     * Absolute microtime at which a wait must give up, or null to wait forever.
     *
     * The budget runs from $sentAt — when the request was handed to the node —
     * rather than from now, so that an async statement gets the same total
     * allowance no matter how long the caller took to start waiting for it.
     * Waits that are not tied to a request (the sync path, which writes
     * immediately before waiting) count from now.
     */
    private function deadlineFor(?float $timeoutInSeconds, ?float $sentAt = null): ?float {
        $timeout = $timeoutInSeconds ?? $this->requestTimeout;

        if ($timeout === null) {
            return null;
        }

        return ($sentAt ?? microtime(true)) + max(0.0, $timeout);
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
     * keep every other one from ever being noticed as overdue.
     *
     * The driver's own heartbeat is skipped as well: it is not one of the
     * caller's requests and is held to
     * {@see ConnectionOptions::$heartbeatTimeoutInSeconds} by
     * {@see self::checkHeartbeat()} instead of to a request budget.
     *
     * @param array<Statement> $statements
     */
    private function deadlineForStatements(array $statements): ?float {

        $earliest = null;

        foreach ($statements as $statement) {
            if ($statement->isResultReady() || $statement === $this->pendingHeartbeat) {
                continue;
            }

            $deadline = $this->deadlineFor(
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
     * The earlier of two deadlines, either of which may be null for "no bound".
     */
    private function earlierDeadline(?float $a, ?float $b): ?float {

        if ($a === null) {
            return $b;
        }

        if ($b === null) {
            return $a;
        }

        return min($a, $b);
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

        $orphanedCount = count($this->orphanedStreams);

        if ($orphanedCount <= max(0, $this->options->maxOrphanedStreams)) {
            return;
        }

        $node = $this->node;
        $context = [
            'operation' => 'enforceOrphanedStreamLimit',
            'orphaned_streams' => $orphanedCount,
            'max_orphaned_streams' => $this->options->maxOrphanedStreams,
            'abandoned_statements' => count($this->statements),
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

    private function getAutoPrepareRequestIfNeeded(Request\Request $request): ?Request\Prepare {

        // auto-prepare query if bind markers are used and not all values are defined with type
        if (
            ($request instanceof Request\Query)
        ) {

            $queryOptions = $request->getOptions();
            $values = $request->getValues();

            if (
                $queryOptions->autoPrepare
                && $values
            ) {
                $hasUnresolvedValues = false;

                foreach ($values as $v) {
                    if (
                        $v !== null
                        && !($v instanceof ValueBase)
                        && !($v instanceof NotSet)
                    ) {
                        $hasUnresolvedValues = true;

                        break;
                    }
                }

                if ($hasUnresolvedValues) {

                    $prepareOptions = new PrepareOptions(keyspace: $queryOptions->keyspace);
                    $prepareRequest = new Request\Prepare($request->getQuery(), $prepareOptions);
                    $prepareRequest->setVersion($this->version);

                    return $prepareRequest;
                }
            }
        }

        return null;
    }

    private function getCachedPrepareResult(Request\Prepare $request): ?Response\Result\CachedPreparedResult {

        return $this->preparedResultCache[$request->getHash()] ?? null;
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
    private function getConnectedNode(): Connection\Node {

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
    private function getNewStreamId(?float $requestTimeoutInSeconds = null): int {

        $waitDeadline = null;
        $waitStarted = false;
        $deadlineExceeded = false;

        while (true) {
            // Both are retried every pass rather than settled once. The pool is
            // the one that fills up while we wait, but the counter is checked
            // beside it because the id space starts over whenever the
            // connection is replaced — and then the counter has every id to
            // give again while the pool it was emptied alongside is still
            // empty. Nothing below returns after replacing the connection
            // today, so this is defensive; getting it wrong the other way would
            // mean sitting out the whole budget and reporting the id space as
            // exhausted with all of it free.
            if ($this->nextStreamId <= self::MAX_STREAM_ID) {
                return $this->nextStreamId++;
            }

            if (!$this->recycledStreams->isEmpty()) {
                return $this->recycledStreams->dequeue();
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
                $waitDeadline = $this->deadlineFor($requestTimeoutInSeconds);
            }

            $deadline = $this->earlierDeadline($waitDeadline, $this->deadlineForStatements($this->statements));

            $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Giving up on a request parks its id rather than releasing it,
                // so this does not by itself make one available; it is done here
                // so that overdue requests are still finished on time.
                $this->timeOutExpiredStatements();

                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    throw new ConnectionException(
                        'Every stream id is in use and none was released in time; the connection has more requests in flight than the protocol allows',
                        ExceptionCode::CONNECTION_STREAM_IDS_EXHAUSTED->value,
                        [
                            'operation' => 'getNewStreamId',
                            'max_stream_id' => self::MAX_STREAM_ID,
                            'statements_in_flight' => count($this->statements),
                            'orphaned_streams' => count($this->orphanedStreams),
                            'request_timeout_seconds' => $requestTimeoutInSeconds ?? $this->requestTimeout,
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
    private function getNextResponseForStream(int $streamId, ?float $requestTimeoutInSeconds = null, ?Statement $statement = null, ?string $requestClass = null): Response\Response {

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
                // matching {@see self::assertStatementIsResolvable()} when it
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
                // assertStatementIsResolvable() raises: the sync path shares
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
            $ownDeadline = $this->deadlineFor($requestTimeoutInSeconds, $statement?->getSentAt() ?? $sentAt);

            // The other requests in flight keep their own budgets while this
            // one waits, so one of them going overdue is noticed here too
            // rather than only whenever its caller next waits on it.
            $deadline = $this->earlierDeadline($ownDeadline, $this->deadlineForStatements($this->statements));

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
                    $mine = $this->intersectStatements($expired, [$statement]);
                    if ($mine !== []) {
                        $this->reportTimedOutStatements($mine, 'getResponseForStatement');
                    }

                    // Not among the expired, so it is still waiting — its budget
                    // has not run out, or a chained follow-up restarted it while
                    // we were reading. Either way the connection's own
                    // bookkeeping is authoritative for a statement, and the
                    // fallback below must not second-guess it; carry on against
                    // the deadline the next pass computes.
                    $statementStillPending = isset($this->statements[$streamId]);
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
    private function handleAutoPrepareResult(Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null): ?Response\Result {

        if (!($result instanceof Response\Result\PreparedResult)) {
            throw new ConnectionException('Unexpected result type while handling auto-prepared statement', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESULT_TYPE->value, [
                'operation' => 'reprepare_result',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($result),
            ]);
        }

        if ($statement !== null) {
            $originalRequest = $statement->getOriginalRequest();
        }

        if (!($originalRequest instanceof Request\Query)) {
            throw new ConnectionException('Original request is not an query request', ExceptionCode::CONNECTION_AUTO_PREPARE_ORIGINAL_NOT_QUERY->value, [
                'operation' => 'auto_prepare_execute',
                'request_class' => $originalRequest ? get_class($originalRequest) : null,
                'expected' => Request\Query::class,
            ]);
        }

        $newExecuteRequest = new Request\Execute(
            $result,
            $originalRequest->getValues(),
            $originalRequest->getConsistency(),
            ExecuteOptions::fromQueryOptions($originalRequest->getOptions())
        );

        if ($statement !== null) {
            $this->chainAsyncRequest($newExecuteRequest, $statement);

            return null;
        }

        $response = $this->syncRequest($newExecuteRequest, $requestTimeoutInSeconds);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during re-execute after auto-preparation', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE_REEXECUTE->value, [
                'operation' => 'auto_prepare_execute',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    private function handleNodeException(Connection\Node $node): void {
        $this->nodeHealth->recordFailure($node->getConfig());
        $this->disconnect();
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
    private function handleReprepareResult(Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null): ?Response\Result {

        if (!($result instanceof Response\Result\PreparedResult)) {
            throw new ConnectionException('Unexpected result type while handling reprepared statement', ExceptionCode::CONNECTION_REPREPARE_UNEXPECTED_RESULT_TYPE->value, [
                'operation' => 'reprepare_result',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($result),
            ]);
        }

        if ($statement !== null) {
            $originalRequest = $statement->getOriginalRequest();
        }

        if (!($originalRequest instanceof Request\Execute)) {
            throw new ConnectionException('Original request is not an execute request', ExceptionCode::CONNECTION_REPREPARE_ORIGINAL_NOT_EXECUTE->value, [
                'operation' => 'reprepare_execute',
                'request_class' => $originalRequest ? get_class($originalRequest) : null,
                'expected' => Request\Execute::class,
            ]);
        }

        $newExecuteRequest = new Request\Execute(
            $result,
            $originalRequest->getValues(),
            $originalRequest->getConsistency(),
            $originalRequest->getOptions()
        );

        if ($statement !== null) {
            $this->chainAsyncRequest($newExecuteRequest, $statement);

            return null;
        }

        $response = $this->syncRequest($newExecuteRequest, $requestTimeoutInSeconds);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during re-execute after repreparation', ExceptionCode::CONNECTION_REPREPARE_UNEXPECTED_RESPONSE_REEXECUTE->value, [
                'operation' => 'reprepare_execute',
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
    private function handleResponse(Request\Request $request, Response\Response $response, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null): ?Response\Response {

        if ($response->hasWarnings()) {
            foreach ($this->warningsListeners as $listener) {
                $listener->onWarnings($response->getWarnings(), $request, $response);
            }
        }

        return match (true) {
            $response instanceof Response\Error => $this->handleResponseError($request, $response, $statement, $requestTimeoutInSeconds),
            $response instanceof Response\Result => $this->handleResponseResult($request, $response, $statement, $requestTimeoutInSeconds),
            default => $response,
        };
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    private function handleResponseError(Request\Request $request, Response\Error $response, ?Statement $statement, ?float $requestTimeoutInSeconds = null): ?Response\Response {

        // re-prepare query if it is unprepared
        if (
            ($request instanceof Request\Execute)
            && ($response instanceof Response\Error\UnpreparedError)
        ) {

            $prevResult = $request->getPreviousResult();
            if (!($prevResult instanceof Response\Result\PreparedResult)) {
                throw new ConnectionException('Unexpected previous result type for UNPREPARED error', ExceptionCode::CONNECTION_UNPREPARED_UNEXPECTED_PREV_RESULT_TYPE->value, [
                    'operation' => 'unprepared_error_handling',
                    'expected' => Response\Result\PreparedResult::class,
                    'received' => get_class($prevResult),
                ]);
            }

            $prevRequest = $prevResult->getRequest();
            if ($prevRequest === null) {
                throw new ConnectionException('Previous prepared result has no associated request', ExceptionCode::CONNECTION_UNPREPARED_PREV_NO_REQUEST->value, [
                    'operation' => 'unprepared_error_handling',
                ]);
            }
            if (!($prevRequest instanceof Request\Prepare)) {
                throw new ConnectionException('Previous result is not a prepare request', ExceptionCode::CONNECTION_UNPREPARED_PREV_NOT_PREPARE_REQUEST->value, [
                    'operation' => 'unprepared_error_handling',
                    'request_class' => get_class($prevRequest),
                    'expected' => Request\Prepare::class,
                ]);
            }

            $newPrepareRequest = new Request\Prepare($prevRequest->getQuery(), $prevRequest->getOptions());

            $this->invalidateCachedPrepareResult($newPrepareRequest);

            if ($statement !== null) {
                $statement->setStatus(StatementStatus::REPREPARING);

                $this->chainAsyncRequest($newPrepareRequest, $statement);

                return null;
            }

            $prepareResponse = $this->syncRequest($newPrepareRequest, $requestTimeoutInSeconds);
            if (!($prepareResponse instanceof Response\Result)) {
                throw new ConnectionException('Unexpected response type during repreparation', ExceptionCode::CONNECTION_REPREPARATION_UNEXPECTED_RESPONSE->value, [
                    'operation' => 'unprepared_error_handling',
                    'expected' => Response\Result::class,
                    'received' => get_class($prepareResponse),
                ]);
            }

            $response = $this->handleReprepareResult($prepareResponse, originalRequest: $request, requestTimeoutInSeconds: $requestTimeoutInSeconds);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    private function handleResponseExecuteResult(Request\Execute $request, Response\Result $result): Response\Result {

        $result->setPreviousResult($request->getPreviousResult());

        return $result;
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
    private function handleResponsePrepareResult(Request\Prepare $request, Response\Result $result, ?Statement $statement, ?float $requestTimeoutInSeconds = null): ?Response\Result {

        $result->setRequest($request);

        if (
            ($result instanceof Response\Result\PreparedResult)
            && !($result instanceof Response\Result\CachedPreparedResult)
        ) {
            $this->cachePrepareResult($request, $result);
        }

        if ($statement !== null) {
            if ($statement->isRepreparing()) {
                $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
                $result = $this->handleReprepareResult($result, statement: $statement, requestTimeoutInSeconds: $requestTimeoutInSeconds);
            } elseif ($statement->isAutoPreparing()) {
                $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
                $result = $this->handleAutoPrepareResult($result, statement: $statement, requestTimeoutInSeconds: $requestTimeoutInSeconds);
            }
        }

        return $result;
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
    private function handleResponseResult(Request\Request $request, Response\Result $result, ?Statement $statement, ?float $requestTimeoutInSeconds = null): ?Response\Result {

        return match (true) {
            $request instanceof Request\Prepare => $this->handleResponsePrepareResult($request, $result, $statement, $requestTimeoutInSeconds),
            $request instanceof Request\Execute => $this->handleResponseExecuteResult($request, $result),
            default => $result,
        };
    }

    /**
     * Whether a stream id can be had without waiting for one to come free.
     *
     * The heartbeat asks before sending: {@see self::getNewStreamId()} reads
     * while it waits, and reading is exactly what the probe must not do to get
     * itself sent.
     */
    private function hasImmediateStreamId(): bool {

        return $this->nextStreamId <= self::MAX_STREAM_ID || !$this->recycledStreams->isEmpty();
    }

    /**
     * The statements of $expired that the caller was actually waiting on.
     *
     * @param array<Statement> $expired
     * @param array<Statement> $waitedOn
     *
     * @return array<Statement>
     */
    private function intersectStatements(array $expired, array $waitedOn): array {

        return array_values(array_filter(
            $expired,
            static fn (Statement $statement): bool => in_array($statement, $waitedOn, true),
        ));
    }

    private function invalidateCachedPrepareResult(Request\Prepare $request): void {

        unset($this->preparedResultCache[$request->getHash()]);
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
     * probe to decide; request budgets are kept either way.
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
    private function keepNonBlockingBookkeeping(bool $readAttempted): void {

        $this->timeOutExpiredStatements();

        if ($readAttempted) {
            $this->checkHeartbeat();
        }
    }

    /**
     * When {@see self::checkHeartbeat()} next has something to do, or null when
     * heartbeats are off or cannot run yet.
     *
     * Reads are bounded by this as well as by the caller's deadline, so that a
     * wait which is otherwise unbounded — for events, say — still returns in
     * time to send the probe or to declare an unanswered one dead. It mirrors
     * the conditions checkHeartbeat() applies rather than second-guessing them:
     * waking early only costs a read that finds nothing, but waking late would
     * delay the probe by however long the read blocked.
     */
    private function nextHeartbeatActionAt(): ?float {

        $interval = $this->options->heartbeatIntervalInSeconds;

        if ($interval === null || !$this->handshakeComplete || $this->sendingHeartbeat) {
            return null;
        }

        if ($this->pendingHeartbeat !== null) {
            return $this->pendingHeartbeatSentAt + $this->options->heartbeatTimeoutInSeconds;
        }

        // checkHeartbeat() will not send a probe it would have to wait for a
        // stream id to send, so there is nothing here to come up for air for.
        // Reporting one anyway would put every read's bound in the past — the
        // probe is due, after all — and turn each wait into a spin over reads
        // that return at once and a probe that is never sent.
        if (!$this->hasImmediateStreamId()) {
            return null;
        }

        return $this->lastResponseAt + $interval;
    }

    private function onEvent(Response\Event $event): void {

        foreach ($this->eventListeners as $listener) {
            $listener->onEvent($event);
        }
    }

    /**
     * Park a stream id whose fate a failure left undecided.
     *
     * Reached when a request ends in something that is neither a node failure —
     * which takes the whole pool with it — nor a timeout, which parks the id
     * itself: a frame the reader could not make sense of, above all. Whether
     * the answer was consumed is exactly what such a failure leaves unknown, so
     * the id is parked rather than recycled, for the reason
     * {@see self::timeOutExpiredStatements()} gives, and released if and when
     * an answer for it does turn up.
     *
     * Not enforcing the orphan limit here is deliberate: this runs while
     * another failure is on its way out, and the connection being replaced is
     * not what the caller should be told about. The next piece of bookkeeping
     * enforces it.
     */
    private function parkUnresolvedStream(int $streamId): void {

        if ($this->node === null || isset($this->orphanedStreams[$streamId])) {
            return;
        }

        unset($this->statements[$streamId]);
        $this->orphanedStreams[$streamId] = microtime(true);
    }

    /**
     * The requests the caller has in flight, i.e. everything waiting for an
     * answer except the driver's own heartbeat, which nobody asked for and
     * which {@see self::checkHeartbeat()} looks after on its own.
     *
     * @return array<int, Statement>
     */
    private function pendingStatements(): array {

        if ($this->pendingHeartbeat === null) {
            return $this->statements;
        }

        return array_filter(
            $this->statements,
            fn (Statement $statement): bool => $statement !== $this->pendingHeartbeat,
        );
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
    private function processResponse(Response\Response $response, Connection\Node $node): ?Response\Response {

        $orphanedStreamId = $response->getStream();
        if (isset($this->orphanedStreams[$orphanedStreamId])) {
            // The late answer to a statement that was already given up on. It
            // has nowhere to go, but its arrival proves the stream id is free
            // again, so it goes back into circulation here.
            unset($this->orphanedStreams[$orphanedStreamId]);
            $this->recycledStreams->enqueue($orphanedStreamId);

            $this->lastResponseAt = microtime(true);
            $this->nodeHealth->recordSuccess($node->getConfig());

            return null;
        }

        if ($this->valueEncodeConfig !== null && ($response instanceof Result\RowsResult)) {
            $response->configureValueEncoding($this->valueEncodeConfig);
        }

        $streamId = $response->getStream();
        if (isset($this->statements[$streamId])) {
            $statement = $this->statements[$streamId];
            unset($this->statements[$streamId]);
            $response = $this->handleResponse($statement->getRequest(), $response, $statement);
            if ($response !== null) {
                $statement->setResponse($response);
                $this->recycledStreams->enqueue($streamId);
            }
        }

        if ($response instanceof Response\Event) {
            $this->onEvent($response);
        }

        $this->lastResponseAt = microtime(true);
        $this->nodeHealth->recordSuccess($node->getConfig());

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
            $response = $this->responseReader->readResponse($node, $this->version, Connection\Node::DO_NOT_WAIT);
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
     * The read is bounded by {@see self::nextHeartbeatActionAt()} as well,
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
        $readDeadline = $this->earlierDeadline($deadline, $this->nextHeartbeatActionAt());

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
     * Report statements the caller was waiting on that have been given up on.
     *
     * Only the connection's bookkeeping has happened by now
     * ({@see self::timeOutExpiredStatements()}); this turns it into the failure
     * the caller sees, naming every statement of theirs that ran out rather
     * than just the first.
     *
     * @param array<Statement> $expired
     *
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    private function reportTimedOutStatements(array $expired, string $operation): never {

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
                'orphaned_streams' => count($this->orphanedStreams),
            ],
            timedOutStatements: $expired,
        );
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function selectNodeAndOpenConnection(): Connection\IoNode {

        $ordered = $this->nodeSelector->order($this->nodes);
        $parts = $this->nodeHealth->partitionByAvailability($ordered);
        $candidates = array_merge($parts['available'], $parts['unavailable']);

        $socketException = null;

        foreach ($candidates as $config) {

            $className = $config->getNodeClass();

            try {
                $node = new $className($config);
                $node->connect();
            } catch (NodeException $e) {
                $socketException = $e;
                $this->nodeHealth->recordFailure($config);

                continue;
            }

            $this->nodeHealth->recordSuccess($config);

            return $node;
        }

        $nodeConfigs = array_map(fn($config) => [
            'host' => $config->host,
            'port' => $config->port,
            'class' => $config->getNodeClass(),
        ], $this->nodes);

        throw new ConnectionException(
            'Unable to connect to any Cassandra node',
            ExceptionCode::CONNECTION_UNABLE_TO_CONNECT_ANY_NODE->value,
            [
                'attempted_nodes' => $nodeConfigs,
                'node_count' => count($this->nodes),
            ],
            $socketException ?? null
        );
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
    private function sendAsyncRequest(Request\Request $request, ?float $requestTimeoutInSeconds = null): Statement {

        $node = $this->getConnectedNode();

        $request->setVersion($this->version);

        // Same precedence the Statement applies below: an explicit argument
        // wins over what the request's options asked for, and only then does
        // the connection default apply.
        $streamId = $this->getNewStreamId($requestTimeoutInSeconds ?? $request->getRequestTimeout());
        $request->setStream($streamId);

        $originalRequest = $request;
        $autoPrepareRequest = $this->getAutoPrepareRequestIfNeeded($request);
        if ($autoPrepareRequest !== null) {
            $autoPrepareRequest->setStream($streamId);

            $request = $autoPrepareRequest;
        }

        // Looked up for the request that is about to go out rather than for the
        // one the caller handed in, so that an auto-prepared query is spared
        // the PREPARE just as an explicit prepareAsync() is. The sync path gets
        // this for free by recursing into syncRequest() for its PREPARE.
        if ($request instanceof Request\Prepare) {
            $cachedResult = $this->getCachedPrepareResult($request);
            if ($cachedResult !== null) {
                $statement = new Statement(
                    connection: $this,
                    streamId: $streamId,
                    request: $request,
                    originalRequest: $originalRequest,
                    requestTimeoutInSeconds: $requestTimeoutInSeconds,
                );

                if ($autoPrepareRequest !== null) {
                    // Nothing has been written yet: what this statement is
                    // really waiting for is the EXECUTE that handling the
                    // cached result chains onto it below.
                    $statement->setStatus(StatementStatus::AUTO_PREPARING);
                }

                $response = $this->handleResponse($statement->getRequest(), $cachedResult, $statement);
                if ($response !== null) {
                    $statement->setResponse($response);
                    $this->recycledStreams->enqueue($streamId);
                }

                return $statement;
            }
        }

        if (isset($this->statements[$streamId])) {
            throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                'operation' => 'sendAsyncRequest',
                'stream_id' => $streamId,
            ]);
        }

        $writeSucceeded = false;
        $nodeFailed = false;

        try {
            $node->writeRequest($request);
            $writeSucceeded = true;

            $this->nodeHealth->recordSuccess($node->getConfig());
        } catch (NodeException $e) {
            $nodeFailed = true;

            $this->handleNodeException($node);

            throw $e;
        } finally {
            if (!$writeSucceeded && !$nodeFailed) {
                // Nothing reached the node — an unencodable request, say — so
                // the stream id was never in use and goes straight back into
                // circulation; leaving it behind would burn one id of the pool
                // per failure for the lifetime of the connection. A node
                // failure needs no such care, because it takes the whole pool
                // with it.
                $this->recycledStreams->enqueue($streamId);
            }
        }

        $statement = new Statement(
            connection: $this,
            streamId: $streamId,
            request: $request,
            originalRequest: $originalRequest,
            requestTimeoutInSeconds: $requestTimeoutInSeconds,
        );

        $this->statements[$streamId] = $statement;

        if ($autoPrepareRequest !== null) {
            $statement->setStatus(StatementStatus::AUTO_PREPARING);
        } else {
            $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
        }

        return $statement;
    }

    /**
     * Give up on every request whose budget has run out, and report them.
     *
     * This is bookkeeping, not a failure of whatever call happens to be waiting:
     * the statements are marked and their stream ids parked here, but it is left
     * to the caller to decide whether any of this is its business. A wait that
     * was asked about one of these statements raises it; a wait for an event or
     * for the next response simply carries on, and the caller learns about it
     * from the statement itself.
     *
     * All expired statements are handled in one pass, so a set of requests that
     * runs out together is finished together rather than one wait at a time.
     *
     * @return array<Statement>
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function timeOutExpiredStatements(): array {

        $now = microtime(true);
        $expired = [];

        foreach ($this->statements as $streamId => $statement) {
            // The heartbeat is the driver's own request, not the caller's, and
            // it answers a different question, so it is held to the heartbeat
            // timeout by checkHeartbeat() rather than to a request budget. Were
            // it timed out here it would be reported to a caller who never sent
            // it, and would tie up an orphaned stream slot every interval.
            if ($statement->isResultReady() || $statement === $this->pendingHeartbeat) {
                continue;
            }

            $deadline = $this->deadlineFor($statement->getRequestTimeout(), $statement->getSentAt());
            if ($deadline === null || $now < $deadline) {
                continue;
            }

            $statement->setStatus(StatementStatus::TIMED_OUT);
            unset($this->statements[$streamId]);
            $this->orphanedStreams[$streamId] = $now;

            $expired[] = $statement;
        }

        $this->enforceOrphanedStreamLimit();

        return $expired;
    }

    /**
     * Give up on whatever was waiting on a stream id, keeping the connection.
     *
     * See {@see self::timeOutExpiredStatements()} for why parking the id rather
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

        unset($this->statements[$streamId]);
        $this->orphanedStreams[$streamId] = microtime(true);

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
                'request_timeout_seconds' => $requestTimeoutInSeconds ?? $this->requestTimeout,
                'orphaned_streams' => count($this->orphanedStreams),
            ],
            timedOutStatements: $statement === null ? [] : [$statement],
        );
    }
}
