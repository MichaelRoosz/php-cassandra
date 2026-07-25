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
        $drainedResponses = false;
        while ($count < $max) {
            $this->readResponse(waitForResponse: false,  drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                break;
            }
            $count++;
        }

        return $count;
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

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

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
        while ($pagingState !== null) {
            $response = $this->execute(
                previousResult: $previousResult,
                values: $values,
                consistency: $consistency,
                options: $options->withPagingState($pagingState)
            )->asRowsResult();

            $responses[] = $response;

            $pagingState = $response->getRowsMetadata()->pagingState;
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

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $statement = $this->asyncRequest($request);

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

        return $this->getNextResponseForStream(
            streamId: $statement->getStreamId(),
            requestTimeoutInSeconds: $statement->getRequestTimeout(),
            statement: $statement,
        );
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

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        $response = $this->syncRequest(new Request\Prepare($query, $options));
        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new ConnectionException('Unexpected response type during prepare', ExceptionCode::CONNECTION_PREPARE_UNEXPECTED_RESPONSE->value, [
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
    public function prepareAsync(string $query, PrepareOptions $options = new PrepareOptions()): Statement {

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

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

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

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
        while ($pagingState !== null) {
            $response = $this->query(
                query: $query,
                values: $values,
                consistency: $consistency,
                options: $options->withPagingState(
                    $pagingState
                )
            )->asRowsResult();

            $responses[] = $response;

            $pagingState = $response->getRowsMetadata()->pagingState;
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

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version->value >= ProtocolVersion::V5->value
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        $request = new Request\Query($query, $values, $consistency, $options);

        return $this->asyncRequest($request);
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
     * Applies to every subsequent blocking call that has no explicit timeout of
     * its own. Raise it around operations Cassandra allows more time for, such
     * as TRUNCATE (60s server-side by default), or pass the timeout directly to
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
     * multiple of it before giving up.
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

            $response = $this->handleAutoPrepareResult($autoPrepareRequest, $prepareResponse, originalRequest: $request, requestTimeoutInSeconds: $requestTimeoutInSeconds);
            if ($response === null) {
                throw new ConnectionException('Unexpected null response during autoPrepare', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result::class,
                    'received' => 'null',
                ]);
            }

            return $response;
        }

        $streamId = $this->getNewStreamId();
        $request->setStream($streamId);

        try {
            $node->writeRequest($request);
            $response = $this->getNextResponseForStream(
                streamId: $streamId,
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
                requestClass: get_class($request),
            );

            $this->recycledStreams->enqueue($streamId);

            $response = $this->handleResponse($request, $response, requestTimeoutInSeconds: $requestTimeoutInSeconds);
            $this->nodeHealth->recordSuccess($node->getConfig());
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
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
            $event = $this->readResponse(waitForResponse: false, drainedResponses: $drainedResponses);
            if ($drainedResponses) {
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
            $response = $this->readResponse(waitForResponse: false, drainedResponses: $drainedResponses);
            if ($drainedResponses) {
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

        // Never resolvable, so reporting "not ready yet" would send a polling
        // caller round a loop that can never end.
        $this->assertStatementIsResolvable($statement);

        $drainedResponses = false;
        do {
            $this->readResponse(waitForResponse: false, drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                break;
            }
            if ($statement->isResultReady()) {
                return true;
            }
        } while (true);

        return false;
    }

    /**
     * Non-blocking: try to resolve from a set of statements, up to $max responses processed.
     * Returns the number of newly resolved statements from the provided set.
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
        $initialReady = 0;
        foreach ($statements as $s) {
            if ($s->isResultReady()) {
                $initialReady++;

                continue;
            }

            $this->assertStatementIsResolvable($s);
        }

        $processed = 0;
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

            $this->readResponse(waitForResponse: false, drainedResponses: $drainedResponses);
            if ($drainedResponses) {
                break;
            }
            $processed++;
        }

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
     *   0     do not wait: return as soon as there is nothing more to read
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * It returns once every statement has been answered or the time is up. A
     * statement that runs out of its own budget is given up on and raises a
     * RequestTimeoutException instead.
     *
     * Even 0 costs one read on the transport, so it can block for as long as
     * the transport's receive timeout; for a look that never blocks at all, use
     * {@see self::tryResolveStatements()}.
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

                return;
            }
        }
    }

    /**
     * Wait until any of the given statements becomes ready and return it.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  let the statements' own budgets bound it (the default)
     *   0     do not wait: return as soon as there is nothing more to read
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * Returns null when the time is up with none of them ready; the statements
     * are untouched and can still be waited on. A statement that runs out of
     * its own budget is given up on and raises a RequestTimeoutException.
     *
     * Even 0 costs one read on the transport, so it can block for as long as
     * the transport's receive timeout; for a look that never blocks at all, use
     * {@see self::tryResolveStatements()}.
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
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  wait for as long as it takes (the default), since an event can
     *         arrive at any time
     *   0     do not wait: return as soon as there is nothing more to read
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * Returns null when the time is up without an event, leaving the connection
     * usable. Even 0 costs one read on the transport; for a look that never
     * blocks at all, use {@see self::tryReadNextEvent()}.
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
            if ($event instanceof Response\Event) {
                return $event;
            }

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // As in waitForNextResponse(): requests that ran out are finished
                // here, but an event listener is not the one who asked about
                // them, so its loop is not interrupted for them.
                $this->timeOutExpiredStatements();

                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    return null;
                }
            }
        }
    }

    /**
     * Wait for the next response of any kind, the counterpart of
     * {@see self::waitForNextEvent()}.
     *
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  use the connection's request timeout (the default)
     *   0     do not wait: return as soon as there is nothing more to read
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * Returns null when the time is up with nothing having arrived and nothing
     * overdue. Even 0 costs one read on the transport; for a look that never
     * blocks at all, use {@see self::tryReadNextResponse()}.
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
            if ($response !== null) {
                return $response;
            }

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                // Requests that ran out are finished here, but the caller asked
                // for the next response, not about any request in particular,
                // so that is not this call's failure to report: they find out
                // from the statement. Nothing came, which is not a failure at
                // all — the connection and its other requests are untouched and
                // the wait can simply be repeated.
                $this->timeOutExpiredStatements();

                if ($waitDeadline !== null && microtime(true) >= $waitDeadline) {
                    return null;
                }
            }
        }
    }

    /**
     * Wait until the given async statements have been answered.
     *
     * @param array<Statement> $statements
     * @param ?float $timeoutInSeconds how long this call may block:
     *   null  let the statements' own budgets bound it (the default)
     *   0     do not wait: return as soon as there is nothing more to read
     *   n     wait at most n seconds
     *   INF   wait for as long as it takes
     *
     * It returns once they have all been answered or the time is up, so check
     * isResultReady() when passing a timeout. A statement that runs out of its
     * own budget is given up on and raises a RequestTimeoutException instead.
     *
     * Even 0 costs one read on the transport, so it can block for as long as
     * the transport's receive timeout; for a look that never blocks at all, use
     * {@see self::tryResolveStatements()}.
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
                'The connection this statement was sent on was closed before the answer arrived, so it can no longer be resolved. Send the request again.',
                ExceptionCode::STATEMENT_ABANDONED->value,
                [
                    'stream_id' => $statement->getStreamId(),
                    'request_class' => get_class($statement->getRequest()),
                    'reason' => 'connection_closed',
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
    private function chainAsyncRequest(Request\Request $request, Statement $statement): void {

        $node = $this->getConnectedNode();

        $request->setVersion($this->version);

        $streamId = $statement->getStreamId();
        $request->setStream($streamId);

        if (isset($this->statements[$streamId])) {
            throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                'operation' => 'sendAsyncRequest',
                'stream_id' => $streamId,
            ]);
        }

        try {
            $node->writeRequest($request);
            $this->nodeHealth->recordSuccess($node->getConfig());
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
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
     * The probe is the driver's own request, not the caller's, so it is held to
     * the heartbeat timeout alone: it is deliberately left out of the request
     * timeout bookkeeping ({@see self::deadlineForStatements()},
     * {@see self::timeOutExpiredStatements()}), which would otherwise report it
     * to a caller who never sent it, and would park a stream id every interval
     * whenever the request timeout is the shorter of the two.
     *
     * Callers must read before calling this, so that an answer already sitting
     * in the socket buffer is never mistaken for an unanswered heartbeat.
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
        if ($interval === null || !$this->handshakeComplete) {
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

        $this->pendingHeartbeat = $this->sendAsyncRequest(new Request\Options());
        $this->pendingHeartbeatSentAt = $now;
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
     * up its budget. Null when nothing is pending, or when there is no timeout.
     *
     * Each statement is held to the timeout its own request asked for. The
     * driver's own heartbeat is skipped: it is not one of the caller's requests
     * and is held to {@see ConnectionOptions::$heartbeatTimeoutInSeconds} by
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
                return null;
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
    private function getNewStreamId(): int {
        if ($this->nextStreamId <= self::MAX_STREAM_ID) {
            return $this->nextStreamId++;
        }

        // Every id has been handed out, so one can only come back with an
        // answer. This is an ordinary bounded wait like any other: a transport
        // read timeout here only means the connection was quiet, so it must not
        // tear the connection down, and the heartbeat is what proves the node
        // is still there while we wait.
        $waitDeadline = $this->deadlineFor(null);
        $deadlineExceeded = false;

        while ($this->recycledStreams->isEmpty()) {
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
                            'request_timeout_seconds' => $this->requestTimeout,
                        ]
                    );
                }
            }
        }

        return $this->recycledStreams->dequeue();
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

        do {
            // The deadline is recomputed every pass rather than taken once: a
            // chained follow-up request (repreparation, auto-prepare) restarts
            // the statement's budget, and a deadline captured before the loop
            // would hold that new request to the budget of the one it replaced.
            $deadline = $this->deadlineFor($requestTimeoutInSeconds, $statement?->getSentAt() ?? $sentAt);

            $response = $this->readResponseUntil($deadline, $deadlineExceeded);

            $this->checkHeartbeat();

            if ($deadlineExceeded) {
                $budgetWasRestarted = false;

                if ($statement !== null) {
                    $expired = $this->intersectStatements($this->timeOutExpiredStatements(), [$statement]);
                    if ($expired !== []) {
                        $this->reportTimedOutStatements($expired, 'getResponseForStatement');
                    }

                    // Still waiting and not overdue after all, so its budget was
                    // restarted while we were reading: carry on against the
                    // deadline the next pass computes.
                    $budgetWasRestarted = isset($this->statements[$streamId]);
                }

                if (!$budgetWasRestarted) {
                    $this->timeOutStream(
                        $streamId,
                        $statement === null ? 'syncRequest' : 'getResponseForStatement',
                        $requestTimeoutInSeconds,
                        $requestClass,
                        $statement,
                    );
                }
            }
        } while ($response === null || $response->getStream() !== $streamId);

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
    private function handleAutoPrepareResult(Request\Prepare $request, Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null): ?Response\Result {

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
    private function handleReprepareResult(Request\Prepare $request, Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null): ?Response\Result {

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

            $response = $this->handleReprepareResult($newPrepareRequest, $prepareResponse, originalRequest: $request, requestTimeoutInSeconds: $requestTimeoutInSeconds);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    private function handleResponseExecuteResult(Request\Execute $request, Response\Result $result, ?Statement $statement): Response\Result {

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
                $result = $this->handleReprepareResult($request, $result, statement: $statement, requestTimeoutInSeconds: $requestTimeoutInSeconds);
            } elseif ($statement->isAutoPreparing()) {
                $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
                $result = $this->handleAutoPrepareResult($request, $result, statement: $statement, requestTimeoutInSeconds: $requestTimeoutInSeconds);
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
            $request instanceof Request\Execute => $this->handleResponseExecuteResult($request, $result, $statement),
            default => $result,
        };
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

    private function onEvent(Response\Event $event): void {

        foreach ($this->eventListeners as $listener) {
            $listener->onEvent($event);
        }
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
    private function readResponse(bool $waitForResponse, bool &$drainedResponses = false): ?Response\Response {
        $node = $this->getConnectedNode();

        try {
            $response = $this->responseReader->readResponse($node, $this->version, $waitForResponse);
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
        }

        if ($response === null) {
            $drainedResponses = true;

            return null;
        }

        $drainedResponses = false;

        return $this->processResponse($response, $node);
    }

    /**
     * Blocking read bounded by $deadline rather than by the transport timeout.
     *
     * A transport read timeout only means that the connection was silent for
     * its stall window. The response reader keeps any partially consumed frame,
     * so resuming the read is always safe — which makes the transport timeout
     * the wrong place to decide that a slow server, or an event stream with
     * nothing to report, has waited long enough. That decision belongs to
     * $deadline, an absolute microtime; null waits indefinitely.
     *
     * Returns null when no complete response was available, with
     * $deadlineExceeded telling the caller whether the wait may continue.
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

        try {
            $response = $this->responseReader->readResponse($node, $this->version, waitForResponse: true);
        } catch (NodeException $e) {
            if (!$e->isReadTimeout()) {
                $this->handleNodeException($node);

                throw $e;
            }

            $response = null;
        }

        if ($response === null) {
            if ($deadline !== null && microtime(true) >= $deadline) {
                $deadlineExceeded = true;
            }

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
    private function sendAsyncRequest(Request\Request $request, ?int $streamId = null, ?float $requestTimeoutInSeconds = null): Statement {

        $node = $this->getConnectedNode();

        $request->setVersion($this->version);

        $streamId = $streamId ?? $this->getNewStreamId();
        $request->setStream($streamId);

        if ($request instanceof Request\Prepare) {
            $cachedResult = $this->getCachedPrepareResult($request);
            if ($cachedResult !== null) {
                $statement = new Statement(
                    connection: $this,
                    streamId: $streamId,
                    request: $request,
                    requestTimeoutInSeconds: $requestTimeoutInSeconds,
                );

                $response = $this->handleResponse($statement->getRequest(), $cachedResult, $statement);
                if ($response !== null) {
                    $statement->setResponse($response);
                    $this->recycledStreams->enqueue($streamId);
                }

                return $statement;
            }
        }

        $originalRequest = $request;
        $autoPrepareRequest = $this->getAutoPrepareRequestIfNeeded($request);
        if ($autoPrepareRequest !== null) {
            $autoPrepareRequest->setStream($streamId);

            $request = $autoPrepareRequest;
        }

        if (isset($this->statements[$streamId])) {
            throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                'operation' => 'sendAsyncRequest',
                'stream_id' => $streamId,
            ]);
        }

        try {
            $node->writeRequest($request);
            $this->nodeHealth->recordSuccess($node->getConfig());
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
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
