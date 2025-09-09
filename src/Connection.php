<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Connection\FrameCodec;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\Flag;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
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
use TypeError;
use ValueError;

final class Connection {
    protected Consistency $consistency = Consistency::ONE;

    /**
     * @var array<EventListener> $eventListeners
     */
    protected array $eventListeners = [];

    protected string $keyspace;

    protected int $lastStreamId = 0;

    protected Lz4Decompressor $lz4Decompressor;

    protected ?Connection\Node $node = null;

    protected Connection\NodeHealth $nodeHealth;

    /**
     * @var array<\Cassandra\Connection\NodeConfig> $nodes
     */
    protected array $nodes;

    protected Connection\NodeSelector $nodeSelector;

    /**
     * @var array<string,string> $options
     */
    protected array $options;

    /**
     * @var array<string, \Cassandra\Response\Result\CachedPreparedResult> $preparedResultCache
     */
    protected array $preparedResultCache = [];

    protected int $preparedResultCacheSize;
    protected int $preparedResultCacheSizeToTrim;

    /**
     * @var SplQueue<int> $recycledStreams
     */
    protected SplQueue $recycledStreams;

    /**
     * @var array<Statement> $statements
     */
    protected array $statements = [];

    protected ?ValueEncodeConfig $valueEncodeConfig = null;

    protected int $version = 0x03;
    protected int $versionIn = 0x83;

    /**
     * @var array<WarningsListener> $warningsListeners
     */
    protected array $warningsListeners = [];

    /**
     * @param array<\Cassandra\Connection\NodeConfig> $nodes
     */
    public function __construct(array $nodes, string $keyspace = '', ConnectionOptions $options = new ConnectionOptions()) {

        $this->nodes = $nodes;
        $this->keyspace = $keyspace;
        $this->options = $options->toArray();
        $this->nodeSelector = $options->nodeSelectionStrategy->createSelector();
        $this->nodeHealth = new Connection\NodeHealth();
        $this->lz4Decompressor = new Lz4Decompressor();

        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->recycledStreams = $recycledStreams;

        $this->preparedResultCacheSize = max(0, $options->preparedResultCacheSize);
        $this->preparedResultCacheSizeToTrim = (int) ceil((float) $this->preparedResultCacheSize * 0.25);
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
     */
    public function asyncRequest(Request\Request $request): Statement {
        return $this->sendAsyncRequest($request);
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

        $node = $this->node = $this->selectNodeAndOpenConnection();

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

        $this->configureOptions($response);

        $response = $this->syncRequest(new Request\Startup($this->options));

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

            if ($this->version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new ConnectionException('Invalid node implementation: expected NodeImplementation', ExceptionCode::CONNECTION_AUTH_INVALID_NODE_IMPLEMENTATION->value, [
                        'operation' => 'connect/authenticate',
                        'node_class' => get_class($node),
                        'expected_interface' => Connection\NodeImplementation::class,
                    ]);
                }
                $node = $this->node = new FrameCodec($node, $this->options['COMPRESSION'] ?? '');
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
            if ($this->version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new ConnectionException('Invalid node implementation: expected NodeImplementation', ExceptionCode::CONNECTION_READY_INVALID_NODE_IMPLEMENTATION->value, [
                        'operation' => 'connect/ready',
                        'node_class' => get_class($node),
                        'expected_interface' => Connection\NodeImplementation::class,
                    ]);
                }
                $node = $this->node = new FrameCodec($node, $this->options['COMPRESSION'] ?? '');
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

        if ($this->keyspace && $this->version < 5) {
            $this->syncRequest(new Request\Query("USE {$this->keyspace};"));
        }
    }

    public function createBatchRequest(BatchType $type = BatchType::LOGGED, ?Consistency $consistency = null, BatchOptions $options = new BatchOptions()): Request\Batch {

        $consistency = $consistency ?? $this->consistency;

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version >= 5
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        return new Request\Batch($type, $consistency, $options);
    }

    public function disconnect(): void {

        $this->preparedResultCache = [];

        if ($this->node === null) {
            return;
        }

        $node = $this->node;
        $this->node = null;
        $node->close();
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
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
            && $this->version >= 5
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
            && $this->version >= 5
        ) {
            $options = $options->withKeyspace($this->keyspace);
        }

        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $statement = $this->asyncRequest($request);

        return $statement;
    }

    /**
     * Wait until all statements received response.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function flush(): void {
        while ($this->statements) {
            $this->readResponse();
        }
    }

    public function getNode(): ?Connection\Node {
        return $this->node;
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
     */
    public function getResponseForStatement(Statement $statement): Response\Response {

        if ($statement->isResultReady()) {
            return $statement->getResponse();
        }

        return $this->getNextResponseForStream($statement->getStreamId());
    }

    public function getVersion(): int {
        return $this->version;
    }

    public function isConnected(): bool {
        return $this->node !== null;
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
     */
    public function prepare(string $query, PrepareOptions $options = new PrepareOptions()): Response\Result\PreparedResult {

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version >= 5
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function prepareAsync(string $query, PrepareOptions $options = new PrepareOptions()): Statement {

        if (
            $options->keyspace === null
            && $this->keyspace
            && $this->version >= 5
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
            && $this->version >= 5
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
            && $this->version >= 5
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

        if ($this->version < 5) {
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

    public function supportsKeyspaceRequestOption(): bool {
        return $this->version >= 5;
    }

    public function supportsNowInSecondsRequestOption(): bool {
        return $this->version >= 5;
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
     */
    public function syncRequest(Request\Request $request): Response\Response {

        $node = $this->getConnectedNode();

        $request->setVersion($this->version);

        if ($request instanceof Request\Prepare) {
            $cachedResult = $this->getCachedPrepareResult($request);
            if ($cachedResult !== null) {
                return $cachedResult;
            }
        }

        $autoPrepareRequest = $this->getAutoPrepareRequestIfNeeded($request);
        if ($autoPrepareRequest !== null) {

            $prepareResponse = $this->syncRequest($autoPrepareRequest);
            if (!($prepareResponse instanceof Response\Result\PreparedResult)) {
                throw new ConnectionException('Unexpected response type during prepare', ExceptionCode::CONNECTION_PREPARE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result::class,
                    'received' => get_class($prepareResponse),
                ]);
            }

            $response = $this->handleAutoPrepareResult($autoPrepareRequest, $prepareResponse, originalRequest: $request);
            if ($response === null) {
                throw new ConnectionException('Unexpected null response during autoPrepare', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result::class,
                    'received' => 'null',
                ]);
            }

            return $response;
        }

        try {
            $node->writeRequest($request);
            $response = $this->getNextResponseForStream(streamId: 0);
            $response = $this->handleResponse($request, $response);
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

    public function unregisterEventListener(EventListener $eventListener): void {
        $this->eventListeners = array_filter($this->eventListeners, fn (EventListener $listener) => $listener !== $eventListener);
    }

    public function unregisterWarningsListener(WarningsListener $warningsListener): void {
        $this->warningsListeners = array_filter($this->warningsListeners, fn (WarningsListener $listener) => $listener !== $warningsListener);
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
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function cachePrepareResult(Request\Prepare $request, Response\Result\PreparedResult $result): void {

        if ($this->preparedResultCacheSize < 1) {
            return;
        }

        $cachedResult = new Response\Result\CachedPreparedResult(
            new Header(version: 5, flags: 0, stream: 0, opcode: Opcode::RESPONSE_RESULT, length: 0),
            new StreamReader(''),
            $result->getPreparedData(),
        );

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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    protected function chainAsyncRequest(Request\Request $request, Statement $statement): void {

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
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function configureOptions(Response\Supported $supportedReponse): void {
        $serverOptions = $supportedReponse->getData();

        if (!isset($serverOptions['PROTOCOL_VERSIONS'])) {
            $this->version = 3;
        } elseif (in_array('5/v5', $serverOptions['PROTOCOL_VERSIONS'])) {
            $this->version = 5;
        } elseif (in_array('4/v4', $serverOptions['PROTOCOL_VERSIONS'])) {
            $this->version = 4;
        } elseif (in_array('3/v3', $serverOptions['PROTOCOL_VERSIONS'])) {
            $this->version = 3;
        } else {
            throw new ConnectionException('Server does not support a compatible protocol version.', ExceptionCode::CONNECTION_SERVER_PROTOCOL_UNSUPPORTED->value, [
                'server_versions' => $serverOptions['PROTOCOL_VERSIONS'] ?? null,
                'client_supported' => ReleaseConstants::PHP_CASSANDRA_SUPPORTED_PROTOCOL_VERSIONS,
            ]);
        }

        if (isset($this->options['COMPRESSION']) && $this->options['COMPRESSION']
            && isset($serverOptions['COMPRESSION']) && $serverOptions['COMPRESSION']
        ) {
            $compressionAlgo = strtolower($this->options['COMPRESSION']);

            if (!in_array($compressionAlgo, $serverOptions['COMPRESSION'])) {
                $nodeConfig = $this->node?->getConfig();

                throw new ConnectionException('Compression "' . $compressionAlgo . '" not supported by server.', ExceptionCode::CONNECTION_COMPRESSION_NOT_SUPPORTED->value, [
                    'host' => $nodeConfig->host ?? null,
                    'port' => $nodeConfig->port ?? null,
                    'compression' => $compressionAlgo,
                    'server_supported' => $serverOptions['COMPRESSION'],
                ]);
            }

            $this->options['COMPRESSION'] = $compressionAlgo;
        } else {
            unset($this->options['COMPRESSION']);
        }

        if ($this->version >= 4) {
            if (isset($this->options['THROW_ON_OVERLOAD']) && $this->options['THROW_ON_OVERLOAD']) {
                $this->options['THROW_ON_OVERLOAD'] = '1';
            } else {
                $this->options['THROW_ON_OVERLOAD'] = '0';
            }
        } else {
            unset($this->options['THROW_ON_OVERLOAD']);
        }

        if ($this->version < 5) {
            unset($this->options['DRIVER_NAME']);
            unset($this->options['DRIVER_VERSION']);
        }

        $this->versionIn = $this->version + 0x80;
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function createResponse(Header $header, string $body): Response\Response {

        $responseClassMap = Response\Response::getResponseClassMap();
        if (!isset($responseClassMap[$header->opcode->value])) {
            throw new ConnectionException('Unknown response type: ' . $header->opcode->value, ExceptionCode::CONNECTION_UNKNOWN_RESPONSE_TYPE->value, [
                'expected' => array_keys($responseClassMap),
                'received' => $header->opcode->value,
            ]);
        }

        $streamReader = new Response\StreamReader($body);
        $resetStream = true;

        $responseClass = $responseClassMap[$header->opcode->value];

        switch ($responseClass) {
            case Response\Result::class:
                $result = new Response\Result($header, $streamReader);
                $resultKind = $result->getKind();

                $resultClassMap = Response\Result::getResultClassMap();
                if (isset($resultClassMap[$resultKind->value])) {
                    $responseClass = $resultClassMap[$resultKind->value];
                } else {
                    throw new ConnectionException('Unknown result kind: ' . $resultKind->value, ExceptionCode::CONNECTION_UNKNOWN_RESULT_KIND->value, [
                        'expected' => array_keys($resultClassMap),
                        'received' => $resultKind->value,
                    ]);
                }

                break;

            case Response\Event::class:
                $result = new Response\Event($header, $streamReader);
                $eventType = $result->getType();

                $eventClassMap = Response\Event::getEventClassMap();
                if (isset($eventClassMap[$eventType->value])) {
                    $responseClass = $eventClassMap[$eventType->value];
                } else {
                    throw new ConnectionException('Unknown event type: ' . $eventType->value, ExceptionCode::CONNECTION_UNKNOWN_EVENT_TYPE->value, [
                        'expected' => array_keys($eventClassMap),
                        'received' => $eventType->value,
                    ]);
                }

                break;

            case Response\Error::class:
                $result = new Response\Error($header, $streamReader);
                $errorCode = $result->getCode();

                $errorClassMap = Response\Error::getErrorClassMap();
                if (isset($errorClassMap[$errorCode])) {
                    $responseClass = $errorClassMap[$errorCode];
                } else {
                    throw new ConnectionException('Unknown error code: ' . $errorCode, ExceptionCode::CONNECTION_UNKNOWN_ERROR_CODE->value, [
                        'expected' => array_keys($errorClassMap),
                        'received' => $errorCode,
                    ]);
                }

                break;

            default:
                $resetStream = false;

                break;
        }

        if ($resetStream) {
            $streamReader->extraDataOffset(0);
            $streamReader->offset(0);
        }

        $response = new $responseClass($header, $streamReader);

        if ($this->valueEncodeConfig !== null && ($response instanceof Response\Result\RowsResult)) {
            $response->configureValueEncoding($this->valueEncodeConfig);
        }

        return $response;
    }

    protected function getAutoPrepareRequestIfNeeded(Request\Request $request): ?Request\Prepare {

        // auto-prepare query if bind markers are used not all values defined with type
        if (
            ($request instanceof Request\Query)
        ) {

            $queryOptions = $request->getOptions();
            $values = $request->getValues();

            if (
                $queryOptions->autoPrepare
                && $values
                && array_find($values, fn($v) => (
                    $v !== null
                    && !($v instanceof ValueBase)
                    && !($v instanceof NotSet)
                )) !== null
            ) {

                $prepareOptions = new PrepareOptions(keyspace: $queryOptions->keyspace);
                $prepareRequest = new Request\Prepare($request->getQuery(), $prepareOptions);

                return $prepareRequest;
            }
        }

        return null;
    }

    protected function getCachedPrepareResult(Request\Prepare $request): ?Response\Result\CachedPreparedResult {

        return $this->preparedResultCache[$request->getHash()] ?? null;
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
     */
    protected function getConnectedNode(): Connection\Node {

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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    protected function getNewStreamId(): int {
        if ($this->lastStreamId < 32767) {
            return ++$this->lastStreamId;
        }

        while ($this->recycledStreams->isEmpty()) {
            $this->readResponse();
        }

        return $this->recycledStreams->dequeue();
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
     */
    protected function getNextResponseForStream(int $streamId = 0): Response\Response {
        do {
            $response = $this->readResponse();
        } while ($response === null || $response->getStream() !== $streamId);

        return $response;
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
     */
    protected function handleAutoPrepareResult(Request\Prepare $request, Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null): ?Response\Result {

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

        $response = $this->syncRequest($newExecuteRequest);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during re-execute after auto-preparation', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE_REEXECUTE->value, [
                'operation' => 'auto_prepare_execute',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    protected function handleNodeException(Connection\Node $node): void {
        $this->nodeHealth->recordFailure($node->getConfig());
        $this->disconnect();
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
     */
    protected function handleReprepareResult(Request\Prepare $request, Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null): ?Response\Result {

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

        $response = $this->syncRequest($newExecuteRequest);
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    protected function handleResponse(Request\Request $request, Response\Response $response, ?Statement $statement = null): ?Response\Response {

        if ($response->hasWarnings()) {
            foreach ($this->warningsListeners as $listener) {
                $listener->onWarnings($response->getWarnings(), $request, $response);
            }
        }

        return match (true) {
            $response instanceof Response\Error => $this->handleResponseError($request, $response, $statement),
            $response instanceof Response\Result => $this->handleResponseResult($request, $response, $statement),
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
     */
    protected function handleResponseError(Request\Request $request, Response\Error $response, ?Statement $statement): ?Response\Response {

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

            if ($statement !== null) {
                $statement->setStatus(StatementStatus::REPREPARING);

                $cachedResult = $this->getCachedPrepareResult($newPrepareRequest);
                if ($cachedResult !== null) {
                    $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);

                    return $this->handleReprepareResult($newPrepareRequest, $cachedResult, statement: $statement);

                } else {
                    $this->chainAsyncRequest($newPrepareRequest, $statement);

                    return null;
                }
            }

            $prepareResponse = $this->syncRequest($newPrepareRequest);
            if (!($prepareResponse instanceof Response\Result)) {
                throw new ConnectionException('Unexpected response type during repreparation', ExceptionCode::CONNECTION_REPREPARATION_UNEXPECTED_RESPONSE->value, [
                    'operation' => 'unprepared_error_handling',
                    'expected' => Response\Result::class,
                    'received' => get_class($prepareResponse),
                ]);
            }

            $response = $this->handleReprepareResult($newPrepareRequest, $prepareResponse, originalRequest: $request);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function handleResponseExecuteResult(Request\Execute $request, Response\Result $result, ?Statement $statement): Response\Result {

        $result->setPreviousResult($request->getPreviousResult());

        return $result;
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
     */
    protected function handleResponsePrepareResult(Request\Prepare $request, Response\Result $result, ?Statement $statement): ?Response\Result {

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
                $result = $this->handleReprepareResult($request, $result, statement: $statement);
            } elseif ($statement->isAutoPreparing()) {
                $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
                $result = $this->handleAutoPrepareResult($request, $result, statement: $statement);
            }
        }

        return $result;
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
     */
    protected function handleResponseResult(Request\Request $request, Response\Result $result, ?Statement $statement): ?Response\Result {

        return match (true) {
            $request instanceof Request\Prepare => $this->handleResponsePrepareResult($request, $result, $statement),
            $request instanceof Request\Execute => $this->handleResponseExecuteResult($request, $result, $statement),
            default => $result,
        };
    }

    protected function onEvent(Response\Event $event): void {

        foreach ($this->eventListeners as $listener) {
            $listener->onEvent($event);
        }
    }

    /**
     * @param array<string> $warnings
     */
    protected function onWarnings(array $warnings, Request\Request $request, Response\Response $response, ): void {

        foreach ($this->warningsListeners as $listener) {
            $listener->onWarnings($warnings, $request, $response);
        }
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
     */
    protected function readResponse(): ?Response\Response {
        $node = $this->getConnectedNode();

        try {
            $version = ord($node->read(1));
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
        }

        if ($version !== $this->versionIn) {
            throw new ConnectionException('Unsupported or mismatched CQL binary protocol version received from server.', ExceptionCode::CONNECTION_PROTOCOL_VERSION_MISMATCH->value, [
                'received_version' => $version,
                'expected_version' => $this->versionIn,
                'supported_versions' => ReleaseConstants::PHP_CASSANDRA_SUPPORTED_PROTOCOL_VERSIONS,
            ]);
        }

        try {
            /**
             * @var false|array{
             *  flags: int,
             *  stream: int,
             *  opcode: int,
             *  length: int
             * } $headerData
             */
            $headerData = unpack('Cflags/nstream/Copcode/Nlength', $node->read(8));
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
        }

        if ($headerData === false) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('Cannot read response header', ExceptionCode::CONNECTION_CANNOT_READ_RESPONSE_HEADER->value, [
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
                'protocol_version' => $this->version,
            ]);
        }

        $headerVersion = $version - 0x80;

        try {
            $header = new Header(
                version: $headerVersion,
                flags: $headerData['flags'],
                stream: $headerData['stream'],
                opcode: Opcode::from($headerData['opcode']),
                length: $headerData['length'],
            );
        } catch (ValueError|TypeError $e) {
            throw new ConnectionException('Invalid opcode type: ' . $headerData['opcode'], ExceptionCode::CONNECTION_INVALID_OPCODE_TYPE->value, [
                'opcode' => $headerData['opcode'],
            ], $e);
        }

        try {
            $body = $header->length === 0 ? '' : $node->read($header->length);
        } catch (NodeException $e) {
            $this->handleNodeException($node);

            throw $e;
        }

        if ($this->version < 5 && $header->length > 0 && $header->flags & Flag::COMPRESSION) {
            $this->lz4Decompressor->setInput($body);
            $body = $this->lz4Decompressor->decompressBlock();
        }

        $response = $this->createResponse($header, $body);

        $streamId = $header->stream;
        if ($streamId !== 0 && isset($this->statements[$streamId])) {
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

        $this->nodeHealth->recordSuccess($node->getConfig());

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     */
    protected function selectNodeAndOpenConnection(): Connection\Node {

        $ordered = $this->nodeSelector->order($this->nodes);
        $parts = $this->nodeHealth->partitionByAvailability($ordered);
        $candidates = array_merge($parts['available'], $parts['unavailable']);

        $socketException = null;

        foreach ($candidates as $config) {

            $className = $config->getNodeClass();

            try {
                $node = new $className($config);
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    protected function sendAsyncRequest(Request\Request $request, ?int $streamId = null): Statement {

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
        );

        $this->statements[$streamId] = $statement;

        if ($autoPrepareRequest !== null) {
            $statement->setStatus(StatementStatus::AUTO_PREPARING);
        } else {
            $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
        }

        return $statement;
    }
}
