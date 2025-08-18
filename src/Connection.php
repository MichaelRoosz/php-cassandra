<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Connection\FrameCodec;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\Flag;
use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Protocol\Header;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Response\Result;
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

    /**
     * @var array<\Cassandra\Connection\NodeConfig> $nodes
     */
    protected array $nodes;

    /**
     * Connection options
     * @var array<string,string> $options
     */
    protected array $options = [
        'CQL_VERSION' => '3.0.0',
        'DRIVER_NAME' => 'php-cassandra-client',
        'DRIVER_VERSION' => '0.9.0',
        // 'COMPRESSION' => 'lz4',
        // 'THROW_ON_OVERLOAD' => '1',
    ];

    /**
     * @var SplQueue<int> $recycledStreams
     */
    protected SplQueue $recycledStreams;

    /**
     * @var array<Statement> $statements
     */
    protected array $statements = [];

    protected int $version = 0x03;
    protected int $versionIn = 0x83;

    /**
     * @param array<\Cassandra\Connection\NodeConfig> $nodes
     * @param string $keyspace
     * @param array<string,string> $options
     */
    public function __construct(array $nodes, string $keyspace = '', array $options = []) {

        $this->nodes = $nodes;
        $this->keyspace = $keyspace;

        $this->lz4Decompressor = new Lz4Decompressor();

        foreach ($options as $optname => $optvalue) {
            $this->options[strtoupper($optname)] = $optvalue;
        }

        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->recycledStreams = $recycledStreams;
    }

    public function addEventListener(EventListener $eventListener): void {
        $this->eventListeners[] = $eventListener;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function asyncRequest(Request\Request $request): Statement {
        return $this->sendAsyncRequest($request);
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function batchAsync(Request\Batch $batchRequest): Statement {
        return $this->asyncRequest($batchRequest);
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function batchSync(Request\Batch $batchRequest): Response\Result {
        $response = $this->syncRequest($batchRequest);

        if (!($response instanceof Response\Result)) {
            throw new Exception('Unexpected response type during batchSync', ExceptionCode::CON_UNEXPECTED_RESPONSE_BATCH_SYNC->value, [
                'operation' => 'batchSync',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function connect(): bool {
        if ($this->node !== null) {
            return true;
        }

        $this->selectNode();

        if ($this->node === null) {
            throw new Exception('Client is not connected to any node. Call connect() before issuing requests.', ExceptionCode::CON_NOT_CONNECTED->value, [
                'operation' => 'connect',
            ]);
        }

        $node = $this->node;

        $response = $this->syncRequest(new Request\Options());
        if (!($response instanceof Response\Supported)) {
            $nodeConfig = $this->node->getConfig();

            throw new Exception('OPTIONS handshake failed: unexpected response type', ExceptionCode::CON_OPTIONS_UNEXPECTED_RESPONSE->value, [
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
                throw new Exception('Username and password must not be empty.', ExceptionCode::CON_AUTH_MISSING_CREDENTIALS->value, [
                    'operation' => 'connect/authenticate',
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'auth_required' => true,
                ]);
            }

            if ($this->version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new Exception('Invalid node implementation: expected NodeImplementation', ExceptionCode::CON_AUTH_INVALID_NODE_IMPLEMENTATION->value, [
                        'operation' => 'connect/authenticate',
                        'node_class' => get_class($node),
                        'expected_interface' => Connection\NodeImplementation::class,
                    ]);
                }
                $this->node = new FrameCodec($node, $this->options['COMPRESSION'] ?? '');
            }

            $authResult = $this->syncRequest(new Request\AuthResponse($nodeConfig->username, $nodeConfig->password));
            if (!($authResult instanceof Response\AuthSuccess)) {
                throw new Exception('Authentication failed.', ExceptionCode::CON_AUTH_FAILED->value, [
                    'operation' => 'connect/authenticate',
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'username' => $nodeConfig->username,
                ]);
            }
        } elseif ($response instanceof Response\Ready) {
            if ($this->version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new Exception('Invalid node implementation: expected NodeImplementation', ExceptionCode::CON_READY_INVALID_NODE_IMPLEMENTATION->value, [
                        'operation' => 'connect/ready',
                        'node_class' => get_class($node),
                        'expected_interface' => Connection\NodeImplementation::class,
                    ]);
                }
                $this->node = new FrameCodec($node, $this->options['COMPRESSION'] ?? '');
            }
        } else {
            $nodeConfig = $node->getConfig();

            throw new Exception('Connection startup failed: unexpected response type', ExceptionCode::CON_STARTUP_UNEXPECTED_RESPONSE->value, [
                'operation' => 'connect/startup',
                'expected' => [Response\Authenticate::class, Response\Ready::class],
                'received' => get_class($response),
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
            ]);
        }

        if ($this->keyspace) {
            $this->syncRequest(new Request\Query("USE {$this->keyspace};"));
        }

        return true;
    }

    public function disconnect(): void {
        if ($this->node !== null) {
            $this->node->close();
            $this->node = null;
        }
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception
     */
    public function executeAsync(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions()): Statement {
        $consistency = $consistency ?? $this->consistency;
        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $statement = $this->asyncRequest($request);

        return $statement;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception
     */
    public function executeSync(Result $previousResult, array $values = [], ?Consistency $consistency = null, ExecuteOptions $options = new ExecuteOptions()): Response\Result {
        $consistency = $consistency ?? $this->consistency;
        $request = new Request\Execute($previousResult, $values, $consistency, $options);

        $response = $this->syncRequest($request);
        if (!($response instanceof Response\Result)) {
            throw new Exception('Unexpected response type during executeSync', ExceptionCode::CON_EXECUTE_UNEXPECTED_RESPONSE->value, [
                'operation' => 'executeSync',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * Wait until all statements received response.
     *
     * @throws \Cassandra\Exception
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
     * @throws \Cassandra\Exception
     */
    public function getResponseForStatement(Statement $statement): Response\Response {

        return $this->getNextResponseForStream($statement->getStreamId());
    }

    public function getVersion(): int {
        return $this->version;
    }

    public function isConnected(): bool {
        return $this->node !== null;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function prepareAsync(string $query, PrepareOptions $options = new PrepareOptions()): Statement {
        $request = new Request\Prepare($query, $options);

        return $this->asyncRequest($request);
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function prepareSync(string $query, PrepareOptions $options = new PrepareOptions()): Response\Result\PreparedResult {
        $response = $this->syncRequest(new Request\Prepare($query, $options));
        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new Exception('Unexpected response type during prepareSync', ExceptionCode::CON_PREPARE_UNEXPECTED_RESPONSE->value, [
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception
     */
    public function queryAsync(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions()): Statement {
        $consistency = $consistency ?? $this->consistency;
        $request = new Request\Query($query, $values, $consistency, $options);

        return $this->asyncRequest($request);
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception
     */
    public function querySync(string $query, array $values = [], ?Consistency $consistency = null, QueryOptions $options = new QueryOptions()): Response\Result {
        $consistency = $consistency ?? $this->consistency;
        $request = new Request\Query($query, $values, $consistency, $options);

        $response = $this->syncRequest($request);

        if (!($response instanceof Response\Result)) {
            throw new Exception('Unexpected response type during querySync', ExceptionCode::CON_QUERY_UNEXPECTED_RESPONSE->value, [
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    public function setConsistency(Consistency $consistency): void {
        $this->consistency = $consistency;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function setKeyspace(string $keyspace): ?Response\Result {
        $this->keyspace = $keyspace;

        if ($this->node === null) {
            return null;
        }

        $response = $this->syncRequest(new Request\Query("USE {$this->keyspace};"));
        if (!($response instanceof Response\Result)) {
            throw new Exception('Unexpected response type during setKeyspace', ExceptionCode::CON_SET_KEYSPACE_UNEXPECTED_RESPONSE->value, [
                'expected' => Response\Result::class,
                'received' => get_class($response),
                'operation' => 'setKeyspace',
                'keyspace' => $this->keyspace,
            ]);
        }

        return $response;
    }

    public function supportsKeyspaceRequestOption(): bool {
        return $this->version >= 5;
    }

    public function supportsNowInSecondsRequestOption(): bool {
        return $this->version >= 5;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function syncRequest(Request\Request $request): Response\Response {
        if ($this->node === null) {
            $this->connect();
        }

        if ($this->node === null) {
            throw new Exception('Client is not connected to any node. Call connect() before issuing requests.', ExceptionCode::CON_NOT_CONNECTED->value, [
                'operation' => 'syncRequest',
            ]);
        }

        $request->setVersion($this->version);
        $this->node->writeRequest($request);

        $response = $this->getNextResponseForStream(streamId: 0);
        $response = $this->handleResponse($request, $response);

        if ($response === null) {
            throw new Exception('Received unexpected null response from server.', ExceptionCode::CON_SYNC_NULL_RESPONSE->value, [
                'operation' => 'syncRequest',
                'request_class' => get_class($request),
            ]);
        }

        if ($response instanceof Response\Error) {
            throw $response->getException();
        }

        return $response;
    }

    public function trigger(Response\Event $event): void {
    }

    /**
     * @throws \Cassandra\Exception
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
            throw new Exception('Server does not support a compatible protocol version.', ExceptionCode::CON_SERVER_PROTOCOL_UNSUPPORTED->value, [
                'server_versions' => $serverOptions['PROTOCOL_VERSIONS'] ?? null,
                'client_supported' => ['3/v3', '4/v4', '5/v5'],
            ]);
        }

        if (isset($this->options['COMPRESSION']) && $this->options['COMPRESSION']
            && isset($serverOptions['COMPRESSION']) && $serverOptions['COMPRESSION']
        ) {
            $compressionAlgo = strtolower($this->options['COMPRESSION']);

            if (!in_array($compressionAlgo, $serverOptions['COMPRESSION'])) {
                $nodeConfig = $this->node?->getConfig();

                throw new Exception('Compression "' . $compressionAlgo . '" not supported by server.', ExceptionCode::CON_COMPRESSION_NOT_SUPPORTED->value, [
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
     * @throws \Cassandra\Exception
     */
    protected function createResponse(Header $header, string $body): Response\Response {

        $responseClassMap = Response\Response::getResponseClassMap();
        if (!isset($responseClassMap[$header->opcode->value])) {
            throw new Exception('Unknown response type: ' . $header->opcode->value, ExceptionCode::CON_UNKNOWN_RESPONSE_TYPE->value, [
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
                    throw new Exception('Unknown result kind: ' . $resultKind->value, ExceptionCode::CON_UNKNOWN_RESULT_KIND->value, [
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
                    throw new Exception('Unknown event type: ' . $eventType->value, ExceptionCode::CON_UNKNOWN_EVENT_TYPE->value, [
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
                    throw new Exception('Unknown error code: ' . $errorCode, ExceptionCode::CON_UNKNOWN_ERROR_CODE->value, [
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

        return new $responseClass($header, $streamReader);
    }

    /**
     * @throws \Cassandra\Exception
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
     * @throws \Cassandra\Exception
     */
    protected function getNextResponseForStream(int $streamId = 0): Response\Response {
        do {
            $response = $this->readResponse();
        } while ($response !== null && $response->getStream() !== $streamId);

        if ($response === null) {
            throw new Exception('Received unexpected null response from server.', ExceptionCode::CON_GET_NEXT_NULL_RESPONSE->value, [
                'operation' => 'getNextResponseForStream',
                'stream_id' => $streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function handleReprepareResult(Request\Prepare $request, Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null): ?Response\Result {

        if (!($result instanceof Response\Result\PreparedResult)) {
            throw new Exception('Unexpected result type while handling reprepared statement', ExceptionCode::CON_REPREPARE_UNEXPECTED_RESULT_TYPE->value, [
                'operation' => 'reprepare_result',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($result),
            ]);
        }

        if ($statement !== null) {
            $originalRequest = $statement->getOriginalRequest();
        }

        if (!($originalRequest instanceof Request\Execute)) {
            throw new Exception('Original request is not an execute request', ExceptionCode::CON_REPREPARE_ORIGINAL_NOT_EXECUTE->value, [
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
            $this->sendAsyncRequest($newExecuteRequest, $result->getStream());

            return null;
        }

        $response = $this->syncRequest($newExecuteRequest);
        if (!($response instanceof Response\Result)) {
            throw new Exception('Unexpected response type during re-execute after repreparation', ExceptionCode::CON_REPREPARE_UNEXPECTED_RESPONSE_REEXECUTE->value, [
                'operation' => 'reprepare_execute',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function handleResponse(Request\Request $request, Response\Response $response, ?Statement $statement = null): ?Response\Response {

        return match (true) {
            $response instanceof Response\Error => $this->handleResponseError($request, $response, $statement),
            $response instanceof Response\Result => $this->handleResponseResult($request, $response, $statement),
            default => $response,
        };
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function handleResponseError(Request\Request $request, Response\Error $response, ?Statement $statement): ?Response\Response {

        // re-prepare query if it is unprepared
        if (
            ($request instanceof Request\Execute)
            && ($response instanceof Response\Error\UnpreparedError)
        ) {

            $prevResult = $request->getPreviousResult();
            if (!($prevResult instanceof Response\Result\PreparedResult)) {
                throw new Exception('Unexpected previous result type for UNPREPARED error', ExceptionCode::CON_UNPREPARED_UNEXPECTED_PREV_RESULT_TYPE->value, [
                    'operation' => 'unprepared_error_handling',
                    'expected' => Response\Result\PreparedResult::class,
                    'received' => get_class($prevResult),
                ]);
            }

            $prevRequest = $prevResult->getRequest();
            if ($prevRequest === null) {
                throw new Exception('Previous prepared result has no associated request', ExceptionCode::CON_UNPREPARED_PREV_NO_REQUEST->value, [
                    'operation' => 'unprepared_error_handling',
                ]);
            }
            if (!($prevRequest instanceof Request\Prepare)) {
                throw new Exception('Previous result is not a prepare request', ExceptionCode::CON_UNPREPARED_PREV_NOT_PREPARE_REQUEST->value, [
                    'operation' => 'unprepared_error_handling',
                    'request_class' => get_class($prevRequest),
                    'expected' => Request\Prepare::class,
                ]);
            }

            $newPrepareRequest = new Request\Prepare($prevRequest->getQuery(), $prevRequest->getOptions());

            if ($statement !== null) {
                $statement->setIsRepreparing(true);
                $this->sendAsyncRequest($newPrepareRequest, $response->getStream());

                return null;
            }

            $prepareResponse = $this->syncRequest($newPrepareRequest);
            if (!($prepareResponse instanceof Response\Result)) {
                throw new Exception('Unexpected response type during repreparation', ExceptionCode::CON_REPREPARATION_UNEXPECTED_RESPONSE->value, [
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
     * @throws \Cassandra\Exception
     */
    protected function handleResponseExecuteResult(Request\Execute $request, Response\Result $result, ?Statement $statement): Response\Result {

        $result->setPreviousResult($request->getPreviousResult());

        return $result;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function handleResponsePrepareResult(Request\Prepare $request, Response\Result $result, ?Statement $statement): ?Response\Result {

        $result->setRequest($request);

        if ($statement !== null && $statement->isRepreparing()) {
            $statement->setIsRepreparing(false);
            $result = $this->handleReprepareResult($request, $result, statement: $statement);
        }

        return $result;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function handleResponseResult(Request\Request $request, Response\Result $result, ?Statement $statement): ?Response\Result {

        return match (true) {
            $request instanceof Request\Prepare => $this->handleResponsePrepareResult($request, $result, $statement),
            $request instanceof Request\Execute => $this->handleResponseExecuteResult($request, $result, $statement),
            default => $result,
        };
    }

    protected function onEvent(Response\Event $event): void {
        $this->trigger($event);

        foreach ($this->eventListeners as $listener) {
            $listener->onEvent($event);
        }
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function readResponse(): ?Response\Response {
        if ($this->node === null) {
            throw new Exception('Client is not connected to any node. Call connect() before issuing requests.', ExceptionCode::CON_NOT_CONNECTED->value, [
                'operation' => 'readResponse',
            ]);
        }

        $version = ord($this->node->read(1));

        if ($version !== $this->versionIn) {
            throw new Exception('Unsupported or mismatched CQL binary protocol version received from server.', ExceptionCode::CON_PROTOCOL_VERSION_MISMATCH->value, [
                'received_version' => $version,
                'expected_version' => $this->versionIn,
                'supported_versions' => ['3/v3', '4/v4', '5/v5'],
            ]);
        }

        /**
         * @var false|array{
         *  flags: int,
         *  stream: int,
         *  opcode: int,
         *  length: int
         * } $headerData
         */
        $headerData = unpack('Cflags/nstream/Copcode/Nlength', $this->node->read(8));
        if ($headerData === false) {
            $nodeConfig = $this->node->getConfig();

            throw new Exception('Cannot read response header', ExceptionCode::CON_CANNOT_READ_RESPONSE_HEADER->value, [
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
            throw new Exception('Invalid opcode type: ' . $headerData['opcode'], ExceptionCode::CON_INVALID_OPCODE_TYPE->value, [
                'opcode' => $headerData['opcode'],
            ], $e);
        }

        $body = $header->length === 0 ? '' : $this->node->read($header->length);

        if ($this->version < 5 && $header->length > 0 && $header->flags & Flag::COMPRESSION->value) {
            $this->lz4Decompressor->setInput($body);
            $body = $this->lz4Decompressor->decompressBlock();
        }

        $response = $this->createResponse($header, $body);

        $streamId = $header->stream;
        if ($streamId !== 0 && isset($this->statements[$streamId])) {
            $statement = $this->statements[$streamId];
            $response = $this->handleResponse($statement->getRequest(), $response, $statement);
            if ($response !== null) {
                $statement->setResponse($response);
                unset($this->statements[$streamId]);
                $this->recycledStreams->enqueue($streamId);
            }
        }

        if ($response instanceof Response\Event) {
            $this->onEvent($response);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function selectNode(): void {

        if (count($this->nodes) > 1) {
            shuffle($this->nodes);
        }

        foreach ($this->nodes as $config) {

            $className = $config->getNodeClass();

            try {
                /**
                 *  @throws \Cassandra\Exception
                */
                $this->node = new $className($config);
            } catch (Exception $e) {
                continue;
            }

            return;
        }

        $nodeConfigs = array_map(fn($config) => [
            'host' => $config->host,
            'port' => $config->port,
            'class' => $config->getNodeClass(),
        ], $this->nodes);

        throw new Exception('Unable to connect to any Cassandra node', ExceptionCode::CON_UNABLE_TO_CONNECT_ANY_NODE->value, [
            'attempted_nodes' => $nodeConfigs,
            'node_count' => count($this->nodes),
        ]);
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function sendAsyncRequest(Request\Request $request, ?int $streamId = null): Statement {
        if ($this->node === null) {
            $this->connect();
        }

        if ($this->node === null) {
            throw new Exception('Client is not connected to any node. Call connect() before issuing requests.', ExceptionCode::CON_NOT_CONNECTED->value, [
                'operation' => 'sendAsyncRequest',
            ]);
        }

        $request->setVersion($this->version);

        $streamId = $streamId ?? $this->getNewStreamId();
        $request->setStream($streamId);

        $this->node->writeRequest($request);

        if (isset($this->statements[$streamId])) {
            $statement = $this->statements[$streamId];
            $statement->setRequest($request);
            $statement->setResponse(null);
        } else {
            $statement = new Statement($this, $streamId, $request);
            $this->statements[$streamId] = $statement;
        }

        return $statement;
    }
}
