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
use Cassandra\Response\ResultKind;
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
     * @var array<string|array{
     *  class?: class-string<\Cassandra\Connection\NodeImplementation>,
     *  host?: ?string,
     *  port?: int,
     *  username?: ?string,
     *  password?: ?string,
     *  socket?: array<int, array<mixed>|int|string>,
     * }|array{
     *  class?: class-string<\Cassandra\Connection\NodeImplementation>,
     *  host?: ?string,
     *  port?: int,
     *  username?: ?string,
     *  password?: ?string,
     *  timeout?: int,
     *  connectTimeout?: int,
     *  persistent?: bool,
     *  ssl?: array<string, mixed>,
     * }> $nodes
     */
    protected $nodes;

    /**
     * Connection options
     * @var array<string,string> $options
     */
    protected array $options = [
        'CQL_VERSION' => '3.0.0',
        'DRIVER_NAME' => 'php-cassandra-client',
        'DRIVER_VERSION' => '0.7.0',
        // 'COMPRESSION' => 'lz4',
        // 'THROW_ON_OVERLOAD' => '1',
    ];

    /**
     * @var SplQueue<int> $recycledStreams
     */
    protected SplQueue $recycledStreams;

    /**
     * @var array<int, class-string<\Cassandra\Response\Response>> $responseClassMap
     */
    protected static array $responseClassMap = [
        Opcode::RESPONSE_ERROR->value => Response\Error::class,
        Opcode::RESPONSE_READY->value => Response\Ready::class,
        Opcode::RESPONSE_AUTHENTICATE->value => Response\Authenticate::class,
        Opcode::RESPONSE_SUPPORTED->value => Response\Supported::class,
        Opcode::RESPONSE_RESULT->value => Response\Result::class,
        Opcode::RESPONSE_EVENT->value => Response\Event::class,
        Opcode::RESPONSE_AUTH_CHALLENGE->value => Response\AuthChallenge::class,
        Opcode::RESPONSE_AUTH_SUCCESS->value => Response\AuthSuccess::class,
    ];

    /**
     * @var array<int, class-string<\Cassandra\Response\Result>> $resultResponseClassMap
     */
    protected static array $resultResponseClassMap = [
        ResultKind::PREPARED->value => Response\Result\PreparedResult::class,
        ResultKind::ROWS->value => Response\Result\RowsResult::class,
        ResultKind::SCHEMA_CHANGE->value => Response\Result\SchemaChangeResult::class,
        ResultKind::SET_KEYSPACE->value => Response\Result\SetKeyspaceResult::class,
        ResultKind::VOID->value => Response\Result\VoidResult::class,
    ];

    /**
     * @var array<Statement> $statements
     */
    protected array $statements = [];

    protected int $version = 0x03;
    protected int $versionIn = 0x83;

    /**
     * @param array<string|array{
     *  class?: class-string<\Cassandra\Connection\NodeImplementation>,
     *  host?: ?string,
     *  port?: int,
     *  username?: ?string,
     *  password?: ?string,
     *  socket?: array<int, array<mixed>|int|string>,
     * }|array{
     *  class?: class-string<\Cassandra\Connection\NodeImplementation>,
     *  host?: ?string,
     *  port?: int,
     *  username?: ?string,
     *  password?: ?string,
     *  timeout?: int,
     *  connectTimeout?: int,
     *  persistent?: bool,
     *  ssl?: array<string, mixed>,
     * }> $nodes
     * @param string $keyspace
     * @param array<string,string> $options
     */
    public function __construct(array $nodes, string $keyspace = '', array $options = []) {
        if (count($nodes) > 1) {
            shuffle($nodes);
        }

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
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
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

        $this->connectToNode();

        if ($this->node === null) {
            throw new Exception('not connected');
        }

        $node = $this->node;

        $response = $this->syncRequest(new Request\Options());
        if (!($response instanceof Response\Supported)) {
            throw new Exception('Connection options exchange failed, received unexpected response type: ' . get_class($response), 0, [
                'expected' => Response\Supported::class,
                'received' => get_class($response),
            ]);
        }

        $this->configureOptions($response);

        $response = $this->syncRequest(new Request\Startup($this->options));

        if ($response instanceof Response\Authenticate) {
            $nodeOptions = $node->getOptions();

            if (!isset($nodeOptions['username']) || !isset($nodeOptions['password'])) {
                throw new Exception('Username and password are required.');
            }

            if (!$nodeOptions['username'] || !$nodeOptions['password']) {
                throw new Exception('Username and password must not be empty required.');
            }

            if ($this->version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new Exception('Node class is not extending NodeImplementation');
                }
                $this->node = new FrameCodec($node, $this->options['COMPRESSION'] ?? '');
            }

            $authResult = $this->syncRequest(new Request\AuthResponse($nodeOptions['username'], $nodeOptions['password']));
            if (!($authResult instanceof Response\AuthSuccess)) {
                throw new Exception('Authentication failed.');
            }
        } elseif ($response instanceof Response\Ready) {
            if ($this->version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new Exception('Node class is not extending NodeImplementation');
                }
                $this->node = new FrameCodec($node, $this->options['COMPRESSION'] ?? '');
            }
        } else {
            throw new Exception('Connection startup failed.');
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
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
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
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
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
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
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
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
                'expected' => Response\Result::class,
                'received' => get_class($response),
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
            throw new Exception('not connected');
        }

        $request->setVersion($this->version);
        $this->node->writeRequest($request);

        $response = $this->getNextResponseForStream(streamId: 0);
        $response = $this->handleResponse($request, $response);

        if ($response === null) {
            throw new Exception('received unexpected null response');
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
            throw new Exception('Server does not support a compatible protocol version.');
        }

        if (isset($this->options['COMPRESSION']) && $this->options['COMPRESSION']
            && isset($serverOptions['COMPRESSION']) && $serverOptions['COMPRESSION']
        ) {
            $compressionAlgo = strtolower($this->options['COMPRESSION']);

            if (!in_array($compressionAlgo, $serverOptions['COMPRESSION'])) {
                throw new Exception('Compression "' . $compressionAlgo . '" not supported by server.');
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
    protected function connectToNode(): void {
        foreach ($this->nodes as $options) {
            if (is_string($options)) {
                if (!preg_match('/^(((tcp|udp|unix|ssl|tls):\/\/)?[\w\.\-]+)(\:(\d+))?/i', $options, $matches)) {
                    throw new Exception('Invalid host: ' . $options);
                }

                $options = ['host' => $matches[1]];

                if (isset($matches[5]) && $matches[5]) {
                    $options['port'] = (int) $matches[5];
                }

                // Use Connection\Stream when protocol prefix is defined.
                try {
                    $this->node = (!isset($matches[2]) || !$matches[2]) ? new Connection\Socket($options) : new Connection\Stream($options);
                } catch (Exception $e) {
                    continue;
                }
            } else {
                $className = isset($options['class']) ? $options['class'] : Connection\Socket::class;
                if (!is_subclass_of($className, Connection\NodeImplementation::class)) {
                    throw new Exception('Invalid connection class: ' . $className);
                }

                try {
                    /**
                     *  @throws \Cassandra\Exception
                    */
                    $this->node = new $className($options);
                } catch (Exception $e) {
                    continue;
                }
            }

            return;
        }

        throw new Exception('Unable to connect to all Cassandra nodes.');
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
            throw new Exception('received unexpected null response');
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function handleReprepareResult(Request\Prepare $request, Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null): ?Response\Result {

        if (!($result instanceof Response\Result\PreparedResult)) {
            throw new Exception('received unexpected result type: ' . get_class($result), 0, [
                'expected' => Response\Result::class,
                'received' => get_class($result),
            ]);
        }

        if ($statement !== null) {
            $originalRequest = $statement->getOriginalRequest();
        }

        if (!($originalRequest instanceof Request\Execute)) {
            throw new Exception('original request is not an execute request');
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
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
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
            && ($response->getData()['code'] === Response\ErrorType::UNPREPARED->value)
        ) {

            $prevResult = $request->getPreviousResult();
            if (!($prevResult instanceof Response\Result\PreparedResult)) {
                throw new Exception('Unexpected previous result type for unprepared error: ' . get_class($prevResult), 0, [
                    'expected' => Response\Result::class,
                    'received' => get_class($prevResult),
                ]);
            }

            $prevRequest = $prevResult->getRequest();
            if ($prevRequest === null) {
                throw new Exception('request of previous result is null');
            }
            if (!($prevRequest instanceof Request\Prepare)) {
                throw new Exception('previous result is not a prepare request');
            }

            $newPrepareRequest = new Request\Prepare($prevRequest->getQuery(), $prevRequest->getOptions());

            if ($statement !== null) {
                $statement->setIsRepreparing(true);
                $this->sendAsyncRequest($newPrepareRequest, $response->getStream());

                return null;
            }

            $prepareResponse = $this->syncRequest($newPrepareRequest);
            if (!($prepareResponse instanceof Response\Result)) {
                throw new Exception('received unexpected response type: ' . get_class($prepareResponse), 0, [
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
            throw new Exception('not connected');
        }

        $version = ord($this->node->read(1));

        if ($version !== $this->versionIn) {
            throw new Exception('php-cassandra only supports CQL binary protocol versions v3, v4 and v5. Please upgrade your Cassandra to 2.1 or later.');
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
            throw new Exception('cannot read header of response');
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
            throw new Exception('Invalid opcode type: ' . $headerData['opcode'], 0, [
                'opcode' => $headerData['opcode'],
            ]);
        }

        $body = $header->length === 0 ? '' : $this->node->read($header->length);

        if (!isset(self::$responseClassMap[$header->opcode->value])) {
            throw new Response\Exception('Unknown response');
        }

        $responseClass = self::$responseClassMap[$header->opcode->value];
        if (!is_subclass_of($responseClass, Response\Response::class)) {
            throw new Exception('received unexpected response type: ' . $responseClass, 0, [
                'expected' => Response\Response::class,
                'received' => $responseClass,
            ]);
        }

        if ($this->version < 5 && $header->length > 0 && $header->flags & Flag::COMPRESSION->value) {
            $this->lz4Decompressor->setInput($body);
            $body = $this->lz4Decompressor->decompressBlock();
        }

        $streamReader = new Response\StreamReader($body);

        switch ($responseClass) {
            case Response\Result::class:
                $resultKind = $streamReader->readInt();
                $streamReader->offset(0);

                if (isset(self::$resultResponseClassMap[$resultKind])) {
                    $responseClass = self::$resultResponseClassMap[$resultKind];
                } else {
                    throw new Exception('Unknown result kind: ' . $resultKind, 0, [
                        'expected' => array_keys(self::$resultResponseClassMap),
                        'received' => $resultKind,
                    ]);
                }

                break;
            default:
                break;
        }

        $response = new $responseClass($header, $streamReader);

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
    protected function sendAsyncRequest(Request\Request $request, ?int $streamId = null): Statement {
        if ($this->node === null) {
            $this->connect();
        }

        if ($this->node === null) {
            throw new Exception('not connected');
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
