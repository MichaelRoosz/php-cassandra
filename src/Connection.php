<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Connection\FrameCodec;
use Cassandra\Protocol\Frame;
use Cassandra\Compression\Lz4Decompressor;
use SplQueue;

class Connection
{
    /**
     * @var array<int, class-string<Response\Response>> $responseClassMap
     */
    protected static array $responseClassMap = [
        Frame::OPCODE_ERROR => Response\Error::class,
        Frame::OPCODE_READY => Response\Ready::class,
        Frame::OPCODE_AUTHENTICATE => Response\Authenticate::class,
        Frame::OPCODE_SUPPORTED => Response\Supported::class,
        Frame::OPCODE_RESULT => Response\Result::class,
        Frame::OPCODE_EVENT => Response\Event::class,
        Frame::OPCODE_AUTH_CHALLENGE => Response\AuthChallenge::class,
        Frame::OPCODE_AUTH_SUCCESS => Response\AuthSuccess::class,
    ];

    /**
     * Connection options
     * @var array<string,string> $_options
     */
    protected array $_options = [
        'CQL_VERSION' => '3.0.0',
        'DRIVER_NAME' => 'php-cassandra-client',
        'DRIVER_VERSION' => '0.7.0',
        // 'COMPRESSION' => 'lz4',
        // 'THROW_ON_OVERLOAD' => '1',
    ];

    protected string $_keyspace;

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
     * }> $_nodes
     */
    protected $_nodes;

    protected ?Connection\Node $_node = null;

    protected Lz4Decompressor $_lz4Decompressor;

    protected int $_lastStreamId = 0;

    /**
     * @var array<Statement> $_statements
     */
    protected array $_statements = [];

    /**
     * @var SplQueue<int> $_recycledStreams
     */
    protected SplQueue $_recycledStreams;

    /**
     * @var array<EventListener> $_eventListeners
     */
    protected array $_eventListeners = [];

    protected int $_consistency = Request\Request::CONSISTENCY_ONE;

    protected int $_version = 0x03;
    protected int $_versionIn = 0x83;

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
    public function __construct(array $nodes, string $keyspace = '', array $options = [])
    {
        if (count($nodes) > 1) {
            shuffle($nodes);
        }

        $this->_nodes = $nodes;
        $this->_keyspace = $keyspace;

        $this->_lz4Decompressor = new Lz4Decompressor();

        foreach ($options as $optname => $optvalue) {
            $this->_options[strtoupper($optname)] = $optvalue;
        }

        /** @var SplQueue<int> $recycledStreams */
        $recycledStreams = new SplQueue();
        $this->_recycledStreams = $recycledStreams;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function _connect(): void
    {
        foreach ($this->_nodes as $options) {
            if (is_string($options)) {
                if (!preg_match('/^(((tcp|udp|unix|ssl|tls):\/\/)?[\w\.\-]+)(\:(\d+))?/i', $options, $matches)) {
                    throw new Exception('Invalid host: ' . $options);
                }

                $options = ['host' => $matches[1]];

                if (!empty($matches[5])) {
                    $options['port'] = (int) $matches[5];
                }

                // Use Connection\Stream when protocol prefix is defined.
                try {
                    $this->_node = empty($matches[2]) ? new Connection\Socket($options) : new Connection\Stream($options);
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
                    $this->_node = new $className($options);
                } catch (Exception $e) {
                    continue;
                }
            }
            return;
        }

        throw new Exception("Unable to connect to all Cassandra nodes.");
    }

    public function disconnect(): void
    {
        if ($this->_node !== null) {
            $this->_node->close();
            $this->_node = null;
        }
    }

    public function isConnected(): bool
    {
        return $this->_node !== null;
    }

    public function addEventListener(EventListener $eventListener): void
    {
        $this->_eventListeners[] = $eventListener;
    }

    protected function onEvent(Response\Event $event): void
    {
        $this->trigger($event);

        foreach ($this->_eventListeners as $listener) {
            $listener->onEvent($event);
        }
    }

    public function trigger(Response\Event $event): void
    {
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function getResponse(int $streamId = 0): Response\Response
    {
        do {
            $response = $this->_getResponse();
        } while ($response->getStream() !== $streamId);

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function _getResponse(): Response\Response
    {
        if ($this->_node === null) {
            throw new Exception('not connected');
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('C', $this->_node->read(1));
        if ($unpacked === false) {
            throw new Response\Exception('cannot read version of response');
        }

        $version = $unpacked[1];

        if ($version !== $this->_versionIn) {
            throw new Exception('php-cassandra only supports CQL binary protocol versions v3, v4 and v5. Please upgrade your Cassandra to 2.1 or later.');
        }

        /**
         * @var false|array{
         *  flags: int,
         *  stream: int,
         *  opcode: int,
         *  length: int
         * } $header
         */
        $header = unpack('Cflags/nstream/Copcode/Nlength', $this->_node->read(8));
        if ($header === false) {
            throw new Exception('cannot read header of response');
        }

        $header['version'] = $version;

        $body = $header['length'] === 0 ? '' : $this->_node->read($header['length']);

        if (!isset(self::$responseClassMap[$header['opcode']])) {
            throw new Response\Exception('Unknown response');
        }

        $responseClass = self::$responseClassMap[$header['opcode']];
        if (!is_subclass_of($responseClass, Response\Response::class)) {
            throw new Exception('received invalid response');
        }

        if ($this->_version < 5 && $header['length'] > 0 && $header['flags'] & Frame::FLAG_COMPRESSION) {
            $this->_lz4Decompressor->setInput($body);
            $body = $this->_lz4Decompressor->decompressBlock();
        }

        $response = new $responseClass($header, new Response\StreamReader($body));

        if ($header['stream'] !== 0) {
            if (isset($this->_statements[$header['stream']])) {
                $this->_statements[$header['stream']]->setResponse($response);
                unset($this->_statements[$header['stream']]);
                $this->_recycledStreams->enqueue($header['stream']);
            } elseif ($response instanceof Response\Event) {
                $this->onEvent($response);
            }
        }

        return $response;
    }

    /**
     * Wait until all statements received response.
     *
     * @throws \Cassandra\Exception
     */
    public function flush(): void
    {
        while ($this->_statements) {
            $this->_getResponse();
        }
    }

    public function getNode(): ?Connection\Node
    {
        return $this->_node;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function connect(): bool
    {
        if ($this->_node !== null) {
            return true;
        }

        $this->_connect();

        if ($this->_node === null) {
            throw new Exception('not connected');
        }

        $node = $this->_node;

        $response = $this->syncRequest(new Request\Options());
        if (!($response instanceof Response\Supported)) {
            throw new Exception('Connection options exchange failed.');
        }

        $this->configureOptions($response);

        $response = $this->syncRequest(new Request\Startup($this->_options));

        if ($response instanceof Response\Authenticate) {
            $nodeOptions = $node->getOptions();

            if (empty($nodeOptions['username']) || empty($nodeOptions['password'])) {
                throw new Exception('Username and password are required.');
            }

            if ($this->_version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new Exception('Node class is not extending NodeImplementation');
                }
                $this->_node = new FrameCodec($node, $this->_options['COMPRESSION'] ?? null);
            }

            $authResult = $this->syncRequest(new Request\AuthResponse($nodeOptions['username'], $nodeOptions['password']));
            if (!($authResult instanceof Response\AuthSuccess)) {
                throw new Exception('Authentication failed.');
            }
        } elseif ($response instanceof Response\Ready) {
            if ($this->_version >= 5) {
                if (!($node instanceof Connection\NodeImplementation)) {
                    throw new Exception('Node class is not extending NodeImplementation');
                }
                $this->_node = new FrameCodec($node, $this->_options['COMPRESSION'] ?? null);
            }
        } else {
            throw new Exception('Connection startup failed.');
        }

        if ($this->_keyspace) {
            $this->syncRequest(new Request\Query("USE {$this->_keyspace};"));
        }

        return true;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function syncRequest(Request\Request $request): Response\Response
    {
        if ($this->_node === null) {
            $this->connect();
        }

        if ($this->_node === null) {
            throw new Exception('not connected');
        }

        $request->setVersion($this->_version);

        $this->_node->writeRequest($request);

        $response = $this->getResponse();

        if ($response instanceof Response\Error) {
            throw $response->getException();
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function asyncRequest(Request\Request $request): Statement
    {
        if ($this->_node === null) {
            $this->connect();
        }

        if ($this->_node === null) {
            throw new Exception('not connected');
        }

        $request->setVersion($this->_version);

        $streamId = $this->_getNewStreamId();
        $request->setStream($streamId);

        $this->_node->writeRequest($request);

        return $this->_statements[$streamId] = new Statement($this, $streamId);
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function configureOptions(Response\Supported $supportedReponse): void
    {
        $serverOptions = $supportedReponse->getData();

        if (!isset($serverOptions['PROTOCOL_VERSIONS'])) {
            $this->_version = 3;
        } elseif (in_array('5/v5', $serverOptions['PROTOCOL_VERSIONS'])) {
            $this->_version = 5;
        } elseif (in_array('4/v4', $serverOptions['PROTOCOL_VERSIONS'])) {
            $this->_version = 4;
        } elseif (in_array('3/v3', $serverOptions['PROTOCOL_VERSIONS'])) {
            $this->_version = 3;
        } else {
            throw new Exception('Server does not support a compatible protocol version.');
        }

        if (!empty($this->_options['COMPRESSION']) && !empty($serverOptions['COMPRESSION'])) {
            $compressionAlgo = strtolower($this->_options['COMPRESSION']);

            if (!in_array($compressionAlgo, $serverOptions['COMPRESSION'])) {
                throw new Exception('Compression "' . $compressionAlgo  . '" not supported by server.');
            }

            $this->_options['COMPRESSION'] = $compressionAlgo;
        } else {
            unset($this->_options['COMPRESSION']);
        }

        if ($this->_version >= 4) {
            if (!empty($this->_options['THROW_ON_OVERLOAD'])) {
                $this->_options['THROW_ON_OVERLOAD'] = '1';
            } else {
                $this->_options['THROW_ON_OVERLOAD'] = '0';
            }
        } else {
            unset($this->_options['THROW_ON_OVERLOAD']);
        }

        if ($this->_version < 5) {
            unset($this->_options['DRIVER_NAME']);
            unset($this->_options['DRIVER_VERSION']);
        }

        $this->_versionIn = $this->_version + 0x80;
    }

    /**
     * @throws \Cassandra\Exception
     */
    protected function _getNewStreamId(): int
    {
        if ($this->_lastStreamId < 32767) {
            return ++$this->_lastStreamId;
        }

        while ($this->_recycledStreams->isEmpty()) {
            $this->_getResponse();
        }

        return $this->_recycledStreams->dequeue();
    }

    /**
     * @return array{
     *   id: string,
     *   result_metadata_id?: string,
     *   metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     page_state?: ?string,
     *     new_metadata_id?: string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     *   result_metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     page_state?: ?string,
     *     new_metadata_id?: string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     * }
     *
     * @throws \Cassandra\Exception
     */
    public function prepare(string $cql): array
    {
        $response = $this->syncRequest(new Request\Prepare($cql));
        if (!($response instanceof Response\Result)) {
            throw new Exception('received invalid response');
        }

        if ($response->getKind() !== Response\Result::PREPARED) {
            throw new Exception('received invalid result');
        }

        return $response->getPreparedData();
    }

    /**
     * @param array<mixed> $values
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     * } $options
     *
     * @throws \Cassandra\Exception
     */
    public function executeSync(string $queryId, array $values = [], ?int $consistency = null, array $options = []): Response\Result
    {
        $request = new Request\Execute($queryId, $values, $consistency === null ? $this->_consistency : $consistency, $options);

        $response = $this->syncRequest($request);

        if (!($response instanceof Response\Result)) {
            throw new Exception('received invalid response');
        }

        return $response;
    }

    /**
     * @param array<mixed> $values
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     * } $options
     *
     * @throws \Cassandra\Exception
     */
    public function executeAsync(string $queryId, array $values = [], ?int $consistency = null, array $options = []): Statement
    {
        $request = new Request\Execute($queryId, $values, $consistency === null ? $this->_consistency : $consistency, $options);

        return $this->asyncRequest($request);
    }

    /**
     * @param array<mixed> $values
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     * } $options
     *
     * @throws \Cassandra\Exception
     */
    public function querySync(string $cql, array $values = [], ?int $consistency = null, array $options = []): Response\Result
    {
        $request = new Request\Query($cql, $values, $consistency === null ? $this->_consistency : $consistency, $options);

        $response = $this->syncRequest($request);

        if (!($response instanceof Response\Result)) {
            throw new Exception('received invalid response');
        }

        return $response;
    }

    /**
     * @param array<mixed> $values
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     * } $options
     *
     * @throws \Cassandra\Exception
     */
    public function queryAsync(string $cql, array $values = [], ?int $consistency = null, array $options = []): Statement
    {
        $request = new Request\Query($cql, $values, $consistency === null ? $this->_consistency : $consistency, $options);

        return $this->asyncRequest($request);
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function batchSync(Request\Batch $batchRequest): Response\Result
    {
        $response = $this->syncRequest($batchRequest);

        if (!($response instanceof Response\Result)) {
            throw new Exception('received invalid response');
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function batchAsync(Request\Batch $batchRequest): Statement
    {
        return $this->asyncRequest($batchRequest);
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function setKeyspace(string $keyspace): ?Response\Result
    {
        $this->_keyspace = $keyspace;

        if ($this->_node === null) {
            return null;
        }

        $response = $this->syncRequest(new Request\Query("USE {$this->_keyspace};"));
        if (!($response instanceof Response\Result)) {
            throw new Exception('received invalid response');
        }

        return $response;
    }

    public function setConsistency(int $consistency): void
    {
        $this->_consistency = $consistency;
    }
}
