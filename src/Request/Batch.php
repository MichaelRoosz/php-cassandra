<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Type;
use Cassandra\Exception;
use Cassandra\Response\Result;

class Batch extends Request {
    final public const TYPE_COUNTER = 2;
    final public const TYPE_LOGGED = 0;
    final public const TYPE_UNLOGGED = 1;

    protected int $batchType;

    protected int $consistency;

    protected int $opcode = Opcode::REQUEST_BATCH;

    /**
     * @var array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  keyspace?: string,
     *  now_in_seconds?: int,
     * } $options
     */
    protected array $options;

    /**
     * @var array<string> $queryArray
     */
    protected array $queryArray = [];

    /**
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  keyspace?: string,
     *  now_in_seconds?: int,
     * } $options
     */
    public function __construct(?int $type = null, ?int $consistency = null, array $options = []) {
        $this->batchType = $type === null ? Batch::TYPE_LOGGED : $type;
        $this->consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->options = $options;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    public function appendPreparedStatement(Result $prepareResult, array $values = []): static {
        if ($prepareResult->getKind() !== Result::PREPARED) {
            throw new Exception('Invalid prepared statement');
        }

        $prepareData = $prepareResult->getPreparedData();

        $queryId = $prepareData['id'];

        if (!isset($prepareData['metadata']['columns'])) {
            throw new Exception('missing query metadata');
        }

        $values = self::strictTypeValues($values, $prepareData['metadata']['columns']);

        $binary = chr(1);

        $binary .= pack('n', strlen($queryId)) . $queryId;
        $binary .= Request::valuesBinary($values, !empty($this->options['names_for_values']));

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     */
    public function appendQuery(string $cql, array $values = []): static {
        $binary = chr(0);

        $binary .= pack('N', strlen($cql)) . $cql;
        $binary .= Request::valuesBinary($values, !empty($this->options['names_for_values']));

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  keyspace?: string,
     *  now_in_seconds?: int,
     * } $options
     *
     * @throws \Cassandra\Exception
     */
    public static function batchQueryParameters(int $consistency, array $options = [], int $version = 3): string {
        $flags = 0;
        $optional = '';

        if (isset($options['serial_consistency'])) {
            $flags |= Query::FLAG_WITH_SERIAL_CONSISTENCY;
            $optional .= pack('n', $options['serial_consistency']);
        }

        if (isset($options['default_timestamp'])) {
            $flags |= Query::FLAG_WITH_DEFAULT_TIMESTAMP;
            $optional .= (new Type\Bigint($options['default_timestamp']))->getBinary();
        }

        if (!empty($options['names_for_values'])) {
            /**
             * @link https://github.com/duoshuo/php-cassandra/issues/40
             * @link https://issues.apache.org/jira/browse/CASSANDRA-10246
             */
            throw new Exception('NAMES_FOR_VALUES in batch request is not supported (see https://issues.apache.org/jira/browse/CASSANDRA-10246). Keep NAMES_FOR_VALUES flag false to avoid this bug.');
        }

        if (isset($options['keyspace'])) {
            if ($version >= 5) {
                $flags |= Query::FLAG_WITH_KEYSPACE;
                $optional .= pack('n', strlen($options['keyspace'])) . $options['keyspace'];
            } else {
                throw new Exception('Option "keyspace" not supported by server');
            }
        }

        if (isset($options['now_in_seconds'])) {
            if ($version >= 5) {
                $flags |= Query::FLAG_WITH_NOW_IN_SECONDS;
                $optional .= pack('N', $options['now_in_seconds']);
            } else {
                throw new Exception('Option "now_in_seconds" not supported by server');
            }
        }

        if ($version < 5) {
            return pack('n', $consistency) . chr($flags) . $optional;
        } else {
            return pack('n', $consistency) . pack('N', $flags) . $optional;
        }
    }

    /**
     * @throws \Cassandra\Exception
     */
    #[\Override]
    public function getBody(): string {
        return chr($this->batchType)
            . pack('n', count($this->queryArray)) . implode('', $this->queryArray)
            . self::batchQueryParameters($this->consistency, $this->options, $this->version);
    }

    /**
     * @deprecated batchQueryParameters() should be used instead
     *
     * @param array<mixed> $values - always empty
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  keyspace?: string,
     *  now_in_seconds?: int,
     * } $options
     *
     * @throws \Cassandra\Exception
     */
    #[\Override]
    public static function queryParameters(int $consistency, array $values = [], array $options = [], int $version = 3): string {
        return self::batchQueryParameters($consistency, $options, $version);
    }
}
