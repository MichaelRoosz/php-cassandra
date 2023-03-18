<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;
use Cassandra\Type;
use Cassandra\Exception;

class Batch extends Request
{
    public const TYPE_LOGGED = 0;
    public const TYPE_UNLOGGED = 1;
    public const TYPE_COUNTER = 2;

    protected int $opcode = Frame::OPCODE_BATCH;

    /**
     * @var array<string> $_queryArray
     */
    protected array $_queryArray = [];

    protected int $_batchType;

    protected int $_consistency;

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
     * } $_options
     */
    protected array $_options;

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
    public function __construct(?int $type = null, ?int $consistency = null, array $options = [])
    {
        $this->_batchType = $type === null ? Batch::TYPE_LOGGED : $type;
        $this->_consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->_options = $options;
    }

    /**
     * @throws \Cassandra\Exception
     */
    public function getBody(): string
    {
        return pack('C', $this->_batchType)
            . pack('n', count($this->_queryArray)) . implode('', $this->_queryArray)
            . self::batchQueryParameters($this->_consistency, $this->_options, $this->version);
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     */
    public function appendQuery(string $cql, array $values = []): static
    {
        $binary = pack('C', 0);

        $binary .= pack('N', strlen($cql)) . $cql;
        $binary .= Request::valuesBinary($values, !empty($this->_options['names_for_values']));

        $this->_queryArray[] = $binary;

        return $this;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     */
    public function appendQueryId(string $queryId, array $values = []): static
    {
        $binary = pack('C', 1);

        $binary .= pack('n', strlen($queryId)) . $queryId;
        $binary .= Request::valuesBinary($values, !empty($this->_options['names_for_values']));

        $this->_queryArray[] = $binary;

        return $this;
    }

    /**
     * @deprecated batchQueryParameters should be used instead
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
    public static function queryParameters(int $consistency, array $values = [], array $options = [], int $version = 3): string
    {
        return self::batchQueryParameters($consistency, $options, $version);
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
    public static function batchQueryParameters(int $consistency, array $options = [], int $version = 3): string
    {
        $flags = 0;
        $optional = '';

        if (isset($options['serial_consistency'])) {
            $flags |= Query::FLAG_WITH_SERIAL_CONSISTENCY;
            $optional .= pack('n', $options['serial_consistency']);
        }

        if (isset($options['default_timestamp'])) {
            $flags |= Query::FLAG_WITH_DEFAULT_TIMESTAMP;
            $optional .= Type\Bigint::binary($options['default_timestamp']);
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
            return pack('n', $consistency) . pack('C', $flags) . $optional;
        } else {
            return pack('n', $consistency) . pack('N', $flags) . $optional;
        }
    }
}
