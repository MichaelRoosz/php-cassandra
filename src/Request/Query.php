<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;

class Query extends Request {
    public const FLAG_VALUES = 0x01;
    public const FLAG_SKIP_METADATA = 0x02;
    public const FLAG_PAGE_SIZE = 0x04;
    public const FLAG_WITH_PAGING_STATE = 0x08;
    public const FLAG_WITH_SERIAL_CONSISTENCY = 0x10;
    public const FLAG_WITH_DEFAULT_TIMESTAMP = 0x20;
    public const FLAG_WITH_NAMES_FOR_VALUES = 0x40;
    public const FLAG_WITH_KEYSPACE = 0x80;
    public const FLAG_WITH_NOW_IN_SECONDS = 0x0100;

    protected int $opcode = Frame::OPCODE_QUERY;

    protected string $_cql;

    /**
     * @var array<mixed> $_values
     */
    protected array $_values;

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
     * QUERY
     *
     * Performs a CQL query. The body of the message consists of a CQL query as a [long
     * string] followed by the [consistency] for the operation.
     *
     * Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
     * TRUNCATE, ...).
     *
     * The server will respond to a QUERY message with a RESULT message, the content
     * of which depends on the query.
     *
     * @param array<mixed> $values
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
    public function __construct(string $cql, array $values = [], ?int $consistency = null, array $options = []) {
        $this->_cql = $cql;
        $this->_values = $values;
        $this->_consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->_options = $options;
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    public function getBody(): string {
        $body = pack('N', strlen($this->_cql)) . $this->_cql;
        $body .= Request::queryParameters($this->_consistency, $this->_values, $this->_options, $this->version);

        return $body;
    }
}
