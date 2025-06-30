<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

final class Query extends Request {
    final public const FLAG_PAGE_SIZE = 0x04;
    final public const FLAG_SKIP_METADATA = 0x02;
    final public const FLAG_VALUES = 0x01;
    final public const FLAG_WITH_DEFAULT_TIMESTAMP = 0x20;
    final public const FLAG_WITH_KEYSPACE = 0x80;
    final public const FLAG_WITH_NAMES_FOR_VALUES = 0x40;
    final public const FLAG_WITH_NOW_IN_SECONDS = 0x0100;
    final public const FLAG_WITH_PAGING_STATE = 0x08;
    final public const FLAG_WITH_SERIAL_CONSISTENCY = 0x10;

    protected int $consistency;

    protected string $cql;

    protected int $opcode = Opcode::REQUEST_QUERY;

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
     * @var array<mixed> $values
     */
    protected array $values;

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
        $this->cql = $cql;
        $this->values = $values;
        $this->consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->options = $options;
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    #[\Override]
    public function getBody(): string {
        $body = pack('N', strlen($this->cql)) . $this->cql;
        $body .= Request::queryParameters($this->consistency, $this->values, $this->options, $this->version);

        return $body;
    }
}
