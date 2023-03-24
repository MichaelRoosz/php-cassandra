<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;

class Execute extends Request {
    protected int $opcode = Frame::OPCODE_EXECUTE;

    protected string $_queryId;

    protected int $_consistency;

    /**
     * @var array<mixed> $_values
     */
    protected $_values;

    /**
     * @var array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  now_in_seconds?: int,
     * } $_options
     */
    protected $_options;

    /**
     * EXECUTE
     *
     * Executes a prepared query. The body of the message must be:
     * <id><n><value_1>....<value_n><consistency>
     * where:
     * - <id> is the prepared query ID. It's the [short bytes] returned as a
     * response to a PREPARE message.
     * - <n> is a [short] indicating the number of following values.
     * - <value_1>...<value_n> are the [bytes] to use for bound variables in the
     * prepared query.
     * - <consistency> is the [consistency] level for the operation.
     * Note that the consistency is ignored by some (prepared) queries (USE, CREATE,
     * ALTER, TRUNCATE, ...).
     * The response from the server will be a RESULT message.
     *
     * @param array<mixed> $values
     * @param array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  now_in_seconds?: int,
     * } $options
     */
    public function __construct(string $queryId, array $values, ?int $consistency = null, array $options = []) {
        $this->_queryId = $queryId;
        $this->_values = $values;

        $this->_consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->_options = $options;

        /**
         * @psalm-suppress InvalidArrayOffset
         * @phpstan-ignore-next-line
         */
        unset($this->_options['keyspace']);
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    public function getBody(): string {
        $body = pack('n', strlen($this->_queryId)) . $this->_queryId;

        $body .= Request::queryParameters($this->_consistency, $this->_values, $this->_options, $this->version);

        return $body;
    }
}
