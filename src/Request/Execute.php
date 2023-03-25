<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;

class Execute extends Request {
    protected int $consistency;
    protected int $opcode = Frame::OPCODE_EXECUTE;

    /**
     * @var array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  now_in_seconds?: int,
     * } $options
     */
    protected $options;

    protected string $queryId;

    /**
     * @var array<mixed> $values
     */
    protected $values;

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
        $this->queryId = $queryId;
        $this->values = $values;

        $this->consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->options = $options;

        /**
         * @psalm-suppress InvalidArrayOffset
         * @phpstan-ignore-next-line
         */
        unset($this->options['keyspace']);
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    public function getBody(): string {
        $body = pack('n', strlen($this->queryId)) . $this->queryId;

        $body .= Request::queryParameters($this->consistency, $this->values, $this->options, $this->version);

        return $body;
    }
}
