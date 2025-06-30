<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Response\Result;

final class Execute extends Request {
    protected int $consistency;
    protected int $opcode = Opcode::REQUEST_EXECUTE;

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

    protected Result $previousResult;

    protected string $queryId;

    protected ?string $resultMetadataId;

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
     * 
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     * 
     */
    public function __construct(Result $previousResult, array $values, ?int $consistency = null, array $options = []) {

        $this->previousResult = $previousResult;

        $previousResultKind = $previousResult->getKind();
        if ($previousResultKind !== Result::PREPARED && $previousResultKind !== Result::ROWS) {
            throw new Exception('received invalid previous result');
        }

        if ($previousResultKind === Result::PREPARED) {
            $prepareData = $previousResult->getPreparedData();
            $executeCallInfo = [
                'id' => $prepareData['id'],
                'query_metadata' => $prepareData['metadata'],
                'result_metadata_id' => $prepareData['result_metadata_id'] ?? null,
            ];
        } else {
            $executeCallInfo = $previousResult->getNextExecuteCallInfo();
            if ($executeCallInfo === null) {
                throw new Exception('prepared statement not found');
            }
        }

        $this->queryId = $executeCallInfo['id'];
        $this->resultMetadataId = $executeCallInfo['result_metadata_id'] ?? null;

        if (!isset($executeCallInfo['query_metadata']['columns'])) {
            throw new Exception('missing query metadata');
        }

        $this->values = self::strictTypeValues($values, $executeCallInfo['query_metadata']['columns']);

        $this->consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->options = $options;

        /**
         * @psalm-suppress InvalidArrayOffset
         * @phpstan-ignore-next-line
         */
        unset($this->options['keyspace']);

        if (!isset($this->options['skip_metadata'])) {
            $this->options['skip_metadata'] = true;
        }
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    #[\Override]
    public function getBody(): string {
        $body = pack('n', strlen($this->queryId)) . $this->queryId;

        if ($this->version >= 5) {
            if ($this->resultMetadataId === null) {
                throw new Exception('missing result metadata id');
            }

            $body .= pack('n', strlen($this->resultMetadataId)) . $this->resultMetadataId;
        }

        $body .= Request::queryParameters($this->consistency, $this->values, $this->options, $this->version);

        return $body;
    }

    public function getConsistency(): int {
        return $this->consistency;
    }

    /**
     * @return array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  now_in_seconds?: int,
     * } $options
     */
    public function getOptions(): array {
        return $this->options;
    }

    public function getPreviousResult(): Result {
        return $this->previousResult;
    }

    /**
     * @return array<mixed> $values
     */
    public function getValues(): array {
        return $this->values;
    }
}
