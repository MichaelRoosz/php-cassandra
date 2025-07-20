<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Response\Result;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Consistency;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;

final class Execute extends Request {
    protected string $queryId = '';
    protected ?string $resultMetadataId = null;

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
     * 
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     * 
     */
    public function __construct(
        protected Result $previousResult,
        array $values,
        protected Consistency $consistency = Consistency::ONE,
        protected ExecuteOptions $options = new ExecuteOptions()
    ) {
        parent::__construct(Opcode::REQUEST_EXECUTE);

        if (
            !($previousResult instanceof PreparedResult)
            && !($previousResult instanceof RowsResult)
        ) {
            throw new Exception('received invalid previous result', 0, [
                'expected' => [PreparedResult::class, RowsResult::class],
                'received' => get_class($previousResult),
            ]);
        }

        if ($previousResult instanceof PreparedResult) {
            $prepareData = $previousResult->getPreparedData();
            $executeCallInfo = new ExecuteCallInfo(
                id: $prepareData->id,
                queryMetadata: $prepareData->metadata,
                resultMetadataId: $prepareData->resultMetadataId,
            );
        } else {
            $executeCallInfo = $previousResult->getNextExecuteCallInfo();
            if ($executeCallInfo === null) {
                throw new Exception('prepared statement not found');
            }
        }

        $this->queryId = $executeCallInfo->id;
        $this->resultMetadataId = $executeCallInfo->resultMetadataId;

        if ($executeCallInfo->queryMetadata->columns !== null) {
            $this->values = self::enocdeValuesForColumnType($values, $executeCallInfo->queryMetadata->columns);
        } else {
            $this->values = $values;
        }

        if (
            $this->options->skipMetadata === null
            && $executeCallInfo->queryMetadata->columns !== null
        ) {
            $this->options = $this->options->withSkipMetadata(true);
        }

        if (
            $this->options->skipMetadata
            && $executeCallInfo->queryMetadata->columns === null
        ) {
            $this->options = $this->options->withSkipMetadata(false);
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

        $body .= self::queryParametersAsBinary($this->consistency, $this->values, $this->options, $this->version);

        return $body;
    }

    public function getConsistency(): Consistency {
        return $this->consistency;
    }

    public function getOptions(): ExecuteOptions {
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
