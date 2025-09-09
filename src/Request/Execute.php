<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Opcode;
use Cassandra\Response\Result;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Consistency;
use Cassandra\Exception\RequestException;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;

final class Execute extends Request {
    protected string $queryId = '';
    protected ?string $rowsMetadataId = null;

    /**
     * @var array<mixed> $values
     */
    protected $values;

    /**
     * @param array<mixed> $values
     * 
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function __construct(
        protected Result $previousResult,
        array $values,
        protected Consistency $consistency = Consistency::ONE,
        protected ExecuteOptions $options = new ExecuteOptions()
    ) {
        parent::__construct(Opcode::REQUEST_EXECUTE);

        if ($this->options->namesForValues === null && !array_is_list($values)) {
            $this->options = $this->options->withNamesForValues(true);
        }

        if ($previousResult instanceof PreparedResult) {
            $preparedData = $previousResult->getPreparedData();
            $pagingStateOfPreviousResult = null;

        } elseif ($previousResult instanceof RowsResult) {
            $preparedData = $previousResult->getLastPreparedData();
            if ($preparedData === null) {
                throw new RequestException(
                    message: 'Prepared statement not found for resumption of execution',
                    code: ExceptionCode::REQUEST_EXECUTE_PREPARED_STATEMENT_NOT_FOUND->value,
                    context: [
                        'previous_result_class' => get_class($previousResult),
                        'hint' => 'Ensure the previous SELECT included metadata required for paging',
                    ]
                );
            }
            $pagingStateOfPreviousResult = $previousResult->getRowsMetadata()->pagingState;
        } else {
            throw new RequestException(
                message: 'Execute request received an invalid previous result instance',
                code: ExceptionCode::REQUEST_EXECUTE_INVALID_PREVIOUS_RESULT->value,
                context: [
                    'expected' => [PreparedResult::class, RowsResult::class],
                    'received' => get_class($previousResult),
                ]
            );
        }

        $this->queryId = $preparedData->id;
        $this->rowsMetadataId = $preparedData->rowsMetadataId;

        $this->values = self::encodeQueryValuesForBindMarkerTypes(
            $values,
            $preparedData->prepareMetadata->bindMarkers,
            $this->options->namesForValues ?? false
        );

        if (
            $this->options->skipMetadata === null
            && $preparedData->rowsMetadata->columns !== null
        ) {
            $this->options = $this->options->withSkipMetadata(true);
        }

        if (
            $this->options->skipMetadata
            && $preparedData->rowsMetadata->columns === null
        ) {
            $this->options = $this->options->withSkipMetadata(false);
        }

        if (
            $this->options->pagingState === null
            && $pagingStateOfPreviousResult !== null
        ) {
            $this->options = $this->options->withPagingState($pagingStateOfPreviousResult);
        }
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        $body = pack('n', strlen($this->queryId)) . $this->queryId;

        if ($this->version >= 5) {
            if ($this->rowsMetadataId === null) {
                throw new RequestException(
                    message: 'Missing result metadata id for protocol v5 execute request',
                    code: ExceptionCode::REQUEST_EXECUTE_MISSING_RESULT_METADATA_ID->value,
                    context: [
                        'protocol_version' => $this->version,
                        'query_id' => $this->queryId,
                    ]
                );
            }

            $body .= pack('n', strlen($this->rowsMetadataId)) . $this->rowsMetadataId;
        }

        $body .= self::encodeQueryParametersAsBinary($this->consistency, $this->values, $this->options, $this->version);

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
