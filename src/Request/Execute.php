<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Opcode;
use Cassandra\Response\Result;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Consistency;
use Cassandra\Exception\RequestException;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;

final class Execute extends Request {
    private string $queryId = '';
    private ?string $rowsMetadataId = null;

    /**
     * The values as the caller passed them, before they were encoded against
     * the bind marker types of the prepared statement this request was built
     * from.
     *
     * Kept because a repreparation is exactly the case where those types may
     * have moved: an ALTER, or a table dropped and created again, is one of the
     * reasons a node stops recognising a statement id. The encoded values below
     * carry the types of the statement the node has just forgotten, and
     * {@see Request::encodeQueryValuesForBindMarkerTypes()} passes a
     * {@see \Cassandra\Value\ValueBase} straight through — so a replacement
     * EXECUTE built from them would go out with the new statement id and the
     * old encoding. {@see \Cassandra\Connection\ResponseDispatcher::handleReprepareResult()}
     * therefore rebuilds from these, and {@see \Cassandra\Request\Batch} keeps
     * its own values unencoded for the same reason.
     *
     * Keyed as the caller keyed them, which is also what makes the rebuild
     * honest about a marker that was renamed: the names are matched afresh
     * against the new metadata, so a value that no longer belongs to any marker
     * is reported rather than bound to whatever sits at its old position.
     *
     * @var array<mixed> $unencodedValues
     */
    private readonly array $unencodedValues;

    /**
     * @var array<mixed> $values
     */
    private $values;

    /**
     * @param array<mixed> $values
     * 
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function __construct(
        private Result $previousResult,
        array $values,
        private Consistency $consistency = Consistency::ONE,
        private ExecuteOptions $options = new ExecuteOptions()
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

        $this->unencodedValues = $values;

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
     * See {@see Request::applyDefaultKeyspace()}.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function applyDefaultKeyspace(string $keyspace): void {

        if (!$this->acceptsDefaultKeyspace($this->options->keyspace)) {
            return;
        }

        if ($this->options->keyspace !== $keyspace) {
            $this->options = $this->options->withKeyspace($keyspace);
        }

        $this->markKeyspaceAsConnectionDefault();
    }

    /**
     * See {@see Request::clearDefaultKeyspace()}.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function clearDefaultKeyspace(): void {

        if (!$this->carriesDefaultKeyspace()) {
            return;
        }

        $this->options = $this->options->withoutKeyspace();

        $this->forgetDefaultKeyspace();
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        self::assertShortString($this->queryId, 'prepared statement id');
        $body = pack('n', strlen($this->queryId)) . $this->queryId;

        if ($this->version->value >= ProtocolVersion::V5->value) {
            if ($this->rowsMetadataId === null) {
                throw new RequestException(
                    message: 'Missing result metadata id for protocol v5 execute request',
                    code: ExceptionCode::REQUEST_EXECUTE_MISSING_RESULT_METADATA_ID->value,
                    context: [
                        'protocol_version' => $this->version->inOptionFormat(),
                        'query_id' => $this->queryId,
                    ]
                );
            }

            self::assertShortString($this->rowsMetadataId, 'result metadata id');
            $body .= pack('n', strlen($this->rowsMetadataId)) . $this->rowsMetadataId;
        }

        $body .= self::encodeQueryParametersAsBinary($this->consistency, $this->values, $this->options, $this->version, namesAreExact: true);

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

    #[\Override]
    public function getRequestTimeout(): ?float {
        return $this->options->requestTimeoutInSeconds;
    }

    /**
     * The values as the caller passed them, see {@see self::$unencodedValues}.
     *
     * @return array<mixed>
     */
    public function getUnencodedValues(): array {
        return $this->unencodedValues;
    }

    /**
     * The values as they will go on the wire, encoded against the bind marker
     * types of the prepared statement this request was built from.
     *
     * {@see self::getUnencodedValues()} is what a request built to replace this
     * one has to be given; see {@see self::$unencodedValues}.
     *
     * @return array<mixed> $values
     */
    public function getValues(): array {
        return $this->values;
    }
}
