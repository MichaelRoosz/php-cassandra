<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayIterator;
use Cassandra\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Request\Request;
use Cassandra\Response\Result\ColumnInfo;
use Cassandra\Response\Result\Data\PreparedData;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsMetadata;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\Result\SchemaChangeResult;
use Cassandra\Response\Result\SetKeyspaceResult;
use Cassandra\Response\Result\VoidResult;
use Iterator;
use IteratorAggregate;
use TypeError;
use ValueError;

/**
 * @implements IteratorAggregate<array<string, mixed>|null>
 */
class Result extends Response implements IteratorAggregate {
    protected int $dataOffset;
    protected ResultKind $kind;
    protected ?PreparedData $lastPreparedData = null;
    protected ?Request $request = null;

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );

        $this->kind = $this->readKind();

        $this->dataOffset = $this->stream->pos();
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function asPreparedResult(): PreparedResult {
        if (!($this instanceof PreparedResult)) {
            throw new Exception('Result is not a PreparedResult', ExceptionCode::RESPONSE_RES_NOT_PREPARED_RESULT->value, [
                'result_kind' => $this->kind->name,
            ]);
        }

        return $this;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function asRowsResult(): RowsResult {
        if (!($this instanceof RowsResult)) {
            throw new Exception('Result is not a RowsResult', ExceptionCode::RESPONSE_RES_NOT_ROWS_RESULT->value, [
                'result_kind' => $this->kind->name,
            ]);
        }

        return $this;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function asSchemaChangeResult(): SchemaChangeResult {
        if (!($this instanceof SchemaChangeResult)) {
            throw new Exception('Result is not a SchemaChangeResult', ExceptionCode::RESPONSE_RES_NOT_SCHEMA_CHANGE_RESULT->value, [
                'result_kind' => $this->kind->name,
            ]);
        }

        return $this;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function asSetKeyspaceResult(): SetKeyspaceResult {
        if (!($this instanceof SetKeyspaceResult)) {
            throw new Exception('Result is not a SetKeyspaceResult', ExceptionCode::RESPONSE_RES_NOT_SET_KEYSPACE_RESULT->value, [
                'result_kind' => $this->kind->name,
            ]);
        }

        return $this;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function asVoidResult(): VoidResult {
        if (!($this instanceof VoidResult)) {
            throw new Exception('Result is not a VoidResult', ExceptionCode::RESPONSE_RES_NOT_VOID_RESULT->value, [
                'result_kind' => $this->kind->name,
            ]);
        }

        return $this;
    }

    #[\Override]
    public function getIterator(): Iterator {
        return new ArrayIterator([]);
    }

    public function getKind(): ResultKind {
        return $this->kind;
    }

    public function getLastPreparedData(): ?PreparedData {
        return $this->lastPreparedData;
    }

    public function getRequest(): ?Request {
        return $this->request;
    }

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<int, class-string<\Cassandra\Response\Result>>
     */
    public static function getResultClassMap(): array {
        return [
            ResultKind::PREPARED->value => Result\PreparedResult::class,
            ResultKind::ROWS->value => Result\RowsResult::class,
            ResultKind::SCHEMA_CHANGE->value => Result\SchemaChangeResult::class,
            ResultKind::SET_KEYSPACE->value => Result\SetKeyspaceResult::class,
            ResultKind::VOID->value => Result\VoidResult::class,
        ];
    }

    public function getRowCount(): int {
        return 0;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function setPreviousResult(Result $previousResult): static {

        if ($previousResult instanceof PreparedResult) {
            $this->lastPreparedData = $previousResult->getPreparedData();
            $this->onPreviousRowsMetadataUpdated($this->lastPreparedData->rowsMetadata);

        } elseif ($previousResult instanceof RowsResult) {

            $lastPreparedData = $previousResult->getLastPreparedData();
            if ($lastPreparedData === null) {
                throw new Exception('Prepared statement context not found in previous result', ExceptionCode::RESPONSE_RES_PREPARED_CONTEXT_NOT_FOUND->value, [
                    'operation' => 'Result::setPreviousResult',
                    'previous_result_class' => get_class($previousResult),
                ]);
            }

            $lastRowsMetadata = $previousResult->getRowsMetadata();

            $this->lastPreparedData = new PreparedData(
                id: $lastPreparedData->id,
                prepareMetadata: $lastPreparedData->prepareMetadata,
                rowsMetadataId: $lastRowsMetadata->metadataId,
                rowsMetadata: $lastRowsMetadata,
            );

            $this->onPreviousRowsMetadataUpdated($lastRowsMetadata);
        }

        return $this;
    }

    public function setRequest(Request $request): void {
        $this->request = $request;
    }

    protected function onPreviousRowsMetadataUpdated(RowsMetadata $previousRowsMetadata): void {
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    protected function readRowsMetadata(): RowsMetadata {
        $flags = $this->stream->readInt();
        $columnsCount = $this->stream->readInt();

        if ($flags & ResultFlag::ROWS_FLAG_HAS_MORE_PAGES) {
            $pagingState = $this->stream->readBytes();
        } else {
            $pagingState = null;
        }

        if ($flags & ResultFlag::ROWS_FLAG_METADATA_CHANGED) {
            $newMetadataId = $this->stream->readShortBytes();
        } else {
            $newMetadataId = null;
        }

        if (!($flags & ResultFlag::ROWS_FLAG_NO_METADATA)) {
            $columns = [];

            if ($flags & ResultFlag::ROWS_FLAG_GLOBAL_TABLES_SPEC) {
                $keyspace = $this->stream->readString();
                $tableName = $this->stream->readString();

                for ($i = 0; $i < $columnsCount; ++$i) {
                    $columns[] = new ColumnInfo(
                        keyspace: $keyspace,
                        tableName: $tableName,
                        name: $this->stream->readString(),
                        type: $this->stream->readTypeInfo(),
                    );
                }
            } else {
                for ($i = 0; $i < $columnsCount; ++$i) {
                    $columns[] = new ColumnInfo(
                        keyspace: $this->stream->readString(),
                        tableName: $this->stream->readString(),
                        name: $this->stream->readString(),
                        type: $this->stream->readTypeInfo(),
                    );
                }
            }

        } else {
            $columns = null;
        }

        return new RowsMetadata(
            flags: $flags,
            columnsCount: $columnsCount,
            pagingState: $pagingState,
            metadataId: $newMetadataId,
            columns: $columns,
        );
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    private function readKind(): ResultKind {
        $this->stream->offset(0);
        $kindInt = $this->stream->readInt();

        try {
            return ResultKind::from($kindInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid result kind value', ExceptionCode::RESPONSE_RES_INVALID_KIND_VALUE->value, [
                'operation' => 'Result::readKind',
                'result_kind' => $kindInt,
            ], $e);
        }
    }
}
