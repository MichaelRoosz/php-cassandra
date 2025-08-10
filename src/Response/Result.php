<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayIterator;
use Cassandra\ColumnInfo;
use Cassandra\Metadata;
use Cassandra\Protocol\Header;
use Cassandra\Request\ExecuteCallInfo;
use Cassandra\Request\Request;
use Cassandra\Response\Result\PreparedResult;
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
    public const RESULT_RESPONSE_CLASS_MAP = [
        ResultKind::PREPARED->value => Result\PreparedResult::class,
        ResultKind::ROWS->value => Result\RowsResult::class,
        ResultKind::SCHEMA_CHANGE->value => Result\SchemaChangeResult::class,
        ResultKind::SET_KEYSPACE->value => Result\SetKeyspaceResult::class,
        ResultKind::VOID->value => Result\VoidResult::class,
    ];

    protected int $dataOffset;
    protected ResultKind $kind;

    protected ?Metadata $metadataOfPreviousResult = null;
    protected ?ExecuteCallInfo $nextExecuteCallInfo = null;
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
            throw new Exception('Result is not a PreparedResult', Exception::RES_NOT_PREPARED_RESULT, [
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
            throw new Exception('Result is not a RowsResult', Exception::RES_NOT_ROWS_RESULT, [
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
            throw new Exception('Result is not a SchemaChangeResult', Exception::RES_NOT_SCHEMA_CHANGE_RESULT, [
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
            throw new Exception('Result is not a SetKeyspaceResult', Exception::RES_NOT_SET_KEYSPACE_RESULT, [
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
            throw new Exception('Result is not a VoidResult', Exception::RES_NOT_VOID_RESULT, [
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

    public function getNextExecuteCallInfo(): ?ExecuteCallInfo {
        return $this->nextExecuteCallInfo;
    }

    public function getRequest(): ?Request {
        return $this->request;
    }

    public function getRowCount(): int {
        return 0;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function setPreviousResult(Result $previousResult): static {

        if ($previousResult instanceof PreparedResult) {
            $prepareData = $previousResult->getPreparedData();

            $this->metadataOfPreviousResult = $prepareData->resultMetadata;
            $this->onPreviousResultUpdated();

            $this->nextExecuteCallInfo = new ExecuteCallInfo(
                id: $prepareData->id,
                queryMetadata: $prepareData->metadata,
                resultMetadataId: $prepareData->resultMetadataId ?? null,
            );

        } elseif ($previousResult instanceof RowsResult) {

            // todo: verify this logic

            $previousMetadata = $previousResult->getMetadata();

            $this->metadataOfPreviousResult = $previousMetadata;
            $this->onPreviousResultUpdated();

            $lastExecuteCallInfo = $previousResult->getNextExecuteCallInfo();
            if ($lastExecuteCallInfo === null) {
                throw new Exception('Prepared statement context not found in previous result', Exception::RES_PREPARED_CONTEXT_NOT_FOUND, [
                    'operation' => 'Result::setPreviousResult',
                    'previous_result_class' => get_class($previousResult),
                ]);
            }

            $resultMetadataId = $previousMetadata->newMetadataId ?? $lastExecuteCallInfo->resultMetadataId ?? null;

            $this->nextExecuteCallInfo = new ExecuteCallInfo(
                id: $lastExecuteCallInfo->id,
                queryMetadata: $lastExecuteCallInfo->queryMetadata,
                resultMetadataId: $resultMetadataId,
            );
        }

        return $this;
    }

    public function setRequest(Request $request): void {
        $this->request = $request;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function getMetadata(): Metadata {
        throw new Exception('Result metadata is not available for this result kind', Exception::RES_METADATA_NOT_AVAILABLE, [
            'operation' => 'Result::getMetadata',
            'result_kind' => $this->kind->name,
        ]);
    }

    protected function onPreviousResultUpdated(): void {
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readKind(): ResultKind {
        $this->stream->offset(0);
        $kindInt = $this->stream->readInt();

        try {
            return ResultKind::from($kindInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid result kind value', Exception::RES_INVALID_KIND_VALUE, [
                'operation' => 'Result::readKind',
                'result_kind' => $kindInt,
            ]);
        }
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    protected function readMetadata(bool $isPrepareMetaData = false): Metadata {
        $flags = $this->stream->readInt();
        $columnsCount = $this->stream->readInt();

        if ($flags & ResultFlag::ROWS_FLAG_HAS_MORE_PAGES->value) {
            $pagingState = $this->stream->readBytes();
        } else {
            $pagingState = null;
        }

        if ($flags & ResultFlag::ROWS_FLAG_METADATA_CHANGED->value) {
            $newMetadataId = $this->stream->readString();
        } else {
            $newMetadataId = null;
        }

        if ($this->getVersion() >= 4 && $isPrepareMetaData) {
            $pkCount = $this->stream->readInt();
            $pkIndex = [];

            if ($pkCount > 0) {
                for ($i = 0; $i < $pkCount; ++$i) {
                    $pkIndex[] =  $this->stream->readShort();
                }
            }
        } else {
            $pkCount = null;
            $pkIndex = null;
        }

        if (!($flags & ResultFlag::ROWS_FLAG_NO_METADATA->value)) {
            $columns = [];

            if ($flags & ResultFlag::ROWS_FLAG_GLOBAL_TABLES_SPEC->value) {
                $keyspace = $this->stream->readString();
                $tableName = $this->stream->readString();

                for ($i = 0; $i < $columnsCount; ++$i) {
                    $columns[] = new ColumnInfo(
                        keyspace: $keyspace,
                        tableName: $tableName,
                        name: $this->stream->readString(),
                        type: $this->stream->readType(),
                    );
                }
            } else {
                for ($i = 0; $i < $columnsCount; ++$i) {
                    $columns[] = new ColumnInfo(
                        keyspace: $this->stream->readString(),
                        tableName: $this->stream->readString(),
                        name: $this->stream->readString(),
                        type: $this->stream->readType(),
                    );
                }
            }
        } else {
            $columns = null;
        }

        return new Metadata(
            flags: $flags,
            columnsCount: $columnsCount,
            newMetadataId: $newMetadataId,
            pagingState: $pagingState,
            pkCount: $pkCount,
            pkIndex: $pkIndex,
            columns: $columns,
        );
    }
}
