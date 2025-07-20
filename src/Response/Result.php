<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayObject;
use Cassandra\ColumnInfo;
use Cassandra\Metadata;
use Cassandra\Protocol\Header;
use Cassandra\Request\ExecuteCallInfo;
use Cassandra\Request\Request;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;
use IteratorAggregate;
use TypeError;
use ValueError;

/**
 * @implements IteratorAggregate<ArrayObject<string, mixed>|array<string, mixed>|null>
 */
abstract class Result extends Response implements IteratorAggregate {
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

    public function getKind(): ResultKind {
        return $this->kind;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getMetadata(): Metadata {
        throw new Exception('Result metadata for kind ' . $this->kind->name . ' is not available');
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

    public function setMetadata(Metadata $metadata): static {
        return $this;
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
            $this->metadataOfPreviousResult = $previousResult->getMetadata();
            $this->onPreviousResultUpdated();

            $lastExecuteCallInfo = $previousResult->getNextExecuteCallInfo();
            if ($lastExecuteCallInfo === null) {
                throw new Exception('prepared statement not found');
            }

            $lastMetadata = $previousResult->getMetadata();

            $resultMetadataId = $lastMetadata->newMetadataId ?? $lastExecuteCallInfo->resultMetadataId ?? null;

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
            throw new Exception('Invalid result kind: ' . $kindInt, 0, [
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
