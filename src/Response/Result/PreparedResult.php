<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayIterator;
use Cassandra\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\PreparedData;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\ResultFlag;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;
use Iterator;

class PreparedResult extends Result {
    protected PreparedData $preparedData;
    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );

        $this->preparedData = $this->readPreparedData();
    }

    public function getData(): ResultData {
        return $this->preparedData;
    }

    #[\Override]
    public function getIterator(): Iterator {
        return new ArrayIterator([
            'id' => $this->preparedData->id,
            'rowsMetadataId' => $this->preparedData->rowsMetadataId,
            'prepareMetadata' => $this->preparedData->prepareMetadata,
            'rowsMetadata' => $this->preparedData->rowsMetadata,
        ]);
    }

    public function getPreparedData(): PreparedData {
        return $this->preparedData;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    private function readPreparedData(): PreparedData {

        if ($this->kind !== ResultKind::PREPARED) {
            throw new Exception('Unexpected result kind: ' . $this->kind->name, ExceptionCode::RESPONSE_PREPARED_UNEXPECTED_KIND->value, [
                'operation' => 'PreparedResult::getPreparedData',
                'expected' => ResultKind::PREPARED->name,
                'received' => $this->kind->name,
            ]);
        }

        $this->stream->offset(4);

        if ($this->getVersion() >= 5) {
            $data = new PreparedData(
                id: $this->stream->readShortBytes(),
                rowsMetadataId: $this->stream->readShortBytes(),
                prepareMetadata: $this->readPrepareMetadata(),
                rowsMetadata: $this->readRowsMetadata(),
            );
        } else {
            $data = new PreparedData(
                id: $this->stream->readShortBytes(),
                prepareMetadata: $this->readPrepareMetadata(),
                rowsMetadata: $this->readRowsMetadata(),
            );
        }

        return $data;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    private function readPrepareMetadata(): PrepareMetadata {
        $flags = $this->stream->readInt();
        $bindMarkersCount = $this->stream->readInt();

        if ($this->getVersion() >= 4) {
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

        $bindMarkers = [];

        if ($flags & ResultFlag::ROWS_FLAG_GLOBAL_TABLES_SPEC) {
            $keyspace = $this->stream->readString();
            $tableName = $this->stream->readString();

            for ($i = 0; $i < $bindMarkersCount; ++$i) {
                $bindMarkers[] = new ColumnInfo(
                    keyspace: $keyspace,
                    tableName: $tableName,
                    name: $this->stream->readString(),
                    type: $this->stream->readTypeInfo(),
                );
            }
        } else {
            for ($i = 0; $i < $bindMarkersCount; ++$i) {
                $bindMarkers[] = new ColumnInfo(
                    keyspace: $this->stream->readString(),
                    tableName: $this->stream->readString(),
                    name: $this->stream->readString(),
                    type: $this->stream->readTypeInfo(),
                );
            }
        }

        return new PrepareMetadata(
            flags: $flags,
            bindMarkersCount: $bindMarkersCount,
            bindMarkers: $bindMarkers,
            pkCount: $pkCount,
            pkIndex: $pkIndex,
        );
    }
}
