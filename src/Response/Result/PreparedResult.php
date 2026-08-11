<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayIterator;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Exception\ResponseException;
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function readPreparedData(): PreparedData {

        if ($this->kind !== ResultKind::PREPARED) {
            throw new ResponseException('Unexpected result kind: ' . $this->kind->name, ExceptionCode::RESPONSE_PREPARED_UNEXPECTED_KIND->value, [
                'operation' => 'PreparedResult::getPreparedData',
                'expected' => ResultKind::PREPARED->name,
                'received' => $this->kind->name,
            ]);
        }

        $this->stream->offset(4);

        if ($this->getProtocolVersion()->value >= ProtocolVersion::V5->value) {
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function readPrepareMetadata(): PrepareMetadata {
        $flags = $this->stream->readInt();
        $bindMarkersCount = $this->stream->readInt();

        if ($bindMarkersCount < 0 || $bindMarkersCount > self::MAX_EAGER_METADATA_ENTRIES) {
            throw new ResponseException(
                'Invalid prepared metadata bind marker count',
                ExceptionCode::RESPONSE_PREPARED_INVALID_BIND_MARKER_COUNT->value,
                [
                    'operation' => 'PreparedResult::readPrepareMetadata',
                    'bind_markers_count' => $bindMarkersCount,
                    'maximum_count' => self::MAX_EAGER_METADATA_ENTRIES,
                ]
            );
        }

        if ($this->getProtocolVersion()->value >= ProtocolVersion::V4->value) {
            $pkCount = $this->stream->readInt();

            $maximumPkCount = min(
                $bindMarkersCount,
                intdiv($this->stream->remainingLength(), 2),
            );

            if ($pkCount < 0 || $pkCount > $maximumPkCount) {
                throw new ResponseException(
                    'Invalid prepared metadata partition key count',
                    ExceptionCode::RESPONSE_PREPARED_INVALID_PK_COUNT->value,
                    [
                        'operation' => 'PreparedResult::readPrepareMetadata',
                        'pk_count' => $pkCount,
                        'maximum_count' => $maximumPkCount,
                        'bind_markers_count' => $bindMarkersCount,
                        'remaining_body_length' => $this->stream->remainingLength(),
                    ]
                );
            }

            $pkIndex = [];
            $seenPkIndexes = [];

            if ($pkCount > 0) {
                for ($i = 0; $i < $pkCount; ++$i) {
                    $index = $this->stream->readShort();
                    if ($index >= $bindMarkersCount || isset($seenPkIndexes[$index])) {
                        throw new ResponseException(
                            'Invalid prepared metadata partition key index',
                            ExceptionCode::RESPONSE_PREPARED_INVALID_PK_INDEX->value,
                            [
                                'operation' => 'PreparedResult::readPrepareMetadata',
                                'pk_position' => $i,
                                'pk_index' => $index,
                                'bind_markers_count' => $bindMarkersCount,
                                'duplicate' => isset($seenPkIndexes[$index]),
                            ]
                        );
                    }

                    $seenPkIndexes[$index] = true;
                    $pkIndex[] = $index;
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
            $this->assertCountFitsRemainingBody(
                count: $bindMarkersCount,
                maximumCount: intdiv($this->stream->remainingLength(), 4),
                minimumBytesPerEntry: 4,
                code: ExceptionCode::RESPONSE_PREPARED_INVALID_BIND_MARKER_COUNT,
                message: 'Prepared metadata bind marker count does not fit in the response body',
                operation: 'PreparedResult::readPrepareMetadata',
                field: 'bind_markers_count',
            );

            for ($i = 0; $i < $bindMarkersCount; ++$i) {
                $bindMarkers[] = new ColumnInfo(
                    keyspace: $keyspace,
                    tableName: $tableName,
                    name: $this->stream->readString(),
                    type: $this->stream->readTypeInfo(),
                );
            }
        } else {
            $this->assertCountFitsRemainingBody(
                count: $bindMarkersCount,
                maximumCount: intdiv($this->stream->remainingLength(), 8),
                minimumBytesPerEntry: 8,
                code: ExceptionCode::RESPONSE_PREPARED_INVALID_BIND_MARKER_COUNT,
                message: 'Prepared metadata bind marker count does not fit in the response body',
                operation: 'PreparedResult::readPrepareMetadata',
                field: 'bind_markers_count',
            );

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
