<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayIterator;
use Cassandra\Protocol\Header;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\PreparedData;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;
use Iterator;

final class PreparedResult extends Result {
    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getData(): ResultData {
        return $this->getPreparedData();
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getIterator(): Iterator {
        return new ArrayIterator([
            'id' => $this->getPreparedData()->id,
            'result_metadata_id' => $this->getPreparedData()->resultMetadataId,
            'metadata' => $this->getPreparedData()->metadata,
            'result_metadata' => $this->getPreparedData()->resultMetadata,
        ]);
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getPreparedData(): PreparedData {

        if ($this->kind !== ResultKind::PREPARED) {
            throw new Exception('Unexpected result kind: ' . $this->kind->name);
        }

        $this->stream->offset(4);

        if ($this->getVersion() >= 5) {
            $data = new PreparedData(
                id: $this->stream->readString(),
                resultMetadataId: $this->stream->readString(),
                metadata: $this->readMetadata(isPrepareMetaData: true),
                resultMetadata: $this->readMetadata(isPrepareMetaData: false),
            );
        } else {
            $data = new PreparedData(
                id: $this->stream->readString(),
                metadata: $this->readMetadata(isPrepareMetaData: true),
                resultMetadata: $this->readMetadata(isPrepareMetaData: false),
            );
        }

        return $data;
    }
}
