<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Exception\ResponseException;
use Cassandra\Response\Result\Data\PreparedData;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;

final class CachedPreparedResult extends PreparedResult {
    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function __construct(Header $header, StreamReader $stream, ?PreparedData $preparedData = null) {

        if ($preparedData === null) {
            throw new ResponseException('Prepared data is required', ExceptionCode::RESPONSE_RES_PREPARED_DATA_REQUIRED->value, [
                'expected' => PreparedData::class,
                'received' => 'null',
            ]);
        }

        $this->header = $header;
        $this->stream = $stream;
        $this->dataOffset = 0;
        $this->kind = ResultKind::PREPARED;
        $this->preparedData = $preparedData;
    }
}
