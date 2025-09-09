<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Exception\ResponseException;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\VoidData;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;
use EmptyIterator;
use Iterator;

final class VoidResult extends Result {
    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    public function getData(): ResultData {
        return $this->getVoidData();
    }

    #[\Override]
    public function getIterator(): Iterator {
        return new EmptyIterator();
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    public function getVoidData(): VoidData {
        if ($this->kind !== ResultKind::VOID) {
            throw new ResponseException('Unexpected result kind: ' . $this->kind->name, ExceptionCode::RESPONSE_VOID_UNEXPECTED_KIND->value, [
                'operation' => 'VoidResult::getVoidData',
                'expected' => ResultKind::VOID->name,
                'received' => $this->kind->name,
            ]);
        }

        $this->stream->offset(4);

        return new VoidData();
    }
}
