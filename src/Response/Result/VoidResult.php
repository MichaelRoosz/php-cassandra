<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use Cassandra\Protocol\Header;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\VoidData;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;
use EmptyIterator;
use Iterator;

final class VoidResult extends Result {
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
     */
    public function getData(): ResultData {
        return $this->getVoidData();
    }

    #[\Override]
    public function getIterator(): Iterator {
        return new EmptyIterator();
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getVoidData(): VoidData {
        if ($this->kind !== ResultKind::VOID) {
            throw new Exception('Unexpected result kind: ' . $this->kind->name, Exception::VOID_UNEXPECTED_KIND, [
                'operation' => 'VoidResult::getVoidData',
                'expected' => ResultKind::VOID->name,
                'received' => $this->kind->name,
            ]);
        }

        $this->stream->offset(4);

        return new VoidData();
    }
}
