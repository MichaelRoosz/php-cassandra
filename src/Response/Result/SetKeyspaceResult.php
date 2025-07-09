<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayIterator;
use Cassandra\Protocol\Header;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\SetKeyspaceData;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;
use Iterator;

final class SetKeyspaceResult extends Result {
    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );
    }

    public function getData(): ResultData {
        return $this->getSetKeyspaceData();
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getIterator(): Iterator {
        return new ArrayIterator([
            'keyspace' => $this->getSetKeyspaceData()->keyspace,
        ]);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getSetKeyspaceData(): SetKeyspaceData {
        if ($this->kind !== ResultKind::SET_KEYSPACE) {
            throw new Exception('Unexpected result kind: ' . $this->kind->name);
        }

        $this->stream->offset(4);

        return new SetKeyspaceData(
            keyspace: $this->stream->readString(),
        );
    }
}
