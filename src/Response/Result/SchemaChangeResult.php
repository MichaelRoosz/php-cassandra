<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayIterator;
use Cassandra\Protocol\Header;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\SchemaChangeData;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;
use Iterator;

final class SchemaChangeResult extends Result {
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
        return $this->getSchemaChangeData();
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getIterator(): Iterator {
        return new ArrayIterator([
            'change_type' => $this->getSchemaChangeData()->changeType,
            'target' => $this->getSchemaChangeData()->target,
            'keyspace' => $this->getSchemaChangeData()->keyspace,
            'name' => $this->getSchemaChangeData()->name,
            'argument_types' => $this->getSchemaChangeData()->argumentTypes,
        ]);
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getSchemaChangeData(): SchemaChangeData {
        if ($this->kind !== ResultKind::SCHEMA_CHANGE) {
            throw new Exception('Unexpected result kind: ' . $this->kind->name);
        }

        $this->stream->offset(4);

        $data = [
            'change_type' => $this->stream->readString(),
            'target' => $this->stream->readString(),
            'keyspace' => $this->stream->readString(),
        ];

        switch ($data['target']) {
            case 'TABLE':
            case 'TYPE':
                $data['name'] = $this->stream->readString();

                break;

            case 'FUNCTION':
            case 'AGGREGATE':
                $data['name'] = $this->stream->readString();
                $data['argument_types'] = $this->stream->readTextList();

                break;
        }

        return new SchemaChangeData(
            changeType: $data['change_type'],
            target: $data['target'],
            keyspace: $data['keyspace'],
            name: $data['name'] ?? null,
            argumentTypes: $data['argument_types'] ?? null,
        );
    }
}
