<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayIterator;
use Cassandra\Protocol\Header;
use Cassandra\Response\Event\Data\SchemaChangeTarget;
use Cassandra\Response\Event\Data\SchemaChangeType;
use Cassandra\Response\Exception;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\ResultData;
use Cassandra\Response\Result\Data\SchemaChangeData;
use Cassandra\Response\ResultKind;
use Cassandra\Response\StreamReader;
use Iterator;
use TypeError;
use ValueError;

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

        $changeTypeAsString =$this->stream->readString();

        try {
            $changeType = SchemaChangeType::from($changeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid schema change type: ' . $changeTypeAsString, 0, [
                'schema_change_type' => $changeTypeAsString,
            ]);
        }

        $targetAsString = $this->stream->readString();

        try {
            $target = SchemaChangeTarget::from($targetAsString);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid schema change target: ' . $targetAsString, 0, [
                'schema_change_target' => $targetAsString,
            ]);
        }

        $keyspace = $this->stream->readString();

        $argumentTypes = null;

        switch ($target) {
            case SchemaChangeTarget::TABLE:
            case SchemaChangeTarget::TYPE:
                $name = $this->stream->readString();

                break;

            case SchemaChangeTarget::FUNCTION:
            case SchemaChangeTarget::AGGREGATE:
                $name = $this->stream->readString();
                $argumentTypes = $this->stream->readTextList();

                break;

            default:
                throw new Exception('Invalid schema change target: ' . $target->value);
        }

        return new SchemaChangeData(
            changeType: $changeType,
            target: $target,
            keyspace: $keyspace,
            name: $name,
            argumentTypes: $argumentTypes,
        );
    }
}
