<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

use ArrayIterator;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Header;
use Cassandra\Response\Event\Data\SchemaChangeTarget;
use Cassandra\Response\Event\Data\SchemaChangeType;
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
        return $this->getSchemaChangeData();
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
     */
    public function getSchemaChangeData(): SchemaChangeData {
        if ($this->kind !== ResultKind::SCHEMA_CHANGE) {
            throw new ResponseException('Unexpected result kind for schema change data', ExceptionCode::RESPONSE_SCHEMA_CHANGE_UNEXPECTED_KIND->value, [
                'operation' => 'SchemaChangeResult::getSchemaChangeData',
                'expected' => ResultKind::SCHEMA_CHANGE->name,
                'received' => $this->kind->name,
            ]);
        }

        $this->stream->offset(4);

        $changeTypeAsString =$this->stream->readString();

        try {
            $changeType = SchemaChangeType::from($changeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid schema change type', ExceptionCode::RESPONSE_SCHEMA_CHANGE_INVALID_TYPE->value, [
                'operation' => 'SchemaChangeResult::getSchemaChangeData',
                'schema_change_type' => $changeTypeAsString,
            ], $e);
        }

        $targetAsString = $this->stream->readString();

        try {
            $target = SchemaChangeTarget::from($targetAsString);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid schema change target', ExceptionCode::RESPONSE_SCHEMA_CHANGE_INVALID_TARGET->value, [
                'operation' => 'SchemaChangeResult::getSchemaChangeData',
                'schema_change_target' => $targetAsString,
            ], $e);
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
                $argumentTypes = $this->stream->readStringList();

                break;

            default:
                throw new ResponseException('Invalid schema change target', ExceptionCode::RESPONSE_SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE->value, [
                    'operation' => 'SchemaChangeResult::getSchemaChangeData',
                    'schema_change_target' => $target->value,
                ]);
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
