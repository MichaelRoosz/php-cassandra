<?php

declare(strict_types=1);

namespace Cassandra\Response\Event;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Header;
use Cassandra\Response\Event;
use Cassandra\Response\Event\Data\EventData;
use Cassandra\Response\Event\Data\SchemaChangeData;
use Cassandra\Response\Event\Data\SchemaChangeTarget;
use Cassandra\Response\Event\Data\SchemaChangeType;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class SchemaChangeEvent extends Event {
    private SchemaChangeData $data;

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );

        $this->data = $this->readData();
    }

    #[\Override]
    public function getData(): EventData {
        return $this->data;
    }

    public function getSchemaChangeData(): SchemaChangeData {
        return $this->data;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readData(): SchemaChangeData {

        $changeTypeAsString = $this->stream->readString();

        try {
            $changeType = SchemaChangeType::from($changeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid schema change type: ' . $changeTypeAsString, ExceptionCode::RESPONSE_EVENT_SCHEMA_CHANGE_INVALID_TYPE->value, [
                'schema_change_type' => $changeTypeAsString,
            ], $e);
        }

        $targetAsString = $this->stream->readString();

        try {
            $target = SchemaChangeTarget::from($targetAsString);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid schema change target: ' . $targetAsString, ExceptionCode::RESPONSE_EVENT_SCHEMA_CHANGE_INVALID_TARGET->value, [
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
                throw new ResponseException(
                    message: 'Invalid schema change target: ' . $target->value,
                    code: ExceptionCode::RESPONSE_EVENT_SCHEMA_CHANGE_UNEXPECTED_TARGET_VALUE->value,
                    context: [
                        'schema_change_target' => $target->value,
                    ]
                );
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
