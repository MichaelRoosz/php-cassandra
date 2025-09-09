<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\EventType;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Header;
use Cassandra\Response\Event\Data\EventData;
use TypeError;
use ValueError;

class Event extends Response {
    protected EventType $type;

    public function __construct(Header $header, StreamReader $stream) {
        parent::__construct($header, $stream);

        $this->type = $this->readType();
    }

    public function getData(): EventData {
        return new EventData();
    }

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<string, class-string<\Cassandra\Response\Event>>
     */
    public static function getEventClassMap(): array {
        return [
            EventType::SCHEMA_CHANGE->value => Event\SchemaChangeEvent::class,
            EventType::STATUS_CHANGE->value => Event\StatusChangeEvent::class,
            EventType::TOPOLOGY_CHANGE->value => Event\TopologyChangeEvent::class,
        ];
    }

    public function getType(): EventType {
        return $this->type;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readType(): EventType {

        $this->stream->offset(0);
        $typeString = $this->stream->readString();

        try {
            return EventType::from($typeString);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid event type: ' . $typeString, ExceptionCode::RESPONSE_EVENT_INVALID_TYPE->value, [
                'event_type' => $typeString,
            ], $e);
        }
    }
}
