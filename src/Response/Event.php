<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Protocol\Header;
use Cassandra\Response\Event\Data\EventData;
use TypeError;
use ValueError;

abstract class Event extends Response {
    public const EVENT_RESPONSE_CLASS_MAP = [
        EventType::SCHEMA_CHANGE->value => Event\SchemaChangeEvent::class,
        EventType::STATUS_CHANGE->value => Event\StatusChangeEvent::class,
        EventType::TOPOLOGY_CHANGE->value => Event\TopologyChangeEvent::class,
    ];

    protected EventType $type;

    public function __construct(Header $header, StreamReader $stream) {
        parent::__construct($header, $stream);

        $this->type = $this->readType();
    }

    abstract public function getData(): EventData;

    public function getType(): EventType {
        return $this->type;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readType(): EventType {

        $this->stream->offset(0);
        $typeString = $this->stream->readString();

        try {
            return EventType::from($typeString);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid event type: ' . $typeString, Exception::EVENT_INVALID_TYPE, [
                'event_type' => $typeString,
            ]);
        }
    }
}
