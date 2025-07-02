<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Type;
use TypeError;
use ValueError;

final class Event extends Response {
    protected ?EventType $type = null;

    /**
     * @return array{
     *  event_type: EventType,
     *  change_type: string,
     *  address: string,
     * }|array{
     *  event_type: EventType,
     *  change_type: string,
     *  target: string,
     *  keyspace: string,
     *  name?: string,
     *  argument_types?: string[]
     * }
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getData(): array {
        $this->stream->offset(0);

        $typeString = $this->stream->readString();

        try {
            $type = EventType::from($typeString);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid event type: ' . $typeString, 0, [
                'event_type' => $typeString,
            ]);
        }

        $this->type = $type;

        switch ($type) {
            case EventType::TOPOLOGY_CHANGE:
            case EventType::STATUS_CHANGE:
                return [
                    'event_type' => $type,
                    'change_type' => $this->stream->readString(),
                    'address' => $this->stream->readInet(),
                ];

            case EventType::SCHEMA_CHANGE:
                $data = [
                    'event_type' => $type,
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

                        $argument_types = $this->stream->readTextList();
                        $data['argument_types'] = $argument_types;

                        break;
                }

                return $data;

            default:
                throw new Exception('Invalid event type: ' . $type);
        }
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getType(): EventType {
        if ($this->type === null) {
            $this->stream->offset(0);
            $typeString = $this->stream->readString();

            try {
                $this->type = EventType::from($typeString);
            } catch (ValueError|TypeError $e) {
                throw new Exception('Invalid event type: ' . $typeString, 0, [
                    'event_type' => $typeString,
                ]);
            }
        }

        return $this->type;
    }
}
