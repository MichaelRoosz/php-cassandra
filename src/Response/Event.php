<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Type;

class Event extends Response {
    public const SCHEMA_CHANGE = 'SCHEMA_CHANGE';
    public const STATUS_CHANGE = 'STATUS_CHANGE';
    public const TOPOLOGY_CHANGE = 'TOPOLOGY_CHANGE';

    protected ?string $type = null;

    /**
     * @return array{
     *  event_type: string,
     *  change_type: string,
     *  address: string,
     * }|array{
     *  event_type: string,
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
        $type = $this->type = $this->stream->readString();

        switch($type) {
            case self::TOPOLOGY_CHANGE:
            case self::STATUS_CHANGE:
                return [
                    'event_type' => $type,
                    'change_type' => $this->stream->readString(),
                    'address' => $this->stream->readInet(),
                ];

            case self::SCHEMA_CHANGE:
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

                        /** @var string[] $argument_types */
                        $argument_types = $this->stream->readList([Type::TEXT]);
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
    public function getType(): string {
        if ($this->type === null) {
            $this->stream->offset(0);
            $this->type = $this->stream->readString();
        }

        return $this->type;
    }
}
