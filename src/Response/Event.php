<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Type;

class Event extends Response {
    public const TOPOLOGY_CHANGE = 'TOPOLOGY_CHANGE';
    public const STATUS_CHANGE = 'STATUS_CHANGE';
    public const SCHEMA_CHANGE = 'SCHEMA_CHANGE';

    protected ?string $_type = null;

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getType(): string {
        if ($this->_type === null) {
            $this->_stream->offset(0);
            $this->_type = $this->_stream->readString();
        }

        return $this->_type;
    }

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
        $this->_stream->offset(0);
        $type = $this->_type = $this->_stream->readString();

        switch($type) {
            case self::TOPOLOGY_CHANGE:
            case self::STATUS_CHANGE:
                return [
                    'event_type' => $type,
                    'change_type' => $this->_stream->readString(),
                    'address' => $this->_stream->readInet(),
                ];

            case self::SCHEMA_CHANGE:
                $data = [
                    'event_type' => $type,
                    'change_type' => $this->_stream->readString(),
                    'target' => $this->_stream->readString(),
                    'keyspace' => $this->_stream->readString(),
                ];

                switch ($data['target']) {
                    case 'TABLE':
                    case 'TYPE':
                        $data['name'] = $this->_stream->readString();

                        break;

                    case 'FUNCTION':
                    case 'AGGREGATE':
                        $data['name'] = $this->_stream->readString();

                        /** @var string[] $argument_types */
                        $argument_types = $this->_stream->readList([Type\Base::TEXT]);
                        $data['argument_types'] = $argument_types;

                        break;
                }

                return $data;

            default:
                throw new Exception('Invalid event type: ' . $type);
        }
    }
}
