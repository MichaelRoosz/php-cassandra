<?php

declare(strict_types=1);

namespace Cassandra\Response\Event;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Response\Event;
use Cassandra\Response\Event\Data\EventData;
use Cassandra\Response\Event\Data\TopologyChangeData;
use Cassandra\Response\Event\Data\TopologyChangeType;
use Cassandra\Exception\ResponseException;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class TopologyChangeEvent extends Event {
    private TopologyChangeData $data;

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

    public function getTopologyChangeData(): TopologyChangeData {
        return $this->data;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readData(): TopologyChangeData {

        $changeTypeAsString = $this->stream->readString();

        try {
            $changeType = TopologyChangeType::from($changeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid topology change type: ' . $changeTypeAsString, ExceptionCode::RESPONSE_EVENT_TOPOLOGY_CHANGE_INVALID_TYPE->value, [
                'topology_change_type' => $changeTypeAsString,
            ], $e);
        }

        $address = $this->stream->readInet();

        return new TopologyChangeData(
            changeType: $changeType,
            address: $address,
        );
    }
}
