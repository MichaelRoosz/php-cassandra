<?php

declare(strict_types=1);

namespace Cassandra\Response\Event;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Response\Event;
use Cassandra\Response\Event\Data\EventData;
use Cassandra\Response\Event\Data\StatusChangeData;
use Cassandra\Response\Event\Data\StatusChangeType;
use Cassandra\Exception\ResponseException;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class StatusChangeEvent extends Event {
    private StatusChangeData $data;

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

    public function getStatusChangeData(): StatusChangeData {
        return $this->data;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readData(): StatusChangeData {

        $changeTypeAsString = $this->stream->readString();

        try {
            $changeType = StatusChangeType::from($changeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid status change type: ' . $changeTypeAsString, ExceptionCode::RESPONSE_EVENT_STATUS_CHANGE_INVALID_TYPE->value, [
                'status_change_type' => $changeTypeAsString,
            ], $e);
        }

        $address = $this->stream->readInet();

        return new StatusChangeData(
            changeType: $changeType,
            address: $address,
        );
    }
}
