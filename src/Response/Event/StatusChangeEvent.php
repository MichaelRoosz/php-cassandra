<?php

declare(strict_types=1);

namespace Cassandra\Response\Event;

use Cassandra\Protocol\Header;
use Cassandra\Response\Event;
use Cassandra\Response\Event\Data\EventData;
use Cassandra\Response\Event\Data\StatusChangeData;
use Cassandra\Response\Event\Data\StatusChangeType;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class StatusChangeEvent extends Event {
    private StatusChangeData $data;

    /**
     * @throws \Cassandra\Response\Exception
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
     * @throws \Cassandra\Response\Exception
     */
    protected function readData(): StatusChangeData {

        $changeTypeAsString = $this->stream->readString();

        try {
            $changeType = StatusChangeType::from($changeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid status change type: ' . $changeTypeAsString, Exception::EVENT_STATUS_CHANGE_INVALID_TYPE, [
                'status_change_type' => $changeTypeAsString,
            ]);
        }

        $address = $this->stream->readInet();

        return new StatusChangeData(
            changeType: $changeType,
            address: $address,
        );
    }
}
