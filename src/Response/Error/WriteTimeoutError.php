<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Consistency;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\WriteTimeoutContext;
use Cassandra\Response\Error\Context\WriteType;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class WriteTimeoutError extends Error {
    private WriteTimeoutContext $context;

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function __construct(Header $header, StreamReader $stream) {

        parent::__construct(
            header: $header,
            stream: $stream,
        );

        $this->context = $this->readContext();
    }

    #[\Override]
    public function getContext(): WriteTimeoutContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readContext(): WriteTimeoutContext {

        $consistencyAsInt = $this->stream->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid consistency: ' . $consistencyAsInt, 0, [
                'consistency' => $consistencyAsInt,
            ]);
        }

        $nodesAcknowledged = $this->stream->readInt();
        $nodesRequired = $this->stream->readInt();
        $writeTypeAsString = $this->stream->readString();

        try {
            $writeType = WriteType::from($writeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid write type: ' . $writeTypeAsString, 0, [
                'write_type' => $writeTypeAsString,
            ]);
        }

        if ($this->getVersion() >= 5) {
            $contentions = $this->stream->readShort();
        } else {
            $contentions = null;
        }

        return new WriteTimeoutContext(
            consistency: $consistency,
            nodesAcknowledged: $nodesAcknowledged,
            nodesRequired: $nodesRequired,
            writeType: $writeType,
            contentions: $contentions,
        );
    }
}
