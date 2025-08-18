<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Consistency;
use Cassandra\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\WriteFailureContext;
use Cassandra\Response\Error\Context\WriteType;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class WriteFailureError extends Error {
    private WriteFailureContext $context;

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
    public function getContext(): WriteFailureContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readContext(): WriteFailureContext {

        $consistencyAsInt = $this->stream->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid consistency: ' . $consistencyAsInt, ExceptionCode::RESPONSE_WRITE_FAILURE_INVALID_CONSISTENCY->value, [
                'consistency' => $consistencyAsInt,
            ], $e);
        }

        $nodesAnswered = $this->stream->readInt();
        $nodesRequired = $this->stream->readInt();

        if ($this->getVersion() >= 5) {
            $reasonMap = $this->stream->readReasonMap();
            $numFailures = null;
        } else {
            $reasonMap = null;
            $numFailures = $this->stream->readInt();
        }

        $writeTypeAsString = $this->stream->readString();

        try {
            $writeType = WriteType::from($writeTypeAsString);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid write type: ' . $writeTypeAsString, ExceptionCode::RESPONSE_WRITE_FAILURE_INVALID_WRITE_TYPE->value, [
                'write_type' => $writeTypeAsString,
            ], $e);
        }

        return new WriteFailureContext(
            consistency: $consistency,
            nodesAnswered: $nodesAnswered,
            nodesRequired: $nodesRequired,
            reasonMap: $reasonMap,
            numFailures: $numFailures,
            writeType: $writeType,
        );
    }
}
