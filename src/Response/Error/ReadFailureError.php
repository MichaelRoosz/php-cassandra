<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Consistency;
use Cassandra\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\ReadFailureContext;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class ReadFailureError extends Error {
    private ReadFailureContext $context;

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
    public function getContext(): ReadFailureContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readContext(): ReadFailureContext {

        $consistencyAsInt = $this->stream->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid consistency: ' . $consistencyAsInt, ExceptionCode::RESPONSE_READ_FAILURE_INVALID_CONSISTENCY->value, [
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

        $dataPresent = $this->stream->readChar();

        return new ReadFailureContext(
            consistency: $consistency,
            nodesAnswered: $nodesAnswered,
            nodesRequired: $nodesRequired,
            reasonMap: $reasonMap,
            numFailures: $numFailures,
            dataPresent: $dataPresent,
        );
    }
}
