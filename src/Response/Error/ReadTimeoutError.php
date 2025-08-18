<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Consistency;
use Cassandra\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\ReadTimeoutContext;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class ReadTimeoutError extends Error {
    private ReadTimeoutContext $context;

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
    public function getContext(): ReadTimeoutContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readContext(): ReadTimeoutContext {

        $consistencyAsInt = $this->stream->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid consistency: ' . $consistencyAsInt, ExceptionCode::RESPONSE_READ_TIMEOUT_INVALID_CONSISTENCY->value, [
                'consistency' => $consistencyAsInt,
            ], $e);
        }

        $nodesAnswered = $this->stream->readInt();
        $nodesRequired = $this->stream->readInt();
        $dataPresent = $this->stream->readChar() !== 0;

        return new ReadTimeoutContext(
            consistency: $consistency,
            nodesAnswered: $nodesAnswered,
            nodesRequired: $nodesRequired,
            dataPresent: $dataPresent,
        );
    }
}
