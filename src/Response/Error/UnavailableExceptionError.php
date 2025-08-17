<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Consistency;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\UnavailableExceptionContext;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class UnavailableExceptionError extends Error {
    private UnavailableExceptionContext $context;

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
    public function getContext(): UnavailableExceptionContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readContext(): UnavailableExceptionContext {

        $consistencyAsInt = $this->stream->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid consistency: ' . $consistencyAsInt, Exception::UNAVAILABLE_INVALID_CONSISTENCY, [
                'consistency' => $consistencyAsInt,
            ], $e);
        }

        $nodesRequired = $this->stream->readInt();
        $nodesAlive = $this->stream->readInt();

        return new UnavailableExceptionContext(
            consistency: $consistency,
            nodesRequired: $nodesRequired,
            nodesAlive: $nodesAlive,
        );
    }
}
