<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\UnavailableExceptionContext;
use Cassandra\Response\StreamReader;

final class UnavailableExceptionError extends Error {
    private UnavailableExceptionContext $context;

    /**
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readContext(): UnavailableExceptionContext {

        $consistency = $this->stream->readConsistency();
        $nodesRequired = $this->stream->readInt();
        $nodesAlive = $this->stream->readInt();

        return new UnavailableExceptionContext(
            consistency: $consistency,
            nodesRequired: $nodesRequired,
            nodesAlive: $nodesAlive,
        );
    }
}
