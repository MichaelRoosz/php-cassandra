<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\FunctionFailureContext;
use Cassandra\Response\StreamReader;

final class FunctionFailureError extends Error {
    private FunctionFailureContext $context;

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
    public function getContext(): FunctionFailureContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readContext(): FunctionFailureContext {

        $keyspace = $this->stream->readString();
        $function = $this->stream->readString();
        $argTypes = $this->stream->readStringList();

        return new FunctionFailureContext(
            keyspace: $keyspace,
            function: $function,
            argTypes: $argTypes,
        );
    }
}
