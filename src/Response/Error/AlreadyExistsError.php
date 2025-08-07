<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\AlreadyExistsContext;
use Cassandra\Response\StreamReader;

final class AlreadyExistsError extends Error {
    private AlreadyExistsContext $context;

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
    public function getContext(): AlreadyExistsContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readContext(): AlreadyExistsContext {

        $keyspace = $this->stream->readString();
        $table = $this->stream->readString();

        return new AlreadyExistsContext(
            keyspace: $keyspace,
            table: $table,
        );
    }
}
