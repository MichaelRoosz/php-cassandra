<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\ReadTimeoutContext;
use Cassandra\Response\StreamReader;

final class ReadTimeoutError extends Error {
    private ReadTimeoutContext $context;

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
    public function getContext(): ReadTimeoutContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readContext(): ReadTimeoutContext {

        $consistency = $this->stream->readConsistency();
        $nodesAnswered = $this->stream->readInt();
        $nodesRequired = $this->stream->readInt();
        $dataPresent = $this->stream->readByte() !== 0;

        return new ReadTimeoutContext(
            consistency: $consistency,
            nodesAnswered: $nodesAnswered,
            nodesRequired: $nodesRequired,
            dataPresent: $dataPresent,
        );
    }
}
