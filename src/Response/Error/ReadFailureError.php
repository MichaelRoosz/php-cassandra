<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\ReadFailureContext;
use Cassandra\Response\StreamReader;

final class ReadFailureError extends Error {
    private ReadFailureContext $context;

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
    public function getContext(): ReadFailureContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readContext(): ReadFailureContext {

        $consistency = $this->stream->readConsistency();
        $nodesAnswered = $this->stream->readInt();
        $nodesRequired = $this->stream->readInt();

        if ($this->getVersion() >= 5) {
            $reasonMap = $this->stream->readReasonMap();
            $numFailures = null;
        } else {
            $reasonMap = null;
            $numFailures = $this->stream->readInt();
        }

        $dataPresent = $this->stream->readByte();

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
