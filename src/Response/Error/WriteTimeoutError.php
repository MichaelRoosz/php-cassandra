<?php

declare(strict_types=1);

namespace Cassandra\Response\Event;

use Cassandra\Consistency;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\WriteTimeoutContext;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class WriteTimeoutError extends Error {
    private WriteTimeoutContext $context;

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
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
     * @throws \Cassandra\Type\Exception
     */
    protected function readContext(): WriteTimeoutContext {

        $consistency = $this->stream->readShort();

        try {
            $consistency = Consistency::from($consistency);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid consistency: ' . $consistency, 0, [
                'consistency' => $consistency,
            ]);
        }

        $nodesAcknowledged = $this->stream->readInt();
        $nodesRequired = $this->stream->readInt();
        $writeType = $this->stream->readString();

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
