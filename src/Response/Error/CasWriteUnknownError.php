<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Consistency;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\CasWriteUnknownContext;
use Cassandra\Response\Exception;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class CasWriteUnknownError extends Error {
    private CasWriteUnknownContext $context;

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
    public function getContext(): CasWriteUnknownContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception    
     */
    protected function readContext(): CasWriteUnknownContext {

        $consistencyAsInt = $this->stream->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid consistency: ' . $consistencyAsInt, 0, [
                'consistency' => $consistencyAsInt,
            ]);
        }

        $nodesAcknowledged = $this->stream->readInt();
        $nodesRequired = $this->stream->readInt();

        return new CasWriteUnknownContext(
            consistency: $consistency,
            nodesAcknowledged: $nodesAcknowledged,
            nodesRequired: $nodesRequired,
        );
    }
}
