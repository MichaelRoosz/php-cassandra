<?php

declare(strict_types=1);

namespace Cassandra\Response\Error;

use Cassandra\Protocol\Header;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\UnpreparedContext;
use Cassandra\Response\StreamReader;

final class UnpreparedError extends Error {
    private UnpreparedContext $context;

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
    public function getContext(): UnpreparedContext {
        return $this->context;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readContext(): UnpreparedContext {

        $unknownStatementId = $this->stream->readString();

        return new UnpreparedContext(
            unknownStatementId: $unknownStatementId,
        );
    }
}
