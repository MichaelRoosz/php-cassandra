<?php

declare(strict_types=1);

namespace Cassandra\Response;

class AuthChallenge extends Response
{
    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getData(): ?string
    {
        return $this->_stream->readBytes();
    }
}
