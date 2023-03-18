<?php

declare(strict_types=1);

namespace Cassandra\Response;

class Authenticate extends Response
{
    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getData(): string
    {
        return $this->_stream->readString();
    }
}
