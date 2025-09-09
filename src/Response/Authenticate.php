<?php

declare(strict_types=1);

namespace Cassandra\Response;

final class Authenticate extends Response {
    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    public function getData(): string {
        $this->stream->offset(0);

        return $this->stream->readString();
    }
}
