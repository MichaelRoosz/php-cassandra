<?php

declare(strict_types=1);

namespace Cassandra\Response;

final class Supported extends Response {
    /**
     * @return array<string,string[]>
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    public function getData(): array {
        $this->stream->offset(0);

        return $this->stream->readStringMultimap();
    }
}
