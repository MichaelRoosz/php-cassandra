<?php

declare(strict_types=1);

namespace Cassandra\Response;

class AuthSuccess extends Response {
    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getData(): ?string {
        /**
         * Indicates that the server is ready to process queries. This message will be
         * sent by the server either after a STARTUP message if no authentication is
         * required, or after a successful CREDENTIALS message.
         */
        $this->_stream->offset(0);

        return $this->_stream->readBytes();
    }
}
