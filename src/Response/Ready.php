<?php

declare(strict_types=1);

namespace Cassandra\Response;

final class Ready extends Response {
    public function getData(): null {
        /**
         * Indicates that the server is ready to process queries. This message will be
         * sent by the server either after a STARTUP message if no authentication is
         * required, or after a successful CREDENTIALS message.
         */
        return null;
    }
}
