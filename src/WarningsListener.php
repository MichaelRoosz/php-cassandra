<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Request\Request;
use Cassandra\Response\Response;

interface WarningsListener {
    /**
     * Called from inside whichever read took the response off the wire, so a
     * listener that blocks holds up the call it was invoked from.
     *
     * @param array<string> $warnings
     *
     * @throws \Throwable an implementation is application code and may throw
     * anything. It costs neither the connection nor the request being resolved:
     * {@see \Cassandra\Connection\ListenerRegistry} catches it, lets the
     * remaining listeners run, and reports it to whoever was reading once the
     * response has been put on its statement — as a
     * {@see \Cassandra\Exception\ConnectionException} wrapping it.
     */
    public function onWarnings(array $warnings, Request $request, Response $response): void;
}
