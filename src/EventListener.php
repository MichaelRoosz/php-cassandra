<?php

declare(strict_types=1);

namespace Cassandra;

interface EventListener {
    /**
     * Called from inside whichever read took the event off the wire, so a
     * listener that blocks holds up the call it was invoked from.
     *
     * @throws \Throwable an implementation is application code and may throw
     * anything. It costs neither the connection nor the request being resolved:
     * {@see \Cassandra\Connection\ListenerRegistry} catches it, lets the
     * remaining listeners run, and reports it to whoever was reading once the
     * connection has finished with the frame — as a
     * {@see \Cassandra\Exception\ConnectionException} wrapping it.
     */
    public function onEvent(Response\Event $event): void;
}
