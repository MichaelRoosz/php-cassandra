<?php

declare(strict_types=1);

namespace Cassandra\Exception;

class NodeException extends CassandraException {
    /**
     * Whether this exception is a transport read timeout, i.e. the connection
     * produced no data within its stall window.
     *
     * Such a timeout says nothing about the health of the connection: the
     * response reader keeps whatever it already consumed, so waiting longer is
     * always safe. Only the caller's own deadline can decide when to give up —
     * a slow query and an idle event stream both look exactly like this.
     */
    public function isReadTimeout(): bool {
        return $this->getCode() === ExceptionCode::SOCKET_TIMEOUT_DURING_READ->value
            || $this->getCode() === ExceptionCode::STREAM_TIMEOUT_DURING_READ->value;
    }
}
