<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;

class RequestOptions {
    /**
     * Reject a request timeout no caller can have meant.
     *
     * Zero or less would put the request out of time before it was sent, which
     * is the same judgement {@see \Cassandra\Connection\ConnectionOptions}
     * makes about its own default and {@see \Cassandra\Connection::setRequestTimeout()}
     * about the value it is handed. "Use the connection default" is spelled
     * null.
     *
     * INF passes, and asks for a request that waits for as long as it takes —
     * the same wait a null connection default gives, spelled per request so that
     * one statement can outlast the default without changing what it means for
     * everything else. {@see \Cassandra\Connection\Deadline::at()} normalises it
     * to that unbounded wait.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    final protected static function assertValidRequestTimeout(?float $requestTimeoutInSeconds): void {

        if ($requestTimeoutInSeconds === null || $requestTimeoutInSeconds > 0.0) {
            return;
        }

        throw new RequestException(
            'Invalid request timeout: it must be greater than zero, or null to use the connection default',
            ExceptionCode::REQUEST_INVALID_REQUEST_TIMEOUT->value,
            [
                'request_timeout_seconds' => $requestTimeoutInSeconds,
            ]
        );
    }
}
