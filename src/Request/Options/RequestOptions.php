<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;

class RequestOptions {
    /**
     * Largest value the protocol's signed 32-bit `[int]` can carry.
     */
    final protected const INT32_MAX = 2147483647;

    /**
     * Smallest value the protocol's signed 32-bit `[int]` can carry.
     */
    final protected const INT32_MIN = -2147483647 - 1;

    /**
     * Reject a "now in seconds" the protocol cannot carry.
     *
     * The option goes out as a signed 32-bit `[int]`, and pack('N', …) takes the
     * low four bytes of whatever it is given without complaint — so a larger
     * value would reach the coordinator as a different timestamp altogether
     * rather than being refused: 2^31 arrives as -2^31, and 2^32 + 100 as 100.
     * The same reasoning as {@see \Cassandra\Request\Request::MAX_SHORT_COUNT}
     * and the int32 bound {@see \Cassandra\Request\Request::encodeQueryValuesAsBinary()}
     * holds bound values to, and it is refused here rather than at encoding time
     * so that it is reported against the value that named it.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    final protected static function assertValidNowInSeconds(?int $nowInSeconds): void {

        if ($nowInSeconds === null || ($nowInSeconds >= self::INT32_MIN && $nowInSeconds <= self::INT32_MAX)) {
            return;
        }

        throw new RequestException(
            'Invalid now_in_seconds: it must fit in a signed 32-bit integer, or be null to leave it to the server',
            ExceptionCode::REQUEST_INVALID_NOW_IN_SECONDS->value,
            [
                'now_in_seconds' => $nowInSeconds,
                'minimum' => self::INT32_MIN,
                'maximum' => self::INT32_MAX,
            ]
        );
    }

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
