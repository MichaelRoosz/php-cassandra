<?php

declare(strict_types=1);

namespace Cassandra\Exception\ServerException;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Response\Error\Context\WriteTimeoutContext;

final class WriteTimeoutException extends ServerException {
    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public function getErrorContext(): WriteTimeoutContext {

        if (!$this->errorContext instanceof WriteTimeoutContext) {
            throw new ResponseException(
                'Error context is not a WriteTimeoutContext',
                ExceptionCode::RESPONSE_ERROR_CONTEXT_INVALID_TYPE->value,
                [
                    'error_type' => get_class($this->errorContext),
                ],
            );
        }

        return $this->errorContext;
    }
}
