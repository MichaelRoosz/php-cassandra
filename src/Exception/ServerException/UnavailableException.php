<?php

declare(strict_types=1);

namespace Cassandra\Exception\ServerException;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Response\Error\Context\UnavailableExceptionContext;

final class UnavailableException extends ServerException {
    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public function getErrorContext(): UnavailableExceptionContext {

        if (!$this->errorContext instanceof UnavailableExceptionContext) {
            throw new ResponseException(
                'Error context is not a UnavailableExceptionContext',
                ExceptionCode::RESPONSE_ERROR_CONTEXT_INVALID_TYPE->value,
                [
                    'error_type' => get_class($this->errorContext),
                ],
            );
        }

        return $this->errorContext;
    }
}
