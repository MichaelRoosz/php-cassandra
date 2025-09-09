<?php

declare(strict_types=1);

namespace Cassandra\Exception\ServerException;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Response\Error\Context\FunctionFailureContext;

final class FunctionFailureException extends ServerException {
    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public function getErrorContext(): FunctionFailureContext {

        if (!$this->errorContext instanceof FunctionFailureContext) {
            throw new ResponseException(
                'Error context is not a FunctionFailureContext',
                ExceptionCode::RESPONSE_ERROR_CONTEXT_INVALID_TYPE->value,
                [
                    'error_type' => get_class($this->errorContext),
                ],
            );
        }

        return $this->errorContext;
    }
}
