<?php

declare(strict_types=1);

namespace Cassandra\Exception\ServerException;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Response\Error\Context\UnpreparedContext;

final class UnpreparedException extends ServerException {
    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public function getErrorContext(): UnpreparedContext {

        if (!$this->errorContext instanceof UnpreparedContext) {
            throw new ResponseException(
                'Error context is not a UnpreparedContext',
                ExceptionCode::RESPONSE_ERROR_CONTEXT_INVALID_TYPE->value,
                [
                    'error_type' => get_class($this->errorContext),
                ],
            );
        }

        return $this->errorContext;
    }
}
