<?php

declare(strict_types=1);

namespace Cassandra\Exception;

use Cassandra\Response\Error\Context\ErrorContext;
use Throwable;

class ServerException extends CassandraException {
    /**
     * @var array{
     *   error_code: int,
     *   error_type: string,
     *   protocol_version: int,
     *   stream_id: int,
     *   tracing_uuid: string|null,
     *   warnings: array<string>,
     *   payload: array<string, ?string>|null,
     * } $baseContext
     */
    protected readonly array $baseContext;

    protected readonly ErrorContext $errorContext;

    /**
     * @param array{
     *   error_code: int,
     *   error_type: string,
     *   protocol_version: int,
     *   stream_id: int,
     *   tracing_uuid: string|null,
     *   warnings: array<string>,
     *   payload: array<string, ?string>|null,
     * } $context
     */
    public function __construct(
        ErrorContext $errorContext,
        string $message,
        int $code,
        array $context,
        ?Throwable $previous = null
    ) {
        parent::__construct($message, $code, $context, $previous);
        $this->baseContext = $context;
        $this->errorContext = $errorContext;
    }

    /**
     * @return array{
     *   error_code: int,
     *   error_type: string,
     *   protocol_version: int,
     *   stream_id: int,
     *   tracing_uuid: string|null,
     *   warnings: array<string>,
     *   payload: array<string, ?string>|null,
     * }
     */
    #[\Override]
    public function getContext(): array {
        return $this->baseContext;
    }

    public function getErrorContext(): ErrorContext {
        return $this->errorContext;
    }
}
