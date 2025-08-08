<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Protocol\Header;
use Cassandra\Response\Error\Context\ErrorContext;
use TypeError;
use ValueError;

/**
 * Indicates an error processing a request.
 */
abstract class Error extends Response {
    public const ERROR_RESPONSE_CLASS_MAP = [
        ErrorType::ALREADY_EXISTS->value => Error\AlreadyExistsError::class,
        ErrorType::AUTHENTICATION_ERROR->value => Error\AuthenticationError::class,
        ErrorType::CAS_WRITE_UNKNOWN->value => Error\CasWriteUnknownError::class,
        ErrorType::CDC_WRITE_FAILURE->value => Error\CdcWriteFailureError::class,
        ErrorType::CONFIG_ERROR->value => Error\ConfigError::class,
        ErrorType::FUNCTION_FAILURE->value => Error\FunctionFailureError::class,
        ErrorType::INVALID->value => Error\InvalidError::class,
        ErrorType::IS_BOOTSTRAPPING->value => Error\IsBootstrappingError::class,
        ErrorType::OVERLOADED->value => Error\OverloadedError::class,
        ErrorType::PROTOCOL_ERROR->value => Error\ProtocolError::class,
        ErrorType::READ_FAILURE->value => Error\ReadFailureError::class,
        ErrorType::READ_TIMEOUT->value => Error\ReadTimeoutError::class,
        ErrorType::SERVER_ERROR->value => Error\ServerError::class,
        ErrorType::SYNTAX_ERROR->value => Error\SyntaxError::class,
        ErrorType::TRUNCATE_ERROR->value => Error\TruncateError::class,
        ErrorType::UNAUTHORIZED->value => Error\UnauthorizedError::class,
        ErrorType::UNAVAILABLE_EXCEPTION->value => Error\UnavailableExceptionError::class,
        ErrorType::UNPREPARED->value => Error\UnpreparedError::class,
        ErrorType::WRITE_FAILURE->value => Error\WriteFailureError::class,
        ErrorType::WRITE_TIMEOUT->value => Error\WriteTimeoutError::class,
    ];

    protected readonly int $code;
    protected readonly string $message;
    protected readonly ErrorType $type;

    public function __construct(Header $header, StreamReader $stream) {
        parent::__construct($header, $stream);

        $data = $this->readData();

        $this->code = $data['code'];
        $this->message = $data['message'];
        $this->type = $data['type'];
    }

    public function getCode(): int {
        return $this->code;
    }

    public function getContext(): ErrorContext {
        return new ErrorContext();
    }

    public function getException(): Exception {
        $baseContext = [
            'error_code' => $this->code,
            'error_type' => $this->type->name,
            'protocol_version' => $this->getVersion(),
            'stream_id' => $this->getStream(),
            'tracing_uuid' => $this->getTracingUuid(),
            'warnings' => $this->getWarnings(),
            'payload' => $this->getPayload(),
        ];

        $message = sprintf('[%s %d] %s', $this->type->name, $this->code, $this->message);

        return new Exception(
            message: $message,
            code: $this->code,
            context: array_merge($baseContext, $this->getContext()->toArray()),
        );
    }

    public function getMessage(): string {
        return $this->message;
    }

    public function getType(): ErrorType {
        return $this->type;
    }

    /**
     * @return array{
     *   code: int,
     *   message: string,
     *   type: ErrorType,
     * }
     * 
     * @throws \Cassandra\Response\Exception
     */
    protected function readData(): array {
        $this->stream->offset(0);

        $code = $this->stream->readInt();
        $message =  $this->stream->readString();

        try {
            $type = ErrorType::from($code);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid error type: ' . $code, Exception::ERROR_INVALID_TYPE, [
                'error_type' => $code,
            ]);
        }

        return [
            'code' => $code,
            'message' => $message,
            'type' => $type,
        ];
    }
}
