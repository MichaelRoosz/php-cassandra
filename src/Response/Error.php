<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Response\Error\AlreadyExistsError;
use Cassandra\Response\Error\AuthenticationError;
use Cassandra\Response\Error\CasWriteUnknownError;
use Cassandra\Response\Error\CdcWriteFailureError;
use Cassandra\Response\Error\ConfigError;
use Cassandra\Response\Error\Context\ErrorContext;
use Cassandra\Response\Error\FunctionFailureError;
use Cassandra\Response\Error\InvalidError;
use Cassandra\Response\Error\IsBootstrappingError;
use Cassandra\Response\Error\OverloadedError;
use Cassandra\Response\Error\ProtocolError;
use Cassandra\Response\Error\ReadFailureError;
use Cassandra\Response\Error\ReadTimeoutError;
use Cassandra\Response\Error\ServerError;
use Cassandra\Response\Error\SyntaxError;
use Cassandra\Response\Error\TruncateError;
use Cassandra\Response\Error\UnauthorizedError;
use Cassandra\Response\Error\UnavailableExceptionError;
use Cassandra\Response\Error\UnpreparedError;
use Cassandra\Response\Error\WriteFailureError;
use Cassandra\Response\Error\WriteTimeoutError;
use TypeError;
use ValueError;

/**
 * Indicates an error processing a request.
 */
class Error extends Response {
    /**
     * @var array<class-string<\Cassandra\Response\Error>, class-string<\Cassandra\Exception\ServerException>>
     */
    protected const exceptionClassMap = [
        AlreadyExistsError::class => ServerException\AlreadyExistsException::class,
        AuthenticationError::class => ServerException\AuthenticationErrorException::class,
        CasWriteUnknownError::class => ServerException\CasWriteUnknownException::class,
        CdcWriteFailureError::class => ServerException\CdcWriteFailureException::class,
        ConfigError::class => ServerException\ConfigErrorException::class,
        FunctionFailureError::class => ServerException\FunctionFailureException::class,
        InvalidError::class => ServerException\InvalidException::class,
        IsBootstrappingError::class => ServerException\IsBootstrappingException::class,
        OverloadedError::class => ServerException\OverloadedException::class,
        ProtocolError::class => ServerException\ProtocolErrorException::class,
        ReadFailureError::class => ServerException\ReadFailureException::class,
        ReadTimeoutError::class => ServerException\ReadTimeoutException::class,
        ServerError::class => ServerException::class,
        SyntaxError::class => ServerException\SyntaxErrorException::class,
        TruncateError::class => ServerException\TruncateErrorException::class,
        UnauthorizedError::class => ServerException\UnauthorizedException::class,
        UnavailableExceptionError::class => ServerException\UnavailableException::class,
        UnpreparedError::class => ServerException\UnpreparedException::class,
        WriteFailureError::class => ServerException\WriteFailureException::class,
        WriteTimeoutError::class => ServerException\WriteTimeoutException::class,
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

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<int, class-string<\Cassandra\Response\Error>>
     */
    public static function getErrorClassMap(): array {
        return [
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
    }

    public function getException(): ServerException {
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

        $exceptionClass = self::exceptionClassMap[static::class] ?? ServerException::class;

        return new $exceptionClass(
            message: $message,
            code: $this->code,
            context: $baseContext,
            errorContext: $this->getContext(),
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
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function readData(): array {
        $this->stream->offset(0);

        $code = $this->stream->readInt();
        $message =  $this->stream->readString();

        try {
            $type = ErrorType::from($code);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException('Invalid error type: ' . $code, ExceptionCode::RESPONSE_ERROR_INVALID_TYPE->value, [
                'error_type' => $code,
            ], $e);
        }

        return [
            'code' => $code,
            'message' => $message,
            'type' => $type,
        ];
    }
}
