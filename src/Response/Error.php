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
    protected int $code;
    protected string $message;
    protected ErrorType $type;

    public function __construct(Header $header, StreamReader $stream) {
        parent::__construct($header, $stream);

        $this->readData();
    }

    public function getCode(): int {
        return $this->code;
    }

    public function getContext(): ErrorContext {
        return new ErrorContext();
    }

    public function getException(): Exception {

        return new Exception(
            message: $this->message,
            code: $this->code,
            context: $this->getContext()->toArray(),
        );
    }

    public function getMessage(): string {
        return $this->message;
    }

    public function getType(): ErrorType {
        return $this->type;
    }

    protected function readData(): void {
        $this->stream->offset(0);

        $this->code = $this->stream->readInt();
        $this->message =  $this->stream->readString();

        try {
            $this->type = ErrorType::from($this->code);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid error type: ' . $this->code, 0, [
                'error_type' => $this->code,
            ]);
        }

        switch ($code) {

            case ErrorType::UNAVAILABLE_EXCEPTION->value:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_required' => $this->stream->readInt(),
                    'nodes_alive' => $this->stream->readInt(),
                ];
                $data['message'] = 'Unavailable exception. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::READ_TIMEOUT->value:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_answered' => $this->stream->readInt(),
                    'nodes_required' => $this->stream->readInt(),
                    'data_present' => $this->stream->readChar(),
                ];

                $data['message'] = 'Read_timeout. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::READ_FAILURE->value:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_answered' => $this->stream->readInt(),
                    'nodes_required' => $this->stream->readInt(),
                ];

                if ($this->getVersion() >= 5) {
                    $data['context']['reasonmap'] = $this->stream->readReasonMap();
                } else {
                    $data['context']['num_failures'] = $this->stream->readInt();
                }

                $data['context']['data_present'] = $this->stream->readChar();

                $data['message'] = 'Read_failure. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::FUNCTION_FAILURE->value:
                $data['context'] += [
                    'keyspace' => $this->stream->readString(),
                    'function' => $this->stream->readString(),
                    'arg_types' => $this->stream->readStringList(),
                ];

                $data['message'] = 'Function_failure. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::WRITE_FAILURE->value:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_answered' => $this->stream->readInt(),
                    'nodes_required' => $this->stream->readInt(),
                ];

                if ($this->getVersion() >= 5) {
                    $data['context']['reasonmap'] = $this->stream->readReasonMap();
                } else {
                    $data['context']['num_failures'] = $this->stream->readInt();
                }

                $data['context']['write_type'] = $this->stream->readString();

                $data['message'] = 'Write_failure. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::CDC_WRITE_FAILURE->value:
                $data['message'] = 'CDC_WRITE_FAILURE: ' . $message;

                break;

            case ErrorType::CAS_WRITE_UNKNOWN->value:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_acknowledged' => $this->stream->readInt(),
                    'nodes_required' => $this->stream->readInt(),
                ];

                $data['message'] = 'CAS_WRITE_UNKNOWN. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::SYNTAX_ERROR->value:
                $data['message'] = 'Syntax_error: ' . $message;

                break;

            case ErrorType::UNAUTHORIZED->value:
                $data['message'] = 'Unauthorized: ' . $message;

                break;

            case ErrorType::INVALID->value:
                $data['message'] = 'Invalid: ' . $message;

                break;

            case ErrorType::CONFIG_ERROR->value:
                $data['message'] = 'Config_error: ' . $message;

                break;

            case ErrorType::ALREADY_EXISTS->value:
                $data['context'] += [
                    'keyspace' => $this->stream->readString(),
                    'table' => $this->stream->readString(),
                ];

                $data['message'] = 'Already_exists. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::UNPREPARED->value:
                $data['context'] += [
                    'unknown_statement_id' => $this->stream->readString(),
                ];

                $data['message'] = 'Unprepared. Error data: ' . var_export($data['context'], true);

                break;

            default:
                $data['message'] = 'Unknown error: ' . $message;
        }

        return $data;
    }
}
