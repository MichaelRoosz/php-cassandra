<?php

declare(strict_types=1);

namespace Cassandra\Response;

final class Error extends Response {
    /**
     * Indicates an error processing a request. The body of the message will be an
     * error code ([int]) followed by a [string] error message. Then, depending on
     * the exception, more content may follow. The error codes are defined in
     * Section 7, along with their additional content if any.
     *
     * @return array{
     *    code: int,
     *    message: string,
     *    context: array<string, string|int|array<int|string, int|string>>
     *  }
     *
     * @throws \Cassandra\Response\Exception
     */
    public function getData(): array {
        $this->stream->offset(0);

        $code = $this->stream->readInt();
        $message =  $this->stream->readString();

        $data = [];
        $data['code'] = $code;
        $data['context'] = ['message' => $message];

        switch ($code) {
            case ErrorType::SERVER_ERROR->value:
                $data['message'] = 'Server error: ' . $message;

                break;

            case ErrorType::PROTOCOL_ERROR->value:
                $data['message'] = 'Protocol error: ' . $message;

                break;

            case ErrorType::AUTHENTICATION_ERROR->value:
                $data['message'] = 'Authentication error: ' . $message;

                break;

            case ErrorType::UNAVAILABLE_EXCEPTION->value:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_required' => $this->stream->readInt(),
                    'nodes_alive' => $this->stream->readInt(),
                ];
                $data['message'] = 'Unavailable exception. Error data: ' . var_export($data['context'], true);

                break;

            case ErrorType::OVERLOADED->value:
                $data['message'] = 'Overloaded: ' . $message;

                break;

            case ErrorType::IS_BOOTSTRAPPING->value:
                $data['message'] = 'Is_bootstrapping: ' . $message;

                break;

            case ErrorType::TRUNCATE_ERROR->value:
                $data['message'] = 'Truncate_error: ' . $message;

                break;

            case ErrorType::WRITE_TIMEOUT->value:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_acknowledged' => $this->stream->readInt(),
                    'nodes_required' => $this->stream->readInt(),
                    'write_type' => $this->stream->readString(),
                ];

                if ($this->getVersion() >= 5) {
                    $data['context']['contentions'] = $this->stream->readShort();
                }

                $data['message'] = 'Write_timeout. Error data: ' . var_export($data['context'], true);

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

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getException(): Exception {
        $data = $this->getData();

        return new Exception($data['message'], $data['code'], $data['context']);
    }
}
