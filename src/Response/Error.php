<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Type;

final class Error extends Response {
    /** @deprecated Use ALREADY_EXISTS instead */
    final public const ALREADY_EXIST = 0x2400;
    final public const ALREADY_EXISTS = 0x2400;
    final public const AUTHENTICATION_ERROR = 0x0100;

    /** @deprecated Use AUTHENTICATION_ERROR instead */
    final public const BAD_CREDENTIALS = 0x0100;
    final public const CAS_WRITE_UNKNOWN = 0x1700;
    final public const CDC_WRITE_FAILURE = 0x1600;
    final public const CONFIG_ERROR = 0x2300;
    final public const FUNCTION_FAILURE = 0x1400;
    final public const INVALID = 0x2200;
    final public const IS_BOOTSTRAPPING = 0x1002;
    final public const OVERLOADED = 0x1001;
    final public const PROTOCOL_ERROR = 0x000A;
    final public const READ_FAILURE = 0x1300;
    final public const READ_TIMEOUT = 0x1200;
    final public const SERVER_ERROR = 0x0000;
    final public const SYNTAX_ERROR = 0x2000;
    final public const TRUNCATE_ERROR = 0x1003;
    final public const UNAUTHORIZED = 0x2100;
    final public const UNAVAILABLE_EXCEPTION = 0x1000;
    final public const UNPREPARED = 0x2500;
    final public const WRITE_FAILURE = 0x1500;
    final public const WRITE_TIMEOUT = 0x1100;

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
            case self::SERVER_ERROR:
                $data['message'] = 'Server error: ' . $message;

                break;

            case self::PROTOCOL_ERROR:
                $data['message'] = 'Protocol error: ' . $message;

                break;

            case self::AUTHENTICATION_ERROR:
                $data['message'] = 'Authentication error: ' . $message;

                break;

            case self::UNAVAILABLE_EXCEPTION:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_required' => $this->stream->readInt(),
                    'nodes_alive' => $this->stream->readInt(),
                ];
                $data['message'] = 'Unavailable exception. Error data: ' . var_export($data['context'], true);

                break;

            case self::OVERLOADED:
                $data['message'] = 'Overloaded: ' . $message;

                break;

            case self::IS_BOOTSTRAPPING:
                $data['message'] = 'Is_bootstrapping: ' . $message;

                break;

            case self::TRUNCATE_ERROR:
                $data['message'] = 'Truncate_error: ' . $message;

                break;

            case self::WRITE_TIMEOUT:
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

            case self::READ_TIMEOUT:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_answered' => $this->stream->readInt(),
                    'nodes_required' => $this->stream->readInt(),
                    'data_present' => $this->stream->readChar(),
                ];

                $data['message'] = 'Read_timeout. Error data: ' . var_export($data['context'], true);

                break;

            case self::READ_FAILURE:
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

            case self::FUNCTION_FAILURE:
                $data['context'] += [
                    'keyspace' => $this->stream->readString(),
                    'function' => $this->stream->readString(),
                    'arg_types' => $this->stream->readStringList(),
                ];

                $data['message'] = 'Function_failure. Error data: ' . var_export($data['context'], true);

                break;

            case self::WRITE_FAILURE:
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

            case self::CDC_WRITE_FAILURE:
                $data['message'] = 'CDC_WRITE_FAILURE: ' . $message;

                break;

            case self::CAS_WRITE_UNKNOWN:
                $data['context'] += [
                    'consistency' => $this->stream->readShort(),
                    'nodes_acknowledged' => $this->stream->readInt(),
                    'nodes_required' => $this->stream->readInt(),
                ];

                $data['message'] = 'CAS_WRITE_UNKNOWN. Error data: ' . var_export($data['context'], true);

                break;

            case self::SYNTAX_ERROR:
                $data['message'] = 'Syntax_error: ' . $message;

                break;

            case self::UNAUTHORIZED:
                $data['message'] = 'Unauthorized: ' . $message;

                break;

            case self::INVALID:
                $data['message'] = 'Invalid: ' . $message;

                break;

            case self::CONFIG_ERROR:
                $data['message'] = 'Config_error: ' . $message;

                break;

            case self::ALREADY_EXISTS:
                $data['context'] += [
                    'keyspace' => $this->stream->readString(),
                    'table' => $this->stream->readString(),
                ];

                $data['message'] = 'Already_exists. Error data: ' . var_export($data['context'], true);

                break;

            case self::UNPREPARED:
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
