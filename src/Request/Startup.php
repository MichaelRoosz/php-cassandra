<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Protocol\Opcode;

final class Startup extends Request {
    /**
     * STARTUP
     *
     * Initialize the connection. The server will respond by either a READY message
     * (in which case the connection is ready for queries) or an AUTHENTICATE message
     * (in which case credentials will need to be provided using CREDENTIALS).
     *
     * This must be the first message of the connection, except for OPTIONS that can
     * be sent before to find out the options supported by the server. Once the
     * connection has been initialized, a client should not send any more STARTUP
     * message.
     *
     * Possible options are:
     * - "CQL_VERSION": the version of CQL to use. This option is mandatory and
     * currenty, the only version supported is "3.0.0". Note that this is
     * different from the protocol version.
     * - "COMPRESSION": the compression algorithm to use for frames (See section 5).
     * This is optional, if not specified no compression will be used.
     *
     * @param array<string, string> $options
     *
     * @throws \Cassandra\Exception\RequestException
     */
    public function __construct(private array $options = []) {
        parent::__construct(Opcode::REQUEST_STARTUP);

        self::validateOptions($options);
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        self::assertShortCount(count($this->options), 'startup options');
        $body = pack('n', count($this->options));
        foreach ($this->options as $name => $value) {
            self::assertShortString($name, 'startup option name');
            self::assertShortString($value, 'startup option value');
            $body .= pack('n', strlen($name)) . $name;
            $body .= pack('n', strlen($value)) . $value;
        }

        return $body;
    }

    /**
     * @param array<mixed> $options
     *
     * @throws \Cassandra\Exception\RequestException
     */
    private static function validateOptions(array $options): void {

        foreach ($options as $name => $value) {
            if (!is_string($name) || !is_string($value)) {
                throw new RequestException(
                    'Invalid startup option; every name and value must be a string',
                    ExceptionCode::REQUEST_INVALID_STARTUP_OPTION->value,
                    [
                        'name_type' => get_debug_type($name),
                        'value_type' => get_debug_type($value),
                    ]
                );
            }
        }
    }
}
