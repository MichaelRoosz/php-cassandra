<?php

declare(strict_types=1);

namespace Cassandra\Request;

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
     */
    public function __construct(protected array $options = []) {
        parent::__construct(Opcode::REQUEST_STARTUP);
    }

    #[\Override]
    public function getBody(): string {
        $body = pack('n', count($this->options));
        foreach ($this->options as $name => $value) {
            $body .= pack('n', strlen($name)) . $name;
            $body .= pack('n', strlen($value)) . $value;
        }

        return $body;
    }
}
