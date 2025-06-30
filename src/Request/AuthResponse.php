<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

final class AuthResponse extends Request {
    /**
     * CREDENTIALS
     *
     * Provides credentials information for the purpose of identification. This
     * message comes as a response to an AUTHENTICATE message from the server, but
     * can be use later in the communication to change the authentication
     * information.
     *
     * The body is a list of key/value informations. It is a [short] n, followed by n
     * pair of [string]. These key/value pairs are passed as is to the Cassandra
     * IAuthenticator and thus the detail of which informations is needed depends on
     * that authenticator.
     *
     * The response to a CREDENTIALS is a READY message (or an ERROR message).
     *
     */
    public function __construct(
        protected string $username,
        protected string $password
    ) {
        parent::__construct(Opcode::REQUEST_AUTH_RESPONSE);
    }

    #[\Override]
    public function getBody(): string {
        $body = chr(0);
        $body .= $this->username;
        $body .= chr(0);
        $body .= $this->password;

        return pack('N', strlen($body)) . $body;
    }
}
