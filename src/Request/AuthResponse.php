<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

final class AuthResponse extends Request {
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
