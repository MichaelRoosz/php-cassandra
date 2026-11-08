<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

final class AuthResponse extends Request {
    public function __construct(
        #[\SensitiveParameter]
        private string $token,
    ) {
        parent::__construct(Opcode::REQUEST_AUTH_RESPONSE);
    }

    #[\Override]
    public function getBody(): string {
        return pack('N', strlen($this->token)) . $this->token;
    }
}
