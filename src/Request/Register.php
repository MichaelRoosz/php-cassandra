<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

final class Register extends Request {
    /**
     * @param array<string> $events
     */
    public function __construct(protected array $events) {
        parent::__construct(Opcode::REQUEST_REGISTER);
    }

    #[\Override]
    public function getBody(): string {
        $body = pack('n', count($this->events));

        foreach ($this->events as $value) {
            $body .= pack('n', strlen($value)) . $value;
        }

        return $body;
    }
}
