<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

final class Register extends Request {
    /**
     * @param array<\Cassandra\EventType> $events
     */
    public function __construct(private array $events) {
        parent::__construct(Opcode::REQUEST_REGISTER);
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        self::assertShortCount(count($this->events), 'registered events');
        $body = pack('n', count($this->events));

        foreach ($this->events as $value) {
            $body .= pack('n', strlen($value->value)) . $value->value;
        }

        return $body;
    }
}
