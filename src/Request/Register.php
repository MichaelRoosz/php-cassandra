<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\EventType;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Protocol\Opcode;

final class Register extends Request {
    /**
     * @param array<\Cassandra\EventType> $events
     *
     * @throws \Cassandra\Exception\RequestException
     */
    public function __construct(private array $events) {
        parent::__construct(Opcode::REQUEST_REGISTER);

        self::validateEvents($events);
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        self::assertShortCount(
            count($this->events),
            'registered events',
            ExceptionCode::REQUEST_TOO_MANY_REGISTER_EVENTS,
        );
        $body = pack('n', count($this->events));

        foreach ($this->events as $value) {
            $body .= pack('n', strlen($value->value)) . $value->value;
        }

        return $body;
    }

    /**
     * @param array<mixed> $events
     *
     * @throws \Cassandra\Exception\RequestException
     */
    private static function validateEvents(array $events): void {

        foreach ($events as $index => $event) {
            if (!$event instanceof EventType) {
                throw new RequestException(
                    'Invalid registered event; every entry must be an EventType',
                    ExceptionCode::REQUEST_INVALID_REGISTER_EVENT->value,
                    [
                        'index' => $index,
                        'actual_type' => get_debug_type($event),
                    ]
                );
            }
        }
    }
}
