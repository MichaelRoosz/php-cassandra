<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration\Data;

use Cassandra\EventListener as EventListenerInterface;
use Cassandra\Response\Event;

final class EventListener implements EventListenerInterface {
    /** @var array<\Cassandra\Response\Event> */
    private array $events;

    /**
     * @return array<\Cassandra\Response\Event>
     */
    public function getEvents(): array {
        return $this->events;
    }

    public function onEvent(Event $event): void {
        $this->events[] = $event;
    }
}
