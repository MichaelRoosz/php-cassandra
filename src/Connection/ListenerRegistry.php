<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\EventListener;
use Cassandra\Request\Request;
use Cassandra\Response\Event;
use Cassandra\Response\Response;
use Cassandra\WarningsListener;

/**
 * The application's callbacks for the two things a connection reports without
 * having been asked: server-pushed events, and the warnings a response carries
 * alongside its result.
 */
final class ListenerRegistry {
    /**
     * @var array<EventListener> $eventListeners
     */
    private array $eventListeners = [];

    /**
     * @var array<WarningsListener> $warningsListeners
     */
    private array $warningsListeners = [];

    public function notifyEvent(Event $event): void {

        foreach ($this->eventListeners as $listener) {
            $listener->onEvent($event);
        }
    }

    /**
     * @param array<string> $warnings
     */
    public function notifyWarnings(array $warnings, Request $request, Response $response): void {

        foreach ($this->warningsListeners as $listener) {
            $listener->onWarnings($warnings, $request, $response);
        }
    }

    public function registerEventListener(EventListener $eventListener): void {

        $this->eventListeners[] = $eventListener;
    }

    public function registerWarningsListener(WarningsListener $warningsListener): void {

        $this->warningsListeners[] = $warningsListener;
    }

    public function unregisterEventListener(EventListener $eventListener): void {

        $this->eventListeners = array_filter($this->eventListeners, fn (EventListener $listener) => $listener !== $eventListener);
    }

    public function unregisterWarningsListener(WarningsListener $warningsListener): void {

        $this->warningsListeners = array_filter($this->warningsListeners, fn (WarningsListener $listener) => $listener !== $warningsListener);
    }
}
