<?php

declare(strict_types=1);

namespace Cassandra;

interface EventListener
{
    public function onEvent(Response\Event $event): void;
}
