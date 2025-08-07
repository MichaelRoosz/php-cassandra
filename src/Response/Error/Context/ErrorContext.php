<?php

declare(strict_types=1);

namespace Cassandra\Response\Error\Context;

class ErrorContext {
    public function __construct() {

    }

    /**
     * @return array<string, mixed>
     */
    public function toArray(): array {
        return [];
    }
}
