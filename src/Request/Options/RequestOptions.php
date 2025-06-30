<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

class RequestOptions {
    /**
     * @return array<string, string|int|bool>
     */
    public function toArray(): array {
        return [];
    }
}
