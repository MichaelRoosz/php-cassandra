<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

final class PrepareOptions extends RequestOptions {
    public function __construct(
        public ?string $keyspace = null,
    ) {
    }
}
