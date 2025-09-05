<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

final class PrepareOptions extends RequestOptions {
    public function __construct(
        public readonly ?string $keyspace = null,
    ) {
    }

    public function withKeyspace(string $keyspace): self {
        return new self(
            keyspace: $keyspace,
        );
    }
}
