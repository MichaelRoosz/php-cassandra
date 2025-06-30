<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

final class PrepareOptions extends RequestOptions {
    public function __construct(
        public ?string $keyspace = null,
    ) {
    }

    /**
     * @return array{
     *  keyspace?: string,
     * }
     */
    #[\Override]
    public function toArray(): array {
        $options = [];

        if ($this->keyspace !== null) {
            $options['keyspace'] = $this->keyspace;
        }

        return $options;
    }
}
