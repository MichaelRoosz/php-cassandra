<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

final class BatchOptions extends QueryOptions {
    public function __construct(
        public ?int $serialConsistency = null,
        public ?int $defaultTimestamp = null,
        public ?string $keyspace = null,
        public ?int $nowInSeconds = null,
    ) {
    }

    /**
     * @return array{
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  keyspace?: string,
     *  now_in_seconds?: int,
     * }
     */
    #[\Override]
    public function toArray(): array {
        $options = [];

        if ($this->serialConsistency !== null) {
            $options['serial_consistency'] = $this->serialConsistency;
        }

        if ($this->defaultTimestamp !== null) {
            $options['default_timestamp'] = $this->defaultTimestamp;
        }

        if ($this->keyspace !== null) {
            $options['keyspace'] = $this->keyspace;
        }

        if ($this->nowInSeconds !== null) {
            $options['now_in_seconds'] = $this->nowInSeconds;
        }

        return $options;
    }
}
