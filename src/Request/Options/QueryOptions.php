<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

class QueryOptions extends RequestOptions {
    public function __construct(
        public ?bool $skipMetadata = null,
        public ?int $pageSize = null,
        public ?string $pagingState = null,
        public ?int $serialConsistency = null,
        public ?int $defaultTimestamp = null,
        public ?bool $namesForValues = null,
        public ?string $keyspace = null,
        public ?int $nowInSeconds = null,
    ) {
    }

    /**
     * @return array{
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  names_for_values?: bool,
     *  keyspace?: string,
     *  now_in_seconds?: int,
     * }
     */
    #[\Override]
    public function toArray(): array {
        $options = [];

        if ($this->skipMetadata !== null) {
            $options['skip_metadata'] = $this->skipMetadata;
        }

        if ($this->pageSize !== null) {
            $options['page_size'] = $this->pageSize;
        }

        if ($this->pagingState !== null) {
            $options['paging_state'] = $this->pagingState;
        }

        if ($this->serialConsistency !== null) {
            $options['serial_consistency'] = $this->serialConsistency;
        }

        if ($this->defaultTimestamp !== null) {
            $options['default_timestamp'] = $this->defaultTimestamp;
        }

        if ($this->namesForValues !== null) {
            $options['names_for_values'] = $this->namesForValues;
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
