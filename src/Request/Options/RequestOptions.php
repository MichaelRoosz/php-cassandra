<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

class RequestOptions {
    /**
     * @return array{
     *  names_for_values?: bool,
     *  skip_metadata?: bool,
     *  page_size?: int,
     *  paging_state?: string,
     *  serial_consistency?: int,
     *  default_timestamp?: int,
     *  keyspace?: string,
     *  now_in_seconds?: int,
     * }
     */
    public function toArray(): array {
        return [];
    }
}
