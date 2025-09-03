<?php

declare(strict_types=1);

namespace Cassandra\Request;

final class QueryFlag {
    public const PAGE_SIZE = 0x04;
    public const SKIP_METADATA = 0x02;
    public const VALUES = 0x01;
    public const WITH_DEFAULT_TIMESTAMP = 0x20;
    public const WITH_KEYSPACE = 0x80;
    public const WITH_NAMES_FOR_VALUES = 0x40;
    public const WITH_NOW_IN_SECONDS = 0x0100;
    public const WITH_PAGING_STATE = 0x08;
    public const WITH_SERIAL_CONSISTENCY = 0x10;
}
