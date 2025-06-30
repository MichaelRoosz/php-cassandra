<?php

declare(strict_types=1);

namespace Cassandra\Request;

enum QueryFlag: int {
    case PAGE_SIZE = 0x04;
    case SKIP_METADATA = 0x02;
    case VALUES = 0x01;
    case WITH_DEFAULT_TIMESTAMP = 0x20;
    case WITH_KEYSPACE = 0x80;
    case WITH_NAMES_FOR_VALUES = 0x40;
    case WITH_NOW_IN_SECONDS = 0x0100;
    case WITH_PAGING_STATE = 0x08;
    case WITH_SERIAL_CONSISTENCY = 0x10;
}
