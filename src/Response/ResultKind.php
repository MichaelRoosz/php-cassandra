<?php

declare(strict_types=1);

namespace Cassandra\Response;

enum ResultKind: int {
    case PREPARED = 0x0004;
    case ROWS = 0x0002;
    case SCHEMA_CHANGE = 0x0005;
    case SET_KEYSPACE = 0x0003;
    case VOID = 0x0001;
}
