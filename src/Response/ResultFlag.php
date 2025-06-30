<?php

declare(strict_types=1);

namespace Cassandra\Response;

enum ResultFlag: int {
    case ROWS_FLAG_GLOBAL_TABLES_SPEC = 0x0001;
    case ROWS_FLAG_HAS_MORE_PAGES = 0x0002;
    case ROWS_FLAG_METADATA_CHANGED = 0x0008;
    case ROWS_FLAG_NO_METADATA = 0x0004;
}
