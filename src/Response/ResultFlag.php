<?php

declare(strict_types=1);

namespace Cassandra\Response;

final class ResultFlag {
    public const ROWS_FLAG_GLOBAL_TABLES_SPEC = 0x0001;
    public const ROWS_FLAG_HAS_MORE_PAGES = 0x0002;
    public const ROWS_FLAG_METADATA_CHANGED = 0x0008;
    public const ROWS_FLAG_NO_METADATA = 0x0004;
}
