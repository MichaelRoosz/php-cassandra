<?php

declare(strict_types=1);

namespace Cassandra\Value\EncodeOption;

enum UuidEncodeOption: string {
    /** The raw 16-byte big-endian binary form (no formatting; fastest to decode). */
    case AS_BINARY = 'binary';

    /** The canonical 36-character string form (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx). */
    case AS_STRING = 'string';
}
