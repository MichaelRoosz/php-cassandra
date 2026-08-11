<?php

declare(strict_types=1);

namespace Cassandra\Value\EncodeOption;

enum MapEncodeOption: string {
    /** Require a native PHP array; decoding fails when the configured key representation cannot be used as an array key. */
    case AS_ARRAY = 'array';

    /** Always return a MapCollection, including for maps whose keys would fit in a PHP array. */
    case AS_MAP_COLLECTION = 'MapCollection';
    /** Return a native PHP array when the configured key representation fits one, and a MapCollection otherwise. */
    case AUTO = 'auto';
}
