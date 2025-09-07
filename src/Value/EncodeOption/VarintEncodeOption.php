<?php

declare(strict_types=1);

namespace Cassandra\Value\EncodeOption;

enum VarintEncodeOption: string {
    /** Will throw an exeception if the Varint is out of int range. */
    case AS_INT = 'int';

    case AS_STRING = 'string';
}
