<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;

abstract class TypeInfo {
    public function __construct(
        public readonly Type $type,
    ) {
    }
}
