<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Type;
use Cassandra\TypeFactory;

abstract class TypeInfo {
    public function __construct(
        public readonly Type $type,
    ) {
    }

    public function isSerializedAsFixedSize(): bool {
        return TypeFactory::isSerializedAsFixedSize($this->type);
    }
}
