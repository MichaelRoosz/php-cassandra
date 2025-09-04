<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Type;

final class Counter extends Bigint {
    #[\Override]
    public function getType(): Type {
        return Type::COUNTER;
    }
}
