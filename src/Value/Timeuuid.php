<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Type;

final class Timeuuid extends Uuid {
    #[\Override]
    public function getType(): Type {
        return Type::TIMEUUID;
    }
}
