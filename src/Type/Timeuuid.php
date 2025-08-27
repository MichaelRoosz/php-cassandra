<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Type;

final class Timeuuid extends Uuid {
    #[\Override]
    public function getType(): Type {
        return Type::TIMEUUID;
    }
}
