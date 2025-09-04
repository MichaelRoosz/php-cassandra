<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Type;

final class Ascii extends Varchar {
    #[\Override]
    public function getType(): Type {
        return Type::ASCII;
    }
}
