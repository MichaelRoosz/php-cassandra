<?php

declare(strict_types=1);

namespace Cassandra\Value;

interface ValueWithMultipleEncodings {
    public function asConfigured(ValueEncodeConfig $valueEncodeConfig): mixed;
}
