<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use PHPUnit\Framework\TestCase;

abstract class AbstractUnitTestCase extends TestCase {
    public function integerHasAtLeast64Bits(): bool {
        return PHP_INT_SIZE >= 8;
    }
}
