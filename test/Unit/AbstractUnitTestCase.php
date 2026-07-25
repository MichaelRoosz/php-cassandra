<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use PHPUnit\Framework\TestCase;

abstract class AbstractUnitTestCase extends TestCase {
    public function integerHasAtLeast64Bits(): bool {
        return PHP_INT_SIZE >= 8;
    }

    /**
     * Deterministic stand-in for random_bytes(). The output is incompressible
     * for practical purposes but fully reproducible from the seed, so a test
     * failure on generated input can always be replayed.
     */
    protected static function pseudoRandomBytes(int $length, int $seed): string {
        $output = '';
        $counter = 0;

        while (strlen($output) < $length) {
            $output .= hash('sha256', $seed . ':' . $counter, true);
            $counter++;
        }

        return substr($output, 0, $length);
    }
}
