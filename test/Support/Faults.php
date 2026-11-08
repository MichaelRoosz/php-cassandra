<?php

declare(strict_types=1);

namespace Cassandra\Test\Support;

use Closure;

/**
 * Makes one call to a PHP function fail on purpose, so that a test can reach the
 * error branch behind it. The overrides in faults-functions.php take effect
 * because the library calls these functions unqualified. Every call passes
 * through to PHP unless a fault is queued for it.
 */
final class Faults {
    private const PASS_THROUGH = "\0faults:pass-through\0";

    /** @var array<string, list<mixed>> */
    private static array $queue = [];

    /**
     * Makes every call of $function return $result until reset().
     */
    public static function always(string $function, mixed $result): void {
        $fault = static function (mixed &...$arguments) use ($function, $result, &$fault): mixed {
            array_unshift(self::$queue[$function], $fault);

            return $result instanceof Closure ? $result(...$arguments) : $result;
        };
        self::$queue[$function][] = $fault;
    }

    /**
     * @param list<mixed> $arguments by reference where the PHP function takes them so
     */
    public static function call(string $function, array &$arguments, Closure $native): mixed {
        if (!isset(self::$queue[$function]) || self::$queue[$function] === []) {
            return $native(...$arguments);
        }

        $fault = array_shift(self::$queue[$function]);
        $result = $fault instanceof Closure ? $fault(...$arguments) : $fault;

        return $result === self::PASS_THROUGH ? $native(...$arguments) : $result;
    }

    /**
     * Queues what the next calls of $function return: a value, or a closure given
     * the call's arguments, by reference where PHP passes them so.
     */
    public static function inject(string $function, mixed ...$results): void {
        foreach ($results as $result) {
            self::$queue[$function][] = $result;
        }
    }

    /**
     * Like inject(), but only for the first call whose arguments satisfy $matches;
     * other calls pass through to PHP meanwhile.
     *
     */
    public static function injectWhere(string $function, Closure $matches, mixed $result): void {
        $fault = static function (mixed &...$arguments) use ($function, $matches, $result, &$fault): mixed {
            if (!$matches(...$arguments)) {
                array_unshift(self::$queue[$function], $fault);

                return self::PASS_THROUGH;
            }

            return $result instanceof Closure ? $result(...$arguments) : $result;
        };
        self::$queue[$function][] = $fault;
    }

    public static function pending(string $function): int {
        return count(self::$queue[$function] ?? []);
    }

    public static function reset(): void {
        self::$queue = [];
    }
}
