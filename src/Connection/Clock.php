<?php

declare(strict_types=1);

namespace Cassandra\Connection;

/**
 * The time scale of every deadline in this library: monotonic seconds, unaffected
 * by NTP steps or a changed system clock. Only differences are meaningful, since
 * the origin is arbitrary. A custom transport compares a read deadline against this.
 *
 * Where hrtime() is disabled or has no timer, this falls back to the wall clock.
 */
final class Clock {
    /**
     * Settled once per process: the two clocks have different origins, so a
     * switch between them would corrupt every deadline already taken.
     */
    private static ?bool $monotonic = null;

    public static function now(): float {
        /** @psalm-suppress RedundantCondition PHP documents false for a platform without a timer; Psalm's stub omits it */
        self::$monotonic ??= function_exists('hrtime') && hrtime(true) !== false;

        return self::$monotonic ? (float) hrtime(true) / 1e9 : microtime(true);
    }
}
