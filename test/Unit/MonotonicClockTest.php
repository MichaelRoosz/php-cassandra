<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\Clock;
use Cassandra\Request\Query;
use Cassandra\Test\Support\Faults;
use Cassandra\Statement;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use ReflectionProperty;
use SplFileInfo;

/**
 * Regression: deadlines used the wall clock, so an NTP step could fire every
 * timeout early or delay them by the size of the step.
 */
final class MonotonicClockTest extends AbstractUnitTestCase {
    protected function tearDown(): void {
        Faults::reset();
        self::forgetClockChoice();
        // Settled again here, so that the next test's hrtime() faults are not taken by the detection.
        Clock::now();
        parent::tearDown();
    }

    public function testAStatementRecordsItsSendTimeOnThatClock(): void {
        $before = Clock::now();
        $connection = new \Cassandra\Connection([new \Cassandra\Connection\StreamNodeConfig()]);
        $statement = new Statement($connection, 1, 1, new Query('SELECT 1'));

        $this->assertGreaterThanOrEqual($before, $statement->getSentAt());
        $this->assertLessThanOrEqual(Clock::now(), $statement->getSentAt());
    }
    public function testNoSourceFileReadsTheWallClockForTiming(): void {
        $offenders = [];
        /** @var SplFileInfo $file */
        foreach (new RecursiveIteratorIterator(new RecursiveDirectoryIterator(__DIR__ . '/../../src')) as $file) {
            // Clock is the one place allowed the wall clock, as its fallback.
            if ($file->getExtension() === 'php' && $file->getBasename() !== 'Clock.php' && str_contains((string) file_get_contents($file->getPathname()), 'microtime(')) {
                $offenders[] = $file->getBasename();
            }
        }

        $this->assertSame([], $offenders);
    }

    public function testTheClockNeverGoesBackwards(): void {
        $previous = Clock::now();
        for ($i = 0; $i < 1000; $i++) {
            $now = Clock::now();
            $this->assertGreaterThanOrEqual($previous, $now);
            $previous = $now;
        }
    }

    /**
     * Regression: with hrtime() disabled, every connection call raised a native
     * Error, and a platform without a timer a TypeError.
     */
    public function testTheWallClockStandsInWhereHrtimeIsUnavailable(): void {
        foreach (['disabled' => ['function_exists', false], 'without a timer' => ['hrtime', false]] as $case => [$function, $result]) {
            self::forgetClockChoice();
            Faults::injectWhere($function, static fn (mixed $argument = null): bool => $function === 'hrtime' || $argument === 'hrtime', $result);

            $before = microtime(true);
            $now = Clock::now();
            $this->assertGreaterThanOrEqual($before, $now, $case);
            $this->assertLessThanOrEqual(microtime(true), $now, $case);

            // The choice holds once made, even when hrtime() would work again.
            $this->assertGreaterThanOrEqual($now, Clock::now(), $case);
            $this->assertGreaterThan(1_000_000_000.0, Clock::now(), $case);
            Faults::reset();
        }
    }

    private static function forgetClockChoice(): void {
        (new ReflectionProperty(Clock::class, 'monotonic'))->setValue(null, null);
    }
}
