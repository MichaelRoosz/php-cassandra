<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Value\Date;
use Cassandra\Value\Time;
use Cassandra\Value\Timestamp;

/**
 * The three temporal types hand back DateTimeImmutable objects anchored at UTC.
 *
 * None of `date`, `time` or `timestamp` carries a timezone on the wire, so the
 * one the driver puts on the object it builds is scaffolding either way — but it
 * has to be the same scaffolding for all three. Time::asDateTime() used to leave
 * it to the ambient default, so an application that had called
 * date_default_timezone_set() got a `time` whose offset disagreed with the
 * `date` and `timestamp` beside it, for values that mean the same instant of the
 * day.
 */
final class TemporalDateTimeZoneTest extends AbstractUnitTestCase {
    private string $originalTimezone = 'UTC';

    protected function setUp(): void {
        // Captured before the skip below, so tearDown puts back the zone this
        // test found rather than the property's initialiser.
        $this->originalTimezone = date_default_timezone_get();

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Date, Time and Timestamp require 64-bit integer');
        }

        // A zone with a large, non-zero offset, so a value that picked it up
        // instead of UTC cannot pass by coincidence.
        date_default_timezone_set('America/New_York');
    }

    protected function tearDown(): void {
        date_default_timezone_set($this->originalTimezone);
    }

    public function testDateIsAnchoredAtUtc(): void {
        $dateTime = (new Date(2147483648))->asDateTime();

        $this->assertSame(0, $dateTime->getOffset());
        $this->assertSame('1970-01-01 00:00:00', $dateTime->format('Y-m-d H:i:s'));
    }

    public function testTimeIsAnchoredAtUtc(): void {
        $dateTime = (new Time(45296000000000))->asDateTime();

        $this->assertSame(0, $dateTime->getOffset(), 'a time must not pick up the ambient timezone');
        $this->assertSame('1970-01-01 12:34:56', $dateTime->format('Y-m-d H:i:s'));
    }

    public function testTimeRoundTripsThroughItsDateTime(): void {
        // Microsecond-aligned, which is the precision a DateTimeImmutable
        // carries; anything finer is truncated, as the test below covers.
        foreach ([0, 1000, 45296123456000, Time::VALUE_MAX - 999] as $nanoseconds) {
            $value = new Time($nanoseconds);

            $this->assertSame(
                $nanoseconds,
                (new Time($value->asDateTime()))->asInteger(),
                'the wall clock survives the conversion at microsecond precision',
            );
        }
    }

    public function testTimestampIsAnchoredAtUtc(): void {
        $dateTime = (new Timestamp(45296000))->asDateTime();

        $this->assertSame(0, $dateTime->getOffset());
        $this->assertSame('1970-01-01 12:34:56', $dateTime->format('Y-m-d H:i:s'));
    }

    public function testTimeTruncatesBelowMicrosecondPrecision(): void {
        // DateTimeImmutable carries microseconds; the type carries nanoseconds.
        $value = new Time(45296123456789);

        $this->assertSame('12:34:56.123456789', $value->asString());
        $this->assertSame('12:34:56.123456', $value->asDateTime()->format('H:i:s.u'));
    }
}
