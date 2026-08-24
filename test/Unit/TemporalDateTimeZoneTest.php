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

    /**
     * Regression: a timestamp string that names an offset keeps it, so the
     * fallback zone above really is only a fallback.
     */
    public function testTimestampStringWithExplicitOffsetKeepsIt(): void {
        // 12:34:56 at +02:00 is 10:34:56 UTC, i.e. two hours less than the
        // zoneless spelling of the same wall clock.
        $this->assertSame(38096000, (new Timestamp('1970-01-01T12:34:56+02:00'))->asInteger());
        $this->assertSame(45296000, (new Timestamp('1970-01-01T12:34:56Z'))->asInteger());
    }

    /**
     * Regression: a timestamp string that names no offset was read in the
     * ambient default timezone, so the same literal meant a different instant
     * on every machine that sent it — while getValue() spelled the value back
     * out in UTC regardless, and `date` and `time` were anchored at UTC on both
     * sides. setUp() has put the process on America/New_York, five hours off.
     */
    public function testTimestampStringWithoutZoneIsReadAsUtc(): void {
        $value = new Timestamp('1970-01-01 12:34:56');

        $this->assertSame(45296000, $value->asInteger());
        $this->assertSame('1970-01-01 12:34:56.000+0000', $value->getValue());
    }

    public function testTimeTruncatesBelowMicrosecondPrecision(): void {
        // DateTimeImmutable carries microseconds; the type carries nanoseconds.
        $value = new Time(45296123456789);

        $this->assertSame('12:34:56.123456789', $value->asString());
        $this->assertSame('12:34:56.123456', $value->asDateTime()->format('H:i:s.u'));
    }
}
