<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Value\Duration;
use DateInterval;

class DurationStringTest extends AbstractUnitTestCase {
    public function testConstructionFromDateIntervalRejectsMismatchedSigns(): void {
        // A DateInterval can hold components that disagree in sign — this is how
        // PHP builds one — but the wire encoding cannot express it, so the
        // server rejects it. The array form was checked and these two were not,
        // so such a duration used to be encoded and sent.
        $this->skipWithout64BitIntegers();

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_DURATION_SIGN_MISMATCH->value);

        Duration::fromValue(DateInterval::createFromDateString('-1 month +5 days'));
    }

    public function testConstructionFromStringRejectsOutOfInt32Range(): void {
        // "months" is an int32 on the wire. Unchecked, this was accepted here
        // and only failed later inside getBinary(), as a VIntCodecException from
        // the encoder rather than as the value error it is.
        $this->skipWithout64BitIntegers();

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_DURATION_MONTHS_OUT_OF_RANGE->value);

        Duration::fromValue('999999999999y');
    }

    public function testDateIntervalIsZeroForADurationWithNothingToCarry(): void {
        // A DateInterval carries microseconds at best, so a duration that is
        // zero — or whose only component is below that resolution — has no
        // components to name. The relative-date string stayed empty and
        // DateInterval::createFromDateString() rejects that, so `0s`, an
        // ordinary value of the type, could not be decoded at all under
        // DurationEncodeOption::AS_DATEINTERVAL. Sub-microsecond precision was
        // already dropped whenever any other component was present; this only
        // makes the case where none is behave the same way.
        $this->skipWithout64BitIntegers();

        foreach ([0, 999, -999] as $nanoseconds) {
            $duration = Duration::fromValue(['months' => 0, 'days' => 0, 'nanoseconds' => $nanoseconds]);

            $interval = $duration->asDateInterval();

            $this->assertSame('+0-0-0 0:0:0', $interval->format('%R%y-%m-%d %h:%i:%s'), (string) $nanoseconds);
            $this->assertSame(
                ['months' => 0, 'days' => 0, 'nanoseconds' => 0],
                Duration::fromValue($interval)->asNativeValue(),
                (string) $nanoseconds
            );
        }
    }

    public function testDateIntervalStringKeepsTheSign(): void {
        // The regression this exists for: the sign was computed and then used
        // only to take the absolute value of each component, so a negative
        // duration and its positive counterpart produced the same string — and
        // reading that back flipped the sign.
        $this->skipWithout64BitIntegers();

        $negative = Duration::fromValue(['months' => -14, 'days' => -3, 'nanoseconds' => -5_000_000_000]);
        $positive = Duration::fromValue(['months' => 14, 'days' => 3, 'nanoseconds' => 5_000_000_000]);

        $this->assertSame('-P1Y2M3DT5S', $negative->asDateIntervalString());
        $this->assertSame('P1Y2M3DT5S', $positive->asDateIntervalString());
    }

    public function testDateIntervalStringRoundTrips(): void {
        $this->skipWithout64BitIntegers();

        $values = [
            ['months' => -14, 'days' => -3, 'nanoseconds' => -5_000_000_000],
            ['months' => 14, 'days' => 3, 'nanoseconds' => 5_000_000_000],
            ['months' => -1, 'days' => 0, 'nanoseconds' => 0],
            ['months' => 0, 'days' => -7, 'nanoseconds' => 0],
            ['months' => 0, 'days' => 0, 'nanoseconds' => -3_600_000_000_000],
            ['months' => 25, 'days' => 0, 'nanoseconds' => 3_661_000_000_000],
            ['months' => 0, 'days' => 0, 'nanoseconds' => 0],
        ];

        foreach ($values as $value) {
            $string = Duration::fromValue($value)->asDateIntervalString();

            $this->assertSame(
                $value,
                Duration::fromValue($string)->asNativeValue(),
                'round trip of ' . $string
            );
        }
    }

    public function testDateIntervalStringSpellsASubSecondOnlyDurationAsZero(): void {
        // ISO 8601 durations carry whole seconds only, so a duration whose whole
        // time part is sub-second has no time components to write. The
        // designator used to be emitted anyway, producing the bare 'PT' — not a
        // duration any reader accepts, this class's own parser included.
        $this->skipWithout64BitIntegers();

        foreach ([500_000_000, -500_000_000, 999_999_999] as $nanoseconds) {
            $duration = Duration::fromValue(['months' => 0, 'days' => 0, 'nanoseconds' => $nanoseconds]);

            $this->assertSame('PT0S', $duration->asDateIntervalString(), (string) $nanoseconds);
            $this->assertSame(
                ['months' => 0, 'days' => 0, 'nanoseconds' => 0],
                Duration::fromValue($duration->asDateIntervalString())->asNativeValue()
            );
        }
    }

    public function testStringFormsAcceptAnExplicitSign(): void {
        $this->skipWithout64BitIntegers();

        $this->assertSame(
            ['months' => -12, 'days' => 0, 'nanoseconds' => 0],
            Duration::fromValue('-P1Y')->asNativeValue()
        );

        $this->assertSame(
            ['months' => 12, 'days' => 0, 'nanoseconds' => 0],
            Duration::fromValue('+P1Y')->asNativeValue()
        );

        $this->assertSame(
            ['months' => 0, 'days' => -14, 'nanoseconds' => 0],
            Duration::fromValue('-P2W')->asNativeValue()
        );
    }

    private function skipWithout64BitIntegers(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Duration requires 64-bit integer');
        }
    }
}
