<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Value\Duration;

class DurationStringTest extends AbstractUnitTestCase {
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
