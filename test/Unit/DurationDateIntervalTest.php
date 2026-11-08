<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Value\Duration;
use DateInterval;
use DateTimeImmutable;

final class DurationDateIntervalTest extends AbstractUnitTestCase {
    public function testAnInvertedIntervalWithNegativeFieldsIsPositive(): void {
        // Regression: "--5" from format('%r%d') was read as nothing, turning the interval into 0s.
        $interval = DateInterval::createFromDateString('-5 days -3 hours');
        $this->assertInstanceOf(DateInterval::class, $interval);
        $interval->invert = 1;

        $this->assertSame('2020-01-06 03:00:00', (new DateTimeImmutable('2020-01-01 00:00:00'))->add($interval)->format('Y-m-d H:i:s'));
        $this->assertSame('5d3h', Duration::fromValue($interval)->asString());
    }

    public function testSignsOfOrdinaryIntervalsAreKept(): void {
        $negative = DateInterval::createFromDateString('-5 days -3 hours');
        $this->assertInstanceOf(DateInterval::class, $negative);
        $this->assertSame('-5d3h', Duration::fromValue($negative)->asString());

        $inverted = new DateInterval('P1M2DT3H');
        $inverted->invert = 1;
        $this->assertSame('-1mo2d3h', Duration::fromValue($inverted)->asString());
        $this->assertSame('1mo2d3h', Duration::fromValue(new DateInterval('P1M2DT3H'))->asString());
    }
}
