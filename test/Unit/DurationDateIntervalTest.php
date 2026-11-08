<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Value\Duration;
use DateInterval;
use DateTimeImmutable;

final class DurationDateIntervalTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{string}>
     */
    public static function stringsThatAreNoDuration(): array {
        return [
            'unknown units' => ['1x2y'],
            'words' => ['5 days'],
            'trailing text' => ['1h junk'],
            'empty' => [''],
        ];
    }
    public function testAnInvertedIntervalWithNegativeFieldsIsPositive(): void {
        // Regression: "--5" from format('%r%d') was read as nothing, turning the interval into 0s.
        $interval = DateInterval::createFromDateString('-5 days -3 hours');
        $this->assertInstanceOf(DateInterval::class, $interval);
        $interval->invert = 1;

        $this->assertSame('2020-01-06 03:00:00', (new DateTimeImmutable('2020-01-01 00:00:00'))->add($interval)->format('Y-m-d H:i:s'));
        $this->assertSame('5d3h', Duration::fromValue($interval)->asString());
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('stringsThatAreNoDuration')]
    public function testAStringThatIsNoDurationIsRefused(string $value): void {
        // Regression: every component is optional, so these parsed as 0s or as their prefix.
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_DURATION_INVALID_VALUE_TYPE->value);
        Duration::fromValue($value);
    }

    public function testAZeroDurationConvertsToADateInterval(): void {
        // Regression: the empty interval string made PHP 8.3+ throw a native exception.
        $this->assertSame('0s', Duration::fromValue(Duration::fromValue('0s')->asDateInterval())->asString());
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
