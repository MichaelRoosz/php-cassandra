<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ValueException;
use Cassandra\Value\Date;
use DateTimeImmutable;
use DateTimeZone;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * Regression: a date past either end of the range wrapped around to the other end
 * instead of being refused, because the day count went through DateInterval,
 * which holds days in 32 bits.
 */
final class DateRangeTest extends AbstractUnitTestCase {
    protected function setUp(): void {
        parent::setUp();

        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }
    }

    /**
     * @return array<string, array{string, int}>
     */
    public static function boundaries(): array {
        return [
            'last day' => ['5881580-07-11', 4_294_967_295],
            'first day' => ['-5877641-06-23', 0],
            'epoch' => ['1970-01-01', 2_147_483_648],
            'day before the epoch' => ['1969-12-31', 2_147_483_647],
        ];
    }

    /**
     * @return array<string, array{string}>
     */
    public static function outOfRange(): array {
        return [
            'day after the last' => ['5881580-07-12'],
            'well past the last' => ['5881581-01-01'],
            'day before the first' => ['-5877641-06-22'],
            'well before the first' => ['-5877642-01-01'],
        ];
    }

    #[DataProvider('outOfRange')]
    public function testADateBeyondTheRangeIsRefused(string $date): void {
        $this->expectException(ValueException::class);
        new Date($date);
    }

    #[DataProvider('outOfRange')]
    public function testADateTimeBeyondTheRangeIsRefused(string $date): void {
        $this->expectException(ValueException::class);
        new Date(self::dateTime($date, 'UTC', 0));
    }

    #[DataProvider('boundaries')]
    public function testTheEndsOfTheRangeAreStoredExactly(string $date, int $wireValue): void {
        $this->assertSame($wireValue, (new Date($date))->asInteger());
        $this->assertSame($wireValue, (new Date(self::dateTime($date, 'Pacific/Kiritimati', 13)))->asInteger());
        $this->assertSame(ltrim($date, '+'), ltrim((new Date($wireValue))->getValue(), '+'));
    }

    /**
     * Built from fields, since PHP's parser misreads years of more than four digits.
     */
    private static function dateTime(string $date, string $zone, int $hour): DateTimeImmutable {
        if (preg_match('/^([+-]?\d+)-(\d{2})-(\d{2})$/', $date, $m) !== 1) {
            self::fail('not a date: ' . $date);
        }

        return (new DateTimeImmutable('now', new DateTimeZone($zone)))->setDate((int) $m[1], (int) $m[2], (int) $m[3])->setTime($hour, 0);
    }
}
