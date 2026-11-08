<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\Value\Date;
use Cassandra\Value\EncodeOption\DateEncodeOption;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use Cassandra\Value\Timestamp;
use Cassandra\Value\ValueEncodeConfig;
use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use PHPUnit\Framework\Attributes\DataProvider;

/**
 * Decoding a temporal cell skips the value object and gmdate() replaces
 * DateTimeImmutable, so both are pinned here against the slow formatting they
 * have to reproduce exactly.
 */
final class TemporalFastPathTest extends AbstractUnitTestCase {
    protected function setUp(): void {
        parent::setUp();

        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('date and timestamp require 64-bit PHP');
        }
    }

    /**
     * @return array<string, array{int}>
     */
    public static function dateValues(): array {
        $unixDay = 2_147_483_648;

        return [
            'minimum' => [0],
            'before year 0' => [2_146_764_119],
            'first day of year 0' => [2_146_764_120],
            'long before the epoch' => [$unixDay - 100_000],
            'day before the epoch' => [$unixDay - 1],
            'epoch' => [$unixDay],
            'leap day' => [$unixDay + 19_782],
            'last day of year 9999' => [2_150_416_544],
            'first day of year 10000' => [2_150_416_545],
            'maximum' => [4_294_967_295],
        ];
    }

    #[DataProvider('dateValues')]
    public function testDateCellsDecodeLikeTheSlowPath(int $value): void {
        $days = $value - 2_147_483_648;
        $interval = new DateInterval('P' . abs($days) . 'D');
        $base = new DateTimeImmutable('1970-01-01', new DateTimeZone('UTC'));
        $expected = ($days < 0 ? $base->sub($interval) : $base->add($interval))->format('Y-m-d');
        if ($value > 2_150_416_544) {
            $expected = '+' . $expected;
        }

        $this->assertSame($expected, Date::formatWireValue($value));
        $this->assertSame($expected, $this->decode(Type::DATE, pack('N', $value), new ValueEncodeConfig()));
        $this->assertSame($expected, (new Date($value))->getValue());
        $this->assertSame($value, $this->decode(
            Type::DATE,
            pack('N', $value),
            new ValueEncodeConfig(dateEncodeOption: DateEncodeOption::AS_INT),
        ));
    }

    #[DataProvider('timestampValues')]
    public function testTimestampCellsDecodeLikeTheSlowPath(int $milliseconds): void {
        $expected = (new DateTimeImmutable('@' . intdiv($milliseconds, 1000)))
            ->modify('+' . (($milliseconds % 1000) * 1000) . ' microseconds')
            ->format('Y-m-d H:i:s.vO');
        if (preg_match('/^\d{5,}-/', $expected) === 1) {
            $expected = '+' . $expected;
        }

        $this->assertSame($expected, Timestamp::formatMilliseconds($milliseconds));
        $this->assertSame($expected, $this->decode(Type::TIMESTAMP, pack('J', $milliseconds), new ValueEncodeConfig()));
        $this->assertSame($expected, (new Timestamp($milliseconds))->getValue());
        $this->assertSame($milliseconds, $this->decode(
            Type::TIMESTAMP,
            pack('J', $milliseconds),
            new ValueEncodeConfig(timestampEncodeOption: TimestampEncodeOption::AS_INT),
        ));
    }

    /**
     * @return array<string, array{int}>
     */
    public static function timestampValues(): array {
        return [
            'before year 0' => [-62_167_219_200_001],
            'first millisecond of year 0' => [-62_167_219_200_000],
            'negative with a fraction' => [-1_500],
            'just before the epoch' => [-1],
            'epoch' => [0],
            'whole second' => [1_700_000_000_000],
            'with a fraction' => [1_700_000_000_123],
            'leap day' => [1_709_210_096_789],
            'last millisecond of year 9999' => [253_402_300_799_999],
            'first millisecond of year 10000' => [253_402_300_800_000],
        ];
    }

    private function decode(Type $type, string $binary, ValueEncodeConfig $config): mixed {
        $reader = new StreamReader(pack('N', strlen($binary)) . $binary);

        return $reader->readValue(new SimpleTypeInfo($type), $config);
    }
}
