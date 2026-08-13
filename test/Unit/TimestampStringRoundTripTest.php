<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Value\Date;
use Cassandra\Value\Timestamp;
use Cassandra\Value\ValueEncodeConfig;

/**
 * A timestamp's string form has to be one this class can read back.
 *
 * `timestamp` is a signed int64 of milliseconds, so a node can hand over a year
 * well past four digits — and PHP's date parser reads an unsigned year of five
 * or more digits as something else entirely. Timestamp::getValue() used to emit
 * one unsigned, so `AS_STRING` (the default encoding for this type) produced a
 * string that the constructor either refused outright or, for most values,
 * accepted as a completely different instant: reading such a column and writing
 * it back changed the value with nothing to show for it.
 *
 * {@see Date} already spelled the sign for the same reason, which is what these
 * tests hold Timestamp to.
 */
final class TimestampStringRoundTripTest extends AbstractUnitTestCase {
    protected function setUp(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Timestamp requires 64-bit integer');
        }
    }
    /**
     * @return array<string, array{0: int}>
     */
    public static function farFutureMillisecondsProvider(): array {
        return [
            'first millisecond past year 9999' => [253402300800000],
            'year 10000' => [253402300800001],
            'five-digit year' => [1000000000000000],
            'largest int64' => [PHP_INT_MAX],
        ];
    }

    /**
     * @return array<string, array{0: int}>
     */
    public static function ordinaryMillisecondsProvider(): array {
        return [
            'epoch' => [0],
            'positive' => [1700000000123],
            'negative' => [-1000],
            'last millisecond of year 9999' => [253402300799999],
            'year 0001' => [-62135596800000],
            'year 0000' => [-62167219200000],
            'negative year' => [-62335564800000],
            'smallest int64' => [PHP_INT_MIN],
        ];
    }

    /**
     * The read path an application gets by default has to be as safe as the
     * explicit one: AS_STRING is what ValueEncodeConfig::default() asks for.
     */
    public function testDefaultEncodingIsTheStringFormThatRoundTrips(): void {
        $config = ValueEncodeConfig::default();

        $milliseconds = 1000000000000000;

        /** @var string $encoded */
        $encoded = (new Timestamp($milliseconds))->asConfigured($config);

        $this->assertIsString($encoded);
        $this->assertSame($milliseconds, (new Timestamp($encoded))->asInteger());
    }

    /**
     * @dataProvider farFutureMillisecondsProvider
     */
    public function testFarFutureTimestampSurvivesItsOwnStringForm(int $milliseconds): void {
        $asString = (new Timestamp($milliseconds))->asString();

        $this->assertStringStartsWith('+', $asString, 'a year past four digits needs the sign PHP\'s parser reads it by');
        $this->assertSame($milliseconds, (new Timestamp($asString))->asInteger());
    }

    /**
     * The years an application actually stores keep the spelling they had, so
     * the sign is not paid for by everything else.
     *
     * @dataProvider ordinaryMillisecondsProvider
     */
    public function testOrdinaryTimestampKeepsItsUnsignedSpellingAndRoundTrips(int $milliseconds): void {
        $timestamp = new Timestamp($milliseconds);
        $asString = $timestamp->asString();

        $this->assertSame(
            $timestamp->asDateTime()->format('Y-m-d H:i:s.vO'),
            $asString,
            'a four-digit or negative year is spelled exactly as it was before'
        );
        $this->assertSame($milliseconds, (new Timestamp($asString))->asInteger());
    }

    /**
     * The behaviour Timestamp is being held to here is Date's, so the two are
     * checked against the same boundary rather than only against themselves.
     */
    public function testTimestampSpellsTheSignWhereDateDoes(): void {
        // 10000-01-01, as a raw `date` value and as milliseconds.
        $this->assertStringStartsWith('+', (new Date(2150416545))->asString());
        $this->assertStringStartsWith('+', (new Timestamp(253402300800000))->asString());

        // 9999-12-31, the last day either of them spells unsigned.
        $this->assertStringStartsNotWith('+', (new Date(2150416544))->asString());
        $this->assertStringStartsNotWith('+', (new Timestamp(253402300799999))->asString());
    }
}
