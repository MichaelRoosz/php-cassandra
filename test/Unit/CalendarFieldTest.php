<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Value\Date;
use Cassandra\Value\Timestamp;

/**
 * Regression: PHP rolls an impossible calendar field into the next one, so
 * 2020-02-30 was stored as March 1.
 */
final class CalendarFieldTest extends AbstractUnitTestCase {
    public function testAnImpossibleDateIsRefused(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value);
        Date::fromValue('2021-02-29');
    }

    public function testAnImpossibleTimestampIsRefused(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_TIMESTAMP_INVALID_VALUE_TYPE->value);
        Timestamp::fromValue('2020-02-30 00:00:00');
    }

    public function testRealDatesAreKept(): void {
        $this->assertSame('2020-02-29', Date::fromValue('2020-02-29')->getValue());
        $this->assertSame('2020-01-01 08:00:00.000+0000', Timestamp::fromValue('2020-01-01T10:00:00+02:00')->getValue());
    }
}
