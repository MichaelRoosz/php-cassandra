<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Value;
use Cassandra\Value\EncodeOption\DateEncodeOption;
use Cassandra\Value\EncodeOption\DurationEncodeOption;
use Cassandra\Value\EncodeOption\TimeEncodeOption;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use Cassandra\Value\EncodeOption\VarintEncodeOption;
use Cassandra\Value\ValueEncodeConfig;
use DateInterval;
use DateTimeImmutable;

final class ValueEncodeOptionsTest extends AbstractIntegrationTestCase {
    public function testDateEncodeOptions(): void {

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Date requires 64-bit integer');
        }

        $this->connection->query('TRUNCATE test_enc_date');

        $val = '2021-01-02';
        $this->connection->query(
            'INSERT INTO test_enc_date (id, v) VALUES (?, ?)',
            [Value\Int32::fromValue(1), Value\Date::fromValue($val)]
        );

        // default (string)
        $res = $this->connection->query('SELECT v FROM test_enc_date WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertSame('2021-01-02', $row['v']);

        // as int
        $cfg = new ValueEncodeConfig(dateEncodeOption: DateEncodeOption::AS_INT);
        $res = $this->connection->query('SELECT v FROM test_enc_date WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsInt($row['v']);
        $this->assertSame(2147502277, $row['v']);

        // as DateTimeImmutable
        $cfg = new ValueEncodeConfig(dateEncodeOption: DateEncodeOption::AS_DATETIME_IMMUTABLE);
        $res = $this->connection->query('SELECT v FROM test_enc_date WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertInstanceOf(DateTimeImmutable::class, $row['v']);
        $this->assertSame('2021-01-02', $row['v']->format('Y-m-d'));
    }

    public function testDurationEncodeOptions(): void {

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Duration requires 64-bit integer');
        }

        $this->connection->query('TRUNCATE test_enc_duration');

        $val = '1y2mo3d4h5m6s7ms8us9ns';
        $this->connection->query(
            'INSERT INTO test_enc_duration (id, v) VALUES (?, ?)',
            [Value\Int32::fromValue(1), Value\Duration::fromValue($val)]
        );

        // default (string)
        $res = $this->connection->query('SELECT v FROM test_enc_duration WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertSame($val, $row['v']);

        // as native array
        $cfg = new ValueEncodeConfig(durationEncodeOption: DurationEncodeOption::AS_NATIVE_VALUE);
        $res = $this->connection->query('SELECT v FROM test_enc_duration WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsArray($row['v']);
        $this->assertArrayHasKey('months', $row['v']);
        $this->assertArrayHasKey('days', $row['v']);
        $this->assertArrayHasKey('nanoseconds', $row['v']);
        $this->assertSame(14, $row['v']['months']);
        $this->assertSame(3, $row['v']['days']);
        $this->assertSame(14706007008009, $row['v']['nanoseconds']);

        // as DateInterval
        $cfg = new ValueEncodeConfig(durationEncodeOption: DurationEncodeOption::AS_DATEINTERVAL);
        $res = $this->connection->query('SELECT v FROM test_enc_duration WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertInstanceOf(DateInterval::class, $row['v']);
        $this->assertSame('P1Y2M3DT4H5M6S7008US', $row['v']->format('P%yY%mM%dDT%hH%iM%sS%fUS'));

        // as DateInterval string
        $cfg = new ValueEncodeConfig(durationEncodeOption: DurationEncodeOption::AS_DATEINTERVAL_STRING);
        $res = $this->connection->query('SELECT v FROM test_enc_duration WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsString($row['v']);
        $this->assertSame('P1Y2M3DT4H5M6S', $row['v']);
    }

    public function testTimeEncodeOptions(): void {

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Time requires 64-bit integer');
        }

        $this->connection->query('TRUNCATE test_enc_time');

        $val = '12:34:56.789012345';
        $this->connection->query(
            'INSERT INTO test_enc_time (id, v) VALUES (?, ?)',
            [Value\Int32::fromValue(1), Value\Time::fromValue($val)]
        );

        // default (string)
        $res = $this->connection->query('SELECT v FROM test_enc_time WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertSame('12:34:56.789012345', $row['v']);

        // as int
        $cfg = new ValueEncodeConfig(timeEncodeOption: TimeEncodeOption::AS_INT);
        $res = $this->connection->query('SELECT v FROM test_enc_time WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsInt($row['v']);
        $this->assertSame(45296789012345, $row['v']);

        // as DateTimeImmutable
        $cfg = new ValueEncodeConfig(timeEncodeOption: TimeEncodeOption::AS_DATETIME_IMMUTABLE);
        $res = $this->connection->query('SELECT v FROM test_enc_time WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertInstanceOf(DateTimeImmutable::class, $row['v']);
        $this->assertSame('12:34:56.789012', $row['v']->format('H:i:s.u'));
    }

    public function testTimestampEncodeOptions(): void {

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Timestamp requires 64-bit integer');
        }

        $this->connection->query('TRUNCATE test_enc_timestamp');

        $val = '2021-01-01 12:23:57.123+0000';
        $this->connection->query(
            'INSERT INTO test_enc_timestamp (id, v) VALUES (?, ?)',
            [Value\Int32::fromValue(1), Value\Timestamp::fromValue($val)]
        );

        // default (string)
        $res = $this->connection->query('SELECT v FROM test_enc_timestamp WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertSame('2021-01-01 12:23:57.123+0000', $row['v']);

        // as int (milliseconds)
        $cfg = new ValueEncodeConfig(timestampEncodeOption: TimestampEncodeOption::AS_INT);
        $res = $this->connection->query('SELECT v FROM test_enc_timestamp WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsInt($row['v']);
        $this->assertSame(1609503837123, $row['v']);

        // as DateTimeImmutable
        $cfg = new ValueEncodeConfig(timestampEncodeOption: TimestampEncodeOption::AS_DATETIME_IMMUTABLE);
        $res = $this->connection->query('SELECT v FROM test_enc_timestamp WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertInstanceOf(DateTimeImmutable::class, $row['v']);
        $this->assertSame('2021-01-01 12:23:57.123+0000', $row['v']->format('Y-m-d H:i:s.vO'));
    }

    public function testVarintEncodeOptions(): void {
        $this->connection->query('TRUNCATE test_enc_varint');

        // choose a large number outside PHP_INT range to force string behavior
        $large = '1234567890123456789012345678901234567890';
        $this->connection->query(
            'INSERT INTO test_enc_varint (id, v) VALUES (?, ?)',
            [Value\Int32::fromValue(1), Value\Varint::fromValue($large)]
        );

        // default (string)
        $res = $this->connection->query('SELECT v FROM test_enc_varint WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsString($row['v']);
        $this->assertSame($large, (string) $row['v']);

        // as string
        $cfg = new ValueEncodeConfig(varintEncodeOption: VarintEncodeOption::AS_STRING);
        $res = $this->connection->query('SELECT v FROM test_enc_varint WHERE id = ?', [Value\Int32::fromValue(1)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsString($row['v']);
        $this->assertSame($large, (string) $row['v']);

        // as int (use a value that fits)
        $this->connection->query('TRUNCATE test_enc_varint');
        $fit = PHP_INT_MAX - 1;
        $this->connection->query(
            'INSERT INTO test_enc_varint (id, v) VALUES (?, ?)',
            [Value\Int32::fromValue(2), Value\Varint::fromValue($fit)]
        );

        $cfg = new ValueEncodeConfig(varintEncodeOption: VarintEncodeOption::AS_INT);
        $res = $this->connection->query('SELECT v FROM test_enc_varint WHERE id = ?', [Value\Int32::fromValue(2)])
            ->asRowsResult();
        $res->configureValueEncoding($cfg);
        $row = $res->fetch();
        $this->assertIsArray($row);
        $this->assertIsInt($row['v']);
        $this->assertSame($fit, $row['v']);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);

        $conn->query('CREATE TABLE IF NOT EXISTS test_enc_date (id int PRIMARY KEY, v date)');
        $conn->query('CREATE TABLE IF NOT EXISTS test_enc_time (id int PRIMARY KEY, v time)');
        $conn->query('CREATE TABLE IF NOT EXISTS test_enc_timestamp (id int PRIMARY KEY, v timestamp)');
        $conn->query('CREATE TABLE IF NOT EXISTS test_enc_duration (id int PRIMARY KEY, v duration)');
        $conn->query('CREATE TABLE IF NOT EXISTS test_enc_varint (id int PRIMARY KEY, v varint)');

        $conn->disconnect();
    }
}
