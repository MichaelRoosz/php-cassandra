<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Type;
use DateInterval;
use DateTimeImmutable;
use PHPUnit\Framework\TestCase;

/**
 * Integration test to verify that data types round-trip correctly through Cassandra.
 * Tests that values inserted via PHP match when selected back, ensuring compatibility
 * with cqlsh and proper serialization/deserialization.
 */
/**
 * Types to test (Cassandra v5):
 * - ascii                  *Implemented
 * - bigint                 *Implemented
 * - blob                   *Implemented
 * - boolean                *Implemented
 * - counter                *Implemented
 * - list                   *Implemented
 * - map                    *Implemented
 * - set                    *Implemented
 * - custom                 *Implemented
 * - date                   *Implemented
 * - decimal                *Implemented
 * - double                 *Implemented
 * - duration               *Implemented
 * - float                  *Implemented
 * - inet                   *Implemented
 * - integer                *Implemented
 * - smallint               *Implemented
 * - time                   *Implemented
 * - timestamp              *Implemented 
 * - timeuuid               *Implemented
 * - tinyint                *Implemented
 * - tuple                  *Implemented
 * - udt                    *Implemented
 * - uuid                   *Implemented
 * - varchar                *Implemented
 * - varint                 *Implemented
 * - vector                 *Implemented
 * 
 * todo: 
 *  - test frozen where possible (list, set, map, udt)
 *      + tuple and vector are always frozen
 *  - test reversed where possible, if relevant
 */
final class DataTypeRoundtripTest extends TestCase {
    private Connection $connection;
    private string $dumpFile = './table_dump.csv';
    private string $testKeyspace = 'datatype_test';

    protected function setUp(): void {
        parent::setUp();
        $this->connection = $this->newConnection('system');

        $this->connection->querySync(
            "DROP KEYSPACE IF EXISTS {$this->testKeyspace}"
        );

        $this->connection->querySync(
            "CREATE KEYSPACE IF NOT EXISTS {$this->testKeyspace} WITH REPLICATION = " .
            "{'class': 'SimpleStrategy', 'replication_factor': 1}"
        );

        $this->connection = $this->newConnection($this->testKeyspace);
    }

    protected function tearDown(): void {
        $this->connection = $this->newConnection('system');
        $this->connection->querySync("DROP KEYSPACE IF EXISTS {$this->testKeyspace}");

        if (file_exists($this->dumpFile)) {
            unlink($this->dumpFile);
        }

        parent::tearDown();
    }

    public function testAsciiRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_ascii (id int PRIMARY KEY, value varchar)'
        );

        $testValues = [
            '',
            'a',
            'Hello World',
            'ASCII only: !@#$%^&*()_+-=[]{}|;:\'",.<>?/~`',
            '12345',
            'UPPERCASE',
            'lowercase',
            'MiXeD cAsE',
            str_repeat('x', 1000), // Long string
            '1Newlines\nand\ttabs',
            "2Newlines\nand\ttabs",
            'NULL and null and None',
            json_encode(['key' => 'value', 'number' => 42]),
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_ascii (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Ascii($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_ascii WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'Ascii value should round-trip correctly');

        }
        $this->compareWithCqlsh('test_ascii', 'id', 'value', $testValues, 'ascii');
    }

    public function testBigintRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_bigint (id int PRIMARY KEY, value bigint)'
        );

        $testValues = [
            0,
            1,
            -1,
            PHP_INT_MAX,
            PHP_INT_MIN,
            9223372036854775807, // max bigint
            -9223372036854775807 - 1, // min bigint
            1000000000000000,
            -1000000000000000,
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_bigint (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Bigint($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_bigint WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, "Bigint value $testValue should round-trip correctly");
        }

        $this->compareWithCqlsh('test_bigint', 'id', 'value', $testValues, 'bigint');
    }

    public function testBlobRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_blob (id int PRIMARY KEY, value blob)'
        );

        $testValues = [
            '',
            'binary data',
            "\x00\x01\x02\x03\xFF",
            random_bytes(100),
            pack('H*', 'deadbeef'),
            str_repeat("\x00", 1000),
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_blob (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Blob($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_blob WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'Blob value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_blob', 'id', 'value', $testValues, 'blob');
    }

    public function testBooleanRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_boolean (id int PRIMARY KEY, value boolean)'
        );

        $testValues = [true, false];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_boolean (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Boolean($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_boolean WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'Boolean value ' . ($testValue ? 'true' : 'false') . ' should round-trip correctly');

        }

        $this->compareWithCqlsh('test_boolean', 'id', 'value', $testValues, 'boolean');
    }

    public function testCounterRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_counter (id int PRIMARY KEY, value counter)'
        );

        $finalValues = [
            1,
            -1,
            1000,
            -1000,
            42,
            -42,
        ];

        foreach ($finalValues as $index => $delta) {
            $this->connection->querySync(
                'UPDATE test_counter SET value = value + ? WHERE id = ?',
                [new Type\Counter($delta), new Type\Integer($index)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_counter WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($delta, $retrievedValue,
                "Counter value $delta should round-trip correctly after single update");
        }

        $this->compareWithCqlsh('test_counter', 'id', 'value', $finalValues, 'bigint');
    }

    public function testCustomRoundtrip(): void {
        $this->connection->querySync(
            "CREATE TABLE IF NOT EXISTS test_custom (id int PRIMARY KEY, value 'org.apache.cassandra.db.marshal.BytesType')"
        );

        $testValues = [
            ['value' => '', 'cql' => '0x', 'javaClass' => 'org.apache.cassandra.db.marshal.BytesType'],
            ['value' => 'hello', 'cql' => '0x68656c6c6f', 'javaClass' => 'org.apache.cassandra.db.marshal.BytesType'],
            ['value' => 'binary data', 'cql' => '0x62696e6172792064617461', 'javaClass' => 'org.apache.cassandra.db.marshal.BytesType'],
            ['value' => pack('H*', 'deadbeef'), 'cql' => '0xdeadbeef', 'javaClass' => 'org.apache.cassandra.db.marshal.BytesType'],
            ['value' => "\x00\x01\x02\x03\xFF", 'cql' => '0x00010203ff', 'javaClass' => 'org.apache.cassandra.db.marshal.BytesType'],
        ];

        foreach ($testValues as $index => $config) {
            $this->connection->querySync(
                'INSERT INTO test_custom (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Custom($config['value'], $config['javaClass'])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_custom WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame(bin2hex($config['value']), bin2hex($retrievedValue), 'Custom value should round-trip correctly through Cassandra');
        }

        $this->compareWithCqlsh('test_custom', 'id', 'value', array_map(fn($value) => ($value['cql']), $testValues), 'custom');
    }

    public function testDateRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_date (id int PRIMARY KEY, value date)'
        );

        $testValues = [
            '1970-01-01' => ['php' => '1970-01-01', 'cql' => '1970-01-01'],
            '1970-01-02' => ['php' => '1970-01-02', 'cql' => '1970-01-02'],
            '1969-12-31' => ['php' => '1969-12-31', 'cql' => '1969-12-31'],
            '1971-01-01' => ['php' => '1971-01-01', 'cql' => '1971-01-01'],
            '1969-01-01' => ['php' => '1969-01-01', 'cql' => '1969-01-01'],
            '2020-01-01' => ['php' => '2020-01-01', 'cql' => '2020-01-01'],
            '-2020-01-02' => ['php' => '-2020-01-02', 'cql' => '-1457317'],
            '-5877641-06-24' => ['php' => '-5877641-06-24', 'cql' => '-2147483647'],
            '-5877641-06-23' => ['php' => '-5877641-06-23', 'cql' => '-2147483648'], // min date
            '0001-01-01' => ['php' => '0001-01-01', 'cql' => '0001-01-01'],
            '9999-07-10' => ['php' => '9999-07-10', 'cql' => '9999-07-10'],
            '10000-07-10' => ['php' => '+10000-07-10', 'cql' => '2933088'],
            '5181580-07-10' => ['php' => '+5181580-07-10', 'cql' => '1891813896'],
            '5881580-07-11' => ['php' => '+5881580-07-11', 'cql' => '2147483647'], // max date
        ];

        // test with string values
        foreach (array_keys($testValues) as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_date (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Date($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_date WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValues[$testValue]['php'], $retrievedValue, "Date value $testValue should round-trip correctly");
        }

        $cqlValues = array_map(fn($value) => $value['cql'], array_values($testValues));
        $this->compareWithCqlsh('test_date', 'id', 'value', $cqlValues, 'date');

        // Test with DateTimeImmutable objects
        $dateTimeValues = [];
        foreach ($testValues as $input => $output) {

            if (!is_string($input)) {
                continue;
            }

            $firstChar = substr($input, 0, 1);
            if ($firstChar !== '+' && $firstChar !== '-') {
                $input = '+' . $input; // Ensure the date string has a sign
            }

            $dateTimeValues[] = [
                'input' => new DateTimeImmutable($input),
                'output' => $output['php'],
            ];
        }

        foreach ($dateTimeValues as $index => $config) {
            $this->connection->querySync(
                'INSERT INTO test_date (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Date($config['input'])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_date WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($config['output'], $retrievedValue,
                "Date value {$config['input']->format('Y-m-d')} should round-trip correctly");
        }
    }

    public function testDecimalRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_decimal (id int PRIMARY KEY, value decimal)'
        );

        $testValues = [
            '0' => ['php' => '0', 'cql' => '0'],
            '0.0' => ['php' => '0.0', 'cql' => '0.0'],
            '1' => ['php' => '1', 'cql' => '1'],
            '1.0' => ['php' => '1.0', 'cql' => '1.0'],
            '10' => ['php' => '10', 'cql' => '10'],
            '-1' => ['php' => '-1', 'cql' => '-1'],
            '-1.0' => ['php' => '-1.0', 'cql' => '-1.0'],
            '-10' => ['php' => '-10', 'cql' => '-10'],
            '123.456' => ['php' => '123.456', 'cql' => '123.456'],
            '-123.456' => ['php' => '-123.456', 'cql' => '-123.456'],
            '999999999999999999999999999999' => ['php' => '999999999999999999999999999999', 'cql' => '999999999999999999999999999999'],
            '-999999999999999999999999999999' => ['php' => '-999999999999999999999999999999', 'cql' => '-999999999999999999999999999999'],
            '999999999999999999999999999999.999999999999999999999999999999' => ['php' => '999999999999999999999999999999.999999999999999999999999999999', 'cql' => '999999999999999999999999999999.999999999999999999999999999999'],
            '-999999999999999999999999999999.999999999999999999999999999999' => ['php' => '-999999999999999999999999999999.999999999999999999999999999999', 'cql' => '-999999999999999999999999999999.999999999999999999999999999999'],
            '0.000000000000000000000000000001' => ['php' => '0.000000000000000000000000000001', 'cql' => '1E-30'],
            '-0.000000000000000000000000000001' => ['php' => '-0.000000000000000000000000000001', 'cql' => '-1E-30'],
            '3.14159265358979323846' => ['php' => '3.14159265358979323846', 'cql' => '3.14159265358979323846'],
            '42' => ['php' => '42', 'cql' => '42'],
            '-42' => ['php' => '-42', 'cql' => '-42'],
            '123456789.987654321' => ['php' => '123456789.987654321', 'cql' => '123456789.987654321'],
            '-123456789.987654321' => ['php' => '-123456789.987654321', 'cql' => '-123456789.987654321'],
            '.11111' => ['php' => '0.11111', 'cql' => '0.11111'],
            '-.11111' => ['php' => '-0.11111', 'cql' => '-0.11111'],
            '11111.' => ['php' => '11111', 'cql' => '11111'],
            '-11111.' => ['php' => '-11111', 'cql' => '-11111'],
        ];

        foreach (array_keys($testValues) as $index => $testValue) {

            $this->connection->querySync(
                'INSERT INTO test_decimal (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Decimal($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_decimal WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValues[$testValue]['php'], $retrievedValue, "Decimal value $testValue should round-trip correctly");
        }

        $cqlValues = array_map(fn($value) => $value['cql'], array_values($testValues));
        $this->compareWithCqlsh('test_decimal', 'id', 'value', $cqlValues, 'decimal');
    }

    public function testDoubleRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_double (id int PRIMARY KEY, value double)'
        );

        $testValues = [
            0.0,
            1.0,
            -1.0,
            3.14159265359,
            -3.14159265359,
            1.7976931348623157E+308, // near PHP_FLOAT_MAX
            2.2250738585072014E-308, // near PHP_FLOAT_MIN
            42.42,
            -42.42,
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_double (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Double($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_double WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertEqualsWithDelta($testValue, $retrievedValue, max($testValue * 0.01, 0.000001), "Double value $testValue should round-trip correctly");

        }

        $this->compareWithCqlsh('test_double', 'id', 'value', $testValues, 'double');
    }

    public function testDurationRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_duration (id int PRIMARY KEY, value duration)'
        );

        $testValues = [
            '89h4m48s' => ['php' => '89h4m48s', 'cql' => '89h4m48s', 'dateinterval' => 'PT89H4M48S'],
            'PT89H8M53S' => ['php' => '89h8m53s', 'cql' => '89h8m53s', 'dateinterval' => 'PT89H8M53S'],
            'P12W' => ['php' => '84d', 'cql' => '84d', 'dateinterval' => 'P84D'],
            'P0000-00-00T89:09:09' => ['php' => '89h9m9s', 'cql' => '89h9m9s', 'dateinterval' => 'PT89H9M9S'],
            'PT0S' => ['php' => '0s', 'cql' => '', 'dateinterval' => 'PT0S'],
            '-1h2m3s' => ['php' => '-1h2m3s', 'cql' => '-1h2m3s', 'dateinterval' => 'PT1H2M3S'],
            'P123Y456M789DT12H34M56S' => ['php' => '161y789d12h34m56s', 'cql' => '161y789d12h34m56s', 'dateinterval' => 'P161Y789DT12H34M56S'],
            '161y2mo112w5d12h34m56s' => ['php' => '161y2mo789d12h34m56s', 'cql' => '161y2mo789d12h34m56s', 'dateinterval' => 'P161Y2M789DT12H34M56S'],
            'P15M' => ['php' => '1y3mo', 'cql' => '1y3mo', 'dateinterval' => 'P1Y3M'],
            'P2Y' => ['php' => '2y', 'cql' => '2y', 'dateinterval' => 'P2Y'],
            'P2Y112W' => ['php' => '2y784d', 'cql' => '2y784d', 'dateinterval' => 'P2Y784D'],
            'P100D' => ['php' => '100d', 'cql' => '100d', 'dateinterval' => 'P100D'],
            'PT12H' => ['php' => '12h', 'cql' => '12h', 'dateinterval' => 'PT12H'],
            'PT3600S' => ['php' => '1h', 'cql' => '1h', 'dateinterval' => 'PT1H'],
            '-0s' => ['php' => '0s', 'cql' => '', 'dateinterval' => 'PT0S'],
        ];

        // test with string values
        foreach (array_keys($testValues) as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_duration (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Duration($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_duration WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame(
                $testValues[$testValue]['php'],
                $retrievedValue,
                "Duration value $testValue should round-trip correctly"
            );

            $this->assertSame(
                $testValues[$testValue]['dateinterval'],
                (new Type\Duration($retrievedValue))->asDateIntervalString(),
                "Duration value $testValue should round-trip correctly as DateInterval string"
            );
        }

        $cqlValues = array_map(fn($value) => $value['cql'], array_values($testValues));
        $this->compareWithCqlsh('test_duration', 'id', 'value', $cqlValues, 'duration');

        // Test with DateInterval objects
        $dateIntervalValues = [];
        foreach ($testValues as $input => $output) {

            if (
                !is_string($input)
                || !str_starts_with($input, 'P')
                || str_contains($input, '-')
                || str_contains($input, ':')
            ) {
                continue;
            }

            $dateIntervalValues[] = [
                'input' => new DateInterval($input),
                'output' => $output['php'],
            ];
        }

        foreach ($dateIntervalValues as $index => $config) {
            $this->connection->querySync(
                'INSERT INTO test_duration (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Duration($config['input'])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_duration WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($config['output'], $retrievedValue,
                "Duration value {$config['input']->format('P%yY%mM%dDT%hH%iM%sS')} should round-trip correctly");
        }
    }

    public function testFloat32Roundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_float32 (id int PRIMARY KEY, value float)'
        );

        $testValues = [
            0.0,
            1.0,
            -1.0,
            3.14159,
            -3.14159,
            3.4028235E38,  // near max float32
            1.175494E-38,  // near min positive float32
            42.42,
            -42.42,
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_float32 (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Float32($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_float32 WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            // Use delta comparison for floats (float32 has less precision)
            $this->assertEqualsWithDelta($testValue, $retrievedValue, max($testValue * 0.01, 0.0001), "Float32 value $testValue should round-trip correctly");

        }

        $this->compareWithCqlsh('test_float32', 'id', 'value', $testValues, 'float');
    }

    public function testInetRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_inet (id int PRIMARY KEY, value inet)'
        );

        $testValues = [
            '127.0.0.1',
            '192.168.1.1',
            '10.0.0.1',
            '255.255.255.255',
            '0.0.0.0',
            '::1',
            '2001:db8::1',
            'fe80::1',
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_inet (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Inet($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_inet WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, "Inet value $testValue should round-trip correctly");
        }

        $this->compareWithCqlsh('test_inet', 'id', 'value', $testValues, 'inet');
    }

    public function testIntegerRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_integer (id int PRIMARY KEY, value int)'
        );

        $testValues = [
            0,
            1,
            -1,
            Type\Integer::VALUE_MAX,
            Type\Integer::VALUE_MIN,
            42,
            -42,
            1000000,
            -1000000,
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_integer (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Integer($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_integer WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Integer value $testValue should round-trip correctly");

        }

        $this->compareWithCqlsh('test_integer', 'id', 'value', $testValues, 'integer');
    }

    public function testListRoundtrip(): void {
        // Test list<varchar>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_list_varchar (id int PRIMARY KEY, value list<varchar>)'
        );

        $testValues = [
            [],
            ['hello'],
            ['hello', 'world'],
            ['', 'test', '', 'empty'],
            ['unicode', '🚀', '中文', 'العربية'],
            array_fill(0, 100, 'repeated'),
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_list_varchar (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\CollectionList($testValue, Type::VARCHAR)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_list_varchar WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $valueToCheck = $testValue === [] ? null : $testValue;

            if ($valueToCheck) {
                sort($valueToCheck);
            }

            if ($retrievedValue) {
                sort($retrievedValue);
            }

            $this->assertSame($valueToCheck, $retrievedValue, 'List<varchar> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_list_varchar', 'id', 'value', $testValues, 'list');

        // Test list<int>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_list_int (id int PRIMARY KEY, value list<int>)'
        );

        $intTestValues = [
            [],
            [1],
            [1, 2, 3],
            [0, -1, 42, -42],
            [Type\Integer::VALUE_MIN, Type\Integer::VALUE_MAX],
            array_fill(0, 50, 123),
        ];

        foreach ($intTestValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_list_int (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\CollectionList($testValue, Type::INT)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_list_int WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $valueToCheck = $testValue === [] ? null : $testValue;

            if ($valueToCheck) {
                sort($valueToCheck);
            }

            if ($retrievedValue) {
                sort($retrievedValue);
            }

            $this->assertSame($valueToCheck, $retrievedValue, 'List<int> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_list_int', 'id', 'value', $intTestValues, 'list');
    }

    public function testMapRoundtrip(): void {
        // Test map<varchar, int>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_map_varchar_int (id int PRIMARY KEY, value map<varchar, int>)'
        );

        $testValues = [
            [],
            ['hello' => 1],
            ['hello' => 1, 'world' => 2],
            ['empty' => 0, 'negative' => -42, 'max' => Type\Integer::VALUE_MAX],
            ['unicode🚀' => 123, '中文' => 456, 'العربية' => 789],
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_map_varchar_int (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\CollectionMap($testValue, Type::VARCHAR, Type::INT)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_map_varchar_int WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $valueToCheck = $testValue === [] ? null : $testValue;

            if ($valueToCheck) {
                sort($valueToCheck);
            }

            if ($retrievedValue) {
                sort($retrievedValue);
            }

            $this->assertSame($valueToCheck, $retrievedValue, 'Map<varchar,int> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_map_varchar_int', 'id', 'value', $testValues, 'map');

        // Test map<int, varchar>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_map_int_varchar (id int PRIMARY KEY, value map<int, varchar>)'
        );

        $intStringTestValues = [
            [],
            [1 => 'one'],
            [1 => 'one', 2 => 'two', 3 => 'three'],
            [0 => 'zero', -1 => 'negative', 42 => 'answer'],
            [Type\Integer::VALUE_MIN => 'min', Type\Integer::VALUE_MAX => 'max'],
        ];

        foreach ($intStringTestValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_map_int_varchar (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\CollectionMap($testValue, Type::INT, Type::VARCHAR)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_map_int_varchar WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $valueToCheck = $testValue === [] ? null : $testValue;

            if ($valueToCheck) {
                sort($valueToCheck);
            }

            if ($retrievedValue) {
                sort($retrievedValue);
            }

            $this->assertSame($valueToCheck, $retrievedValue, 'Map<int,varchar> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_map_int_varchar', 'id', 'value', $intStringTestValues, 'map');
    }

    public function testSetRoundtrip(): void {
        // Test set<varchar>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_set_varchar (id int PRIMARY KEY, value set<varchar>)'
        );

        $testValues = [
            [],
            ['hello'],
            ['hello', 'world'],
            ['unique', 'values', 'only'],
            ['unicode', '🚀', '中文', 'العربية'],
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_set_varchar (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\CollectionSet($testValue, Type::VARCHAR)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_set_varchar WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $valueToCheck = $testValue === [] ? null : $testValue;

            if ($valueToCheck) {
                sort($valueToCheck);
            }

            if ($retrievedValue) {
                sort($retrievedValue);
            }

            $this->assertEquals($valueToCheck, $retrievedValue, 'Set<varchar> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_set_varchar', 'id', 'value', $testValues, 'set');

        // Test set<int>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_set_int (id int PRIMARY KEY, value set<int>)'
        );

        $intTestValues = [
            [],
            [1],
            [1, 2, 3],
            [0, -1, 42, -42],
            [Type\Integer::VALUE_MIN, Type\Integer::VALUE_MAX, 0],
        ];

        foreach ($intTestValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_set_int (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\CollectionSet($testValue, Type::INT)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_set_int WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $valueToCheck = $testValue === [] ? null : $testValue;

            if ($valueToCheck) {
                sort($valueToCheck);
            }

            if ($retrievedValue) {
                sort($retrievedValue);
            }

            $this->assertEquals($valueToCheck, $retrievedValue, 'Set<int> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_set_int', 'id', 'value', $intTestValues, 'set');
    }

    public function testSmallintRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_smallint (id int PRIMARY KEY, value smallint)'
        );

        $testValues = [
            0,
            1,
            -1,
            32767,  // max smallint
            -32768, // min smallint
            100,
            -100,
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_smallint (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Smallint($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_smallint WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Smallint value $testValue should round-trip correctly");

        }
        $this->compareWithCqlsh('test_smallint', 'id', 'value', $testValues, 'smallint');
    }

    public function testTimeRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_time (id int PRIMARY KEY, value time)'
        );

        $testValues = [
            '00:00:00' => ['php' => '00:00:00.000000000', 'cql' => '00:00:00.000000000'],
            '00:00:00.000' => ['php' => '00:00:00.000000000', 'cql' => '00:00:00.000000000'],
            '00:00:00.000000000' => ['php' => '00:00:00.000000000', 'cql' => '00:00:00.000000000'],
            '23:59:59' => ['php' => '23:59:59.000000000', 'cql' => '23:59:59.000000000'],
            '23:59:59.999999999' => ['php' => '23:59:59.999999999', 'cql' => '23:59:59.999999999'],
            '12:34:56' => ['php' => '12:34:56.000000000', 'cql' => '12:34:56.000000000'],
            '12:34:56.789012345' => ['php' => '12:34:56.789012345', 'cql' => '12:34:56.789012345'],
        ];

        // Test with integer and string values
        foreach (array_keys($testValues) as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_time (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Time($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_time WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValues[$testValue]['php'], $retrievedValue,
                "Time value $testValue should round-trip correctly");
        }

        $cqlValues = array_map(fn($value) => $value['cql'], array_values($testValues));
        $this->compareWithCqlsh('test_time', 'id', 'value', $cqlValues, 'time');

        // Test with DateTimeImmutable objects
        $dateTimeValues = [
            ['input' => new DateTimeImmutable('00:00:00'), 'output' => '00:00:00.000000000'],
            ['input' => new DateTimeImmutable('00:00:00.000'), 'output' => '00:00:00.000000000'],
            ['input' => new DateTimeImmutable('00:00:00.000000000'), 'output' => '00:00:00.000000000'],
            ['input' => new DateTimeImmutable('23:59:59'), 'output' => '23:59:59.000000000'],
            ['input' => new DateTimeImmutable('23:59:59.999999999'), 'output' => '23:59:59.999999000'],
            ['input' => new DateTimeImmutable('12:34:56'), 'output' => '12:34:56.000000000'],
            ['input' => new DateTimeImmutable('12:34:56.789012345'), 'output' => '12:34:56.789012000'],
        ];

        foreach ($dateTimeValues as $index => $config) {
            $this->connection->querySync(
                'INSERT INTO test_time (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Time($config['input'])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_time WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($config['output'], $retrievedValue,
                "Time value {$config['input']->format('Y-m-d H:i:s.vO')} should round-trip correctly");
        }

    }

    public function testTimestampRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_timestamp (id int PRIMARY KEY, value timestamp)'
        );

        $currentTime = new DateTimeImmutable('now');
        $currentTimeInMilliseconds = $currentTime->getTimestamp() * 1000 + (int) $currentTime->format('v');

        $testValues = [
            0 => ['php' => '1970-01-01 00:00:00.000+0000', 'cql' => '1970-01-01 00:00:00.000+0000'],
            1609459200000 => ['php' => '2021-01-01 00:00:00.000+0000', 'cql' => '2021-01-01 00:00:00.000+0000'],
            1234567890123 => ['php' => '2009-02-13 23:31:30.123+0000', 'cql' => '2009-02-13 23:31:30.123+0000'],
            -62135596800000 => ['php' => '0001-01-01 00:00:00.000+0000', 'cql' => '1-01-01 00:00:00.000+0000'],
            253402300799999 => ['php' => '9999-12-31 23:59:59.999+0000', 'cql' => '9999-12-31 23:59:59.998+0000'],
            $currentTimeInMilliseconds => ['php' => $currentTime->format('Y-m-d H:i:s.vO'), 'cql' => $currentTime->format('Y-m-d H:i:s.vO')],
            '1970-01-01 00:00:00.789+0000' => ['php' => '1970-01-01 00:00:00.789+0000', 'cql' => '1970-01-01 00:00:00.789+0000'],
            '2021-01-01 12:23:57' => ['php' => '2021-01-01 12:23:57.000+0000', 'cql' => '2021-01-01 12:23:57.000+0000'],
            '2009-02-13 23:31:30.123+0000' => ['php' => '2009-02-13 23:31:30.123+0000', 'cql' => '2009-02-13 23:31:30.123+0000'],
            '0001-01-01' => ['php' => '0001-01-01 00:00:00.000+0000', 'cql' => '1-01-01 00:00:00.000+0000'],
            '9999-12-31 23:59:59.999+0000' => ['php' => '9999-12-31 23:59:59.999+0000', 'cql' => '9999-12-31 23:59:59.998+0000'],
            '2021-01-01 12:23:57.123+0000' => ['php' => '2021-01-01 12:23:57.123+0000', 'cql' => '2021-01-01 12:23:57.123+0000'],
            '2021-01-01 12:23:57.123' => ['php' => '2021-01-01 12:23:57.123+0000', 'cql' => '2021-01-01 12:23:57.123+0000'],
            '2021-01-01 12:23:57' => ['php' => '2021-01-01 12:23:57.000+0000', 'cql' => '2021-01-01 12:23:57.000+0000'],
            '2021-01-01 12:23' => ['php' => '2021-01-01 12:23:00.000+0000', 'cql' => '2021-01-01 12:23:00.000+0000'],
            '2021-01-01' => ['php' => '2021-01-01 00:00:00.000+0000', 'cql' => '2021-01-01 00:00:00.000+0000'],
        ];

        // Test with integer and string values
        foreach (array_keys($testValues) as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_timestamp (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Timestamp($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_timestamp WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValues[$testValue]['php'], $retrievedValue,
                "Timestamp value $testValue should round-trip correctly");
        }

        $cqlValues = array_map(fn($value) => $value['cql'], array_values($testValues));
        $this->compareWithCqlsh('test_timestamp', 'id', 'value', $cqlValues, 'timestamp');

        // Test with DateTimeImmutable objects
        $dateTimeValues = [];
        foreach ($testValues as $input => $output) {

            if (!is_string($input)) {
                continue;
            }

            $dateTimeValues[] = [
                'input' => new DateTimeImmutable($input),
                'output' => $output['php'],
            ];
        }

        foreach ($dateTimeValues as $index => $config) {
            $this->connection->querySync(
                'INSERT INTO test_timestamp (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Timestamp($config['input'])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_timestamp WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($config['output'], $retrievedValue,
                "Timestamp value {$config['input']->format('Y-m-d H:i:s.vO')} should round-trip correctly");
        }
    }

    public function testTimeuuidRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_timeuuid (id int PRIMARY KEY, value timeuuid)'
        );

        $testValues = [
            '123e4567-e89b-12d3-a456-426614174000',
            '6ba7b810-9dad-11d1-80b4-00c04fd430c8',
            'f47ac10b-58cc-11e1-b3d4-080027620cdd',
            '1b4e28ba-2fa1-11d2-883f-0016d3cca427',
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_timeuuid (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Timeuuid($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_timeuuid WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame(strtolower($testValue), strtolower($retrievedValue),
                "Timeuuid value $testValue should round-trip correctly");
        }

        $this->compareWithCqlsh('test_timeuuid', 'id', 'value', $testValues, 'uuid');
    }

    public function testTinyintRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_tinyint (id int PRIMARY KEY, value tinyint)'
        );

        $testValues = [
            0,
            1,
            -1,
            127,  // max tinyint
            -128, // min tinyint
            50,
            -50,
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_tinyint (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Tinyint($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_tinyint WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Tinyint value $testValue should round-trip correctly");

        }
        $this->compareWithCqlsh('test_tinyint', 'id', 'value', $testValues, 'tinyint');
    }

    public function testTupleRoundtrip(): void {
        // Test tuple<varchar, int>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_tuple_varchar_int (id int PRIMARY KEY, value tuple<varchar, int>)'
        );

        $testValues = [
            ['hello', 42],
            ['', 0],
            ['world', -1],
            ['unicode🚀', Type\Integer::VALUE_MAX],
            ['test', Type\Integer::VALUE_MIN],
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_tuple_varchar_int (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Tuple($testValue, [Type::VARCHAR, Type::INT])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_tuple_varchar_int WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'Tuple<varchar,int> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_tuple_varchar_int', 'id', 'value', $testValues, 'tuple');

        // Test tuple<int, varchar, boolean>
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_tuple_int_varchar_boolean (id int PRIMARY KEY, value tuple<int, varchar, boolean>)'
        );

        $tripleTestValues = [
            [1, 'one', true],
            [0, '', false],
            [-42, 'negative', true],
            [Type\Integer::VALUE_MAX, 'max', false],
            [Type\Integer::VALUE_MIN, 'min', true],
        ];

        foreach ($tripleTestValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_tuple_int_varchar_boolean (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Tuple($testValue, [Type::INT, Type::VARCHAR, Type::BOOLEAN])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_tuple_int_varchar_boolean WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'Tuple<int,varchar,boolean> value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_tuple_int_varchar_boolean', 'id', 'value', $tripleTestValues, 'tuple');

        // Test tuple with null values
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_tuple_with_nulls (id int PRIMARY KEY, value tuple<varchar, int>)'
        );

        $nullTestValues = [
            ['hello', null],
            [null, 42],
            [null, null],
        ];

        foreach ($nullTestValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_tuple_with_nulls (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Tuple($testValue, [Type::VARCHAR, Type::INT])]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_tuple_with_nulls WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'Tuple with null values should round-trip correctly');
        }

        // cqlsh does not support tuples with null values,
        // it will throw the error "AttributeError - 'NoneType' object has no attribute 'replace'"
        // so we skip this test for now
        //$this->compareWithCqlsh('test_tuple_with_nulls', 'id', 'value', $nullTestValues, 'tuple');
    }

    public function testUdtRoundtrip(): void {
        // Create a User Defined Type in Cassandra
        $this->connection->querySync(
            'CREATE TYPE IF NOT EXISTS address (street varchar, city varchar, zip int)'
        );

        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_udt_address (id int PRIMARY KEY, value address)'
        );

        $testValues = [
            ['street' => '123 Main St', 'city' => 'New York', 'zip' => 10001],
            ['street' => '', 'city' => 'Empty Street', 'zip' => 0],
            ['street' => 'Unicode Street 🏠', 'city' => '中文市', 'zip' => 12345],
            ['street' => 'Long Street Name That Goes On And On', 'city' => 'Very Long City Name', 'zip' => Type\Integer::VALUE_MAX],
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_udt_address (id, value) VALUES (?, ?)',
                [
                    new Type\Integer($index),
                    new Type\UDT($testValue, [
                        'street' => Type::VARCHAR,
                        'city' => Type::VARCHAR,
                        'zip' => Type::INT,
                    ]),
                ]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_udt_address WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'UDT address value should round-trip correctly');
        }

        $this->compareWithCqlsh('test_udt_address', 'id', 'value', $testValues, 'udt');

        // Test UDT with null fields
        $this->connection->querySync(
            'CREATE TYPE IF NOT EXISTS person (name varchar, age int, active boolean)'
        );

        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_udt_person (id int PRIMARY KEY, value person)'
        );

        $personTestValues = [
            ['name' => 'John Doe', 'age' => 30, 'active' => true],
            ['name' => 'Jane Smith', 'age' => null, 'active' => false],
            ['name' => null, 'age' => 25, 'active' => true],
            ['name' => null, 'age' => null, 'active' => null],
        ];

        foreach ($personTestValues as $index => $testValue) {

            $allFieldsNull = true;
            foreach ($testValue as $field) {
                if ($field !== null) {
                    $allFieldsNull = false;

                    break;
                }
            }

            $this->connection->querySync(
                'INSERT INTO test_udt_person (id, value) VALUES (?, ?)',
                [
                    new Type\Integer($index),
                    new Type\UDT($testValue, [
                        'name' => Type::VARCHAR,
                        'age' => Type::INT,
                        'active' => Type::BOOLEAN,
                    ]),
                ]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_udt_person WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($allFieldsNull ? null : $testValue, $retrievedValue, 'UDT person value with nulls should round-trip correctly');
        }

        $this->compareWithCqlsh('test_udt_person', 'id', 'value', $personTestValues, 'udt');
    }

    public function testUuidRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_uuid (id int PRIMARY KEY, value uuid)'
        );

        $testValues = [
            '00000000-0000-0000-0000-000000000000', // Nil UUID
            '550e8400-e29b-41d4-a716-446655440000', // Example UUID
            'f47ac10b-58cc-4372-a567-0e02b2c3d479', // Random UUID
            '123e4567-e89b-12d3-a456-426614174000', // Version 1 style
            strtoupper('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'), // Uppercase
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_uuid (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Uuid($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_uuid WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            // UUID should be normalized to lowercase
            $this->assertSame(strtolower($testValue), strtolower($retrievedValue),
                "UUID value $testValue should round-trip correctly");

        }
        $this->compareWithCqlsh('test_uuid', 'id', 'value', $testValues, 'uuid');
    }

    public function testVarcharRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_varchar (id int PRIMARY KEY, value varchar)'
        );

        $testValues = [
            '',
            'a',
            'Hello, World!',
            '12345',
            'UPPERCASE',
            'lowercase',
            'MiXeD cAsE',
            'Unicode: 🚀 中文 العربية русский',
            'Special chars: !@#$%^&*()_+-=[]{}|;:\'",.<>?/~`',
            str_repeat('x', 1000), // Long string
            '1Newlines\nand\ttabs',
            "2Newlines\nand\ttabs",
            'NULL and null and None',
            json_encode(['key' => 'value', 'number' => 42]),
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_varchar (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Varchar($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_varchar WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue, 'Varchar value should round-trip correctly');

        }
        $this->compareWithCqlsh('test_varchar', 'id', 'value', $testValues, 'varchar');
    }

    public function testVarintRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_varint (id int PRIMARY KEY, value varint)'
        );

        $testValues = [
            0,
            1,
            10,
            -1,
            -10,
            42,
            -42,
            PHP_INT_MAX,
            PHP_INT_MIN,
            '0',
            '1',
            '10',
            '-1',
            '-10',
            '123456789012345678901234567890',
            '-123456789012345678901234567890',
            '999999999999999999999999999999999999999999999999999999999999999999',
            '-999999999999999999999999999999999999999999999999999999999999999999',
            '170141183460469231731687303715884105727', // 2^127 - 1
            '-170141183460469231731687303715884105728', // -2^127
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_varint (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Varint($testValue)]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_varint WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame((string) $testValue, $retrievedValue, "Varint value $testValue should round-trip correctly");
        }

        $stringTestValues = array_map('strval', $testValues);
        $this->compareWithCqlsh('test_varint', 'id', 'value', $stringTestValues, 'varint');
    }

    public function testVectorRoundtrip(): void {

        $cassandraVersion = getenv('APP_CASSANDRA_VERSION');
        if ($cassandraVersion !== null && version_compare($cassandraVersion, '5.0', '<')) {
            $this->markTestSkipped('Vectors are not supported in Cassandra versions before 5.0');

            return;
        }

        // test vector of 3 float values (data type with fixed size)
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_vector_float3 (id int PRIMARY KEY, value vector<float, 3>)'
        );

        $testValues = [
            [0, 0, 0],
            [1.1, 2.2, 3.3],
            [-1.1, -2.2, -3.3],
            [3.4028E38, 1.1754E-38, -3.4028E38],
            [42.5, -42.5, 0.125],
        ];

        foreach ($testValues as $index => $testValue) {
            $this->connection->querySync(
                'INSERT INTO test_vector_float3 (id, value) VALUES (?, ?)',
                [
                    new Type\Integer($index),
                    new Type\Vector($testValue, Type::FLOAT, 3),
                ]
            );

            $result = $this->connection->querySync(
                'SELECT value FROM test_vector_float3 WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertIsArray($retrievedValue, 'Vector should be returned as an array');
            $this->assertCount(3, $retrievedValue, 'Vector length should be 3');

            // Compare element-wise with delta suitable for float32
            foreach ([0, 1, 2] as $i) {
                $this->assertEqualsWithDelta(
                    (float) $testValue[$i],
                    (float) $retrievedValue[$i],
                    max(abs($testValue[$i]) * 0.01, 0.0001),
                    "Vector element {$i} should round-trip correctly"
                );
            }
        }

        $this->compareWithCqlsh('test_vector_float3', 'id', 'value', $testValues, 'vector');

        // todo: test vector of 4 varint values (data type with variable size)
    }

    private function compareWithCqlsh(string $tableName, string $idColumn, string $valueColumn, array $testValues, string $dataType): void {

        $cqlshResults = $this->dumpTableWithCqlsh($this->testKeyspace, $tableName, $idColumn, $valueColumn);

        foreach ($testValues as $idValue => $phpValue) {
            if (!isset($cqlshResults[$idValue])) {
                $this->fail("No cqlsh result found for ID {$idValue} in table {$tableName}");
            }

            if (!isset($cqlshResults[$idValue][$valueColumn])) {
                $this->fail("Column '{$valueColumn}' not found in cqlsh result for ID {$idValue} in table {$tableName}");
            }

            $cqlshValue = $cqlshResults[$idValue][$valueColumn];

            match ($dataType) {
                'ascii', 'varchar' => $this->assertSame($phpValue, $cqlshValue, 'PHP string value should match cqlsh output'),
                'bigint', 'integer', 'smallint', 'tinyint' => $this->assertSame((string) $phpValue, (string) $cqlshValue, 'PHP integer value should match cqlsh output'),
                'blob' => $this->assertSame('0x' . bin2hex($phpValue), $cqlshValue, 'PHP blob value should match cqlsh hex output'),
                'boolean' => $this->assertSame($phpValue ? 'True' : 'False', $cqlshValue, 'PHP boolean value should match cqlsh output'),
                'uuid' => $this->assertSame(strtolower($phpValue), strtolower($cqlshValue), 'PHP UUID value should match cqlsh output'),
                'float' => $this->assertEqualsWithDelta((float) $phpValue, (float) $cqlshValue, max($phpValue * 0.01, 0.0001), 'PHP float value should match cqlsh output'),
                'double' => $this->assertEqualsWithDelta((float) $phpValue, (float) $cqlshValue, max($phpValue * 0.01, 0.000001), 'PHP float value should match cqlsh output'),
                'list', 'vector' => $this->verifyCqlListMatchesPhpList($phpValue, $cqlshValue),
                'set' => $this->verifyCqlSetMatchesPhpSet($phpValue, $cqlshValue),
                'map' => $this->verifyCqlMapMatchesPhpMap($phpValue, $cqlshValue),
                'udt' => $this->verifyCqlUdtMatchesPhpUdt($phpValue, $cqlshValue),
                'tuple' => $this->verifyCqlTupleMatchesPhpTuple($phpValue, $cqlshValue),
                default => $this->assertSame((string) $phpValue, (string) $cqlshValue, 'PHP value should match cqlsh output'),
            };
        }
    }

    /**
     * @param string[] $columnList
     */
    private function dumpTableWithCqlsh(string $keyspace, string $tableName, string $idColumn, string $valueColumn): array {

        $containerName = 'php-cassandra-test-db';

        $containerCheck = shell_exec("docker ps --filter name={$containerName} --format '{{.Names}}' 2>/dev/null");
        if (trim($containerCheck) !== $containerName) {
            $this->markTestSkipped("Cassandra container '{$containerName}' is not running. Please start with 'docker-compose up -d'");
        }

        $options = [
            'HEADER' => 'TRUE',
            'DELIMITER' => '|',
            'QUOTE' => '"',
            'ESCAPE' => '?',
            'NULL' => '__NULL__',
            'DATETIMEFORMAT' => '%Y-%m-%d %H:%M:%S.%f%z',
            'DECIMALSEP' => '.',
            'PAGESIZE' => '100',
            'ENCODING' => 'UTF8',
            'NUMPROCESSES' => '1',
        ];

        $optionsString = implode(' AND ', array_map(fn($k, $v) => "{$k} = '{$v}'", array_keys($options), $options));

        $columnList = [$idColumn, $valueColumn];
        $query = "COPY {$keyspace}.\"{$tableName}\" (" . implode(',', $columnList) . ") TO '/tmp/table_dump.csv' WITH " . $optionsString . ' ;';
        $escapedQuery = escapeshellarg($query);
        $command = "docker exec {$containerName} cqlsh -k {$this->testKeyspace} -e {$escapedQuery} 2>&1";

        $output = shell_exec($command);
        if ($output === null) {
            $this->fail("Failed to execute cqlsh command in container: {$command}");
        }

        if (
            str_contains($output, 'Connection error')
            || str_contains($output, 'SyntaxException')
            || str_contains($output, 'InvalidRequest')
            || !str_contains($output, 'rows exported to 1 files')
        ) {
            $this->fail("cqlsh command failed: {$command}\nOutput: {$output}");
        }

        shell_exec("docker cp {$containerName}:/tmp/table_dump.csv {$this->dumpFile}");

        $rowData = [];
        $handle = fopen($this->dumpFile, 'r');
        if ($handle === false) {
            $this->fail('Failed to open CSV file for reading: ' . $this->dumpFile);
        }

        $header = fgetcsv(
            stream: $handle,
            length: 0,
            separator: $options['DELIMITER'],
            enclosure: $options['QUOTE'],
            escape: $options['ESCAPE']
        );

        while (($row = fgetcsv(
            stream: $handle,
            length: 0,
            separator: $options['DELIMITER'],
            enclosure: $options['QUOTE'],
            escape: $options['ESCAPE']
        )) !== false) {

            $row = array_map(fn($value) => str_replace([
                $options['NULL'],
                $options['ESCAPE'] . $options['QUOTE'],
                $options['ESCAPE'] . $options['ESCAPE'],
                '\\\\',
            ], [
                null, // Replace NULL placeholder with PHP null
                $options['QUOTE'],
                $options['ESCAPE'],
                '______ESCAPE______',
            ], $value), $row);

            $row = array_map(fn($value) => str_replace([
                '\\t',
                '\\n',
            ], [
                "\t",
                "\n",
            ], $value), $row);

            $row = array_map(fn($value) => str_replace([
                '______ESCAPE______',
            ], [
                '\\',
            ], $value), $row);

            $rowData[] = array_combine($header, $row);
        }

        fclose($handle);

        $rows = [];
        foreach ($rowData as $row) {
            $rows[$row[$idColumn]] = $row;
        }

        return $rows;
    }

    private static function getHost(): string {
        return getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
    }

    private static function getPort(): int {
        $port = getenv('APP_CASSANDRA_PORT') ?: '9042';

        return (int) $port;
    }

    private function newConnection(string $keyspace = 'app'): Connection {
        $nodes = [
            new SocketNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: '',
                password: ''
            ),
        ];

        $conn = new Connection($nodes, $keyspace);
        $conn->setConsistency(Consistency::ONE);
        $this->assertTrue($conn->connect());
        $this->assertTrue($conn->isConnected());

        return $conn;
    }

    private function verifyCqlListMatchesPhpList(array $phpValue, string $cqlshValue): void {

        if ($phpValue === []) {
            $this->assertSame('', $cqlshValue, 'PHP list value should match cqlsh output');

            return;
        }

        // convert to json
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        if ($phpValue) {
            sort($phpValue);
        }

        if ($cqlshArray) {
            sort($cqlshArray);
        }

        $this->assertSame($phpValue, $cqlshArray, 'PHP list value should match cqlsh output');
    }

    private function verifyCqlMapMatchesPhpMap(array $phpValue, string $cqlshValue): void {

        if ($phpValue === []) {
            $this->assertSame('', $cqlshValue, 'PHP map value should match cqlsh output');

            return;
        }

        // fix keys
        $cqlshValue = preg_replace('/([a-zA-Z0-9-_]+):/', '"$1":', $cqlshValue);
        $cqlshValue = str_replace(': ,', ': null,', $cqlshValue);

        // convert to json
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        if ($phpValue) {
            sort($phpValue);
        }

        if ($cqlshArray) {
            sort($cqlshArray);
        }

        $this->assertSame($phpValue, $cqlshArray, 'PHP map value should match cqlsh output');
    }

    private function verifyCqlSetMatchesPhpSet(array $phpValue, string $cqlshValue): void {

        if ($phpValue === []) {
            $this->assertSame('', $cqlshValue, 'PHP set value should match cqlsh output');

            return;
        }

        // convert to json
        $cqlshValue = str_replace(['{', '}'], ['[', ']'], $cqlshValue);
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        if ($phpValue) {
            sort($phpValue);
        }

        if ($cqlshArray) {
            sort($cqlshArray);
        }

        $this->assertSame($phpValue, $cqlshArray, 'PHP set value should match cqlsh output');
    }

    private function verifyCqlTupleMatchesPhpTuple(array $phpValue, string $cqlshValue): void {

        if ($phpValue === []) {
            $this->assertSame('', $cqlshValue, 'PHP tuple value should match cqlsh output');

            return;
        }

        // convert to json
        $cqlshValue = str_replace(['(', ')'], ['[', ']'], $cqlshValue);
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        if ($phpValue) {
            sort($phpValue);
        }

        if ($cqlshArray) {
            sort($cqlshArray);
        }

        $this->assertSame($phpValue, $cqlshArray, 'PHP tuple value should match cqlsh output');
    }

    private function verifyCqlUdtMatchesPhpUdt(array $phpValue, string $cqlshValue): void {

        $allFieldsNull = true;
        foreach ($phpValue as $field) {
            if ($field !== null) {
                $allFieldsNull = false;

                break;
            }
        }

        if ($allFieldsNull) {
            $this->assertSame('', $cqlshValue, 'PHP UDT value should match cqlsh output');

            return;
        }

        // fix keys
        $cqlshValue = preg_replace('/([a-zA-Z0-9-_]+):/', '"$1":', $cqlshValue);
        $cqlshValue = str_replace(': ,', ': null,', $cqlshValue);

        // convert to json
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        if ($phpValue) {
            sort($phpValue);
        }

        if ($cqlshArray) {
            sort($cqlshArray);
        }

        $this->assertSame($phpValue, $cqlshArray, 'PHP UDT value should match cqlsh output');
    }
}
