<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Type;
use PHPUnit\Framework\TestCase;

/**
 * Integration test to verify that data types round-trip correctly through Cassandra.
 * Tests that values inserted via PHP match when selected back, ensuring compatibility
 * with cqlsh and proper serialization/deserialization.
 */
/**
 * Types to test (Cassandra v5):
 * - ascii                  *Implemented    
 * - bigint (counter)       *Implemented
 * - blob                   *Implemented
 * - boolean                *Implemented
 * - list
 * - map
 * - set
 * - custom
 * - date                   *Implemented
 * - decimal
 * - double                 *Implemented
 * - duration
 * - float                  *Implemented
 * - inet                   *Implemented
 * - integer                *Implemented
 * - smallint               *Implemented   
 * - time
 * - timestamp              *Implemented 
 * - tinyint                *Implemented
 * - tuple
 * - udt
 * - uuid (timeuuid)        *Implemented
 * - varchar                *Implemented
 * - varint
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
            9223372036854775807,  // max bigint
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

    public function testDateRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_date (id int PRIMARY KEY, value date)'
        );

        $testValues = [
            '1970-01-01' => '1970-01-01',
            '1970-01-02' => '1970-01-02',
            '1969-12-31' => '1969-12-31',
            '1971-01-01' => '1971-01-01',
            '1969-01-01' => '1969-01-01',
            '2020-01-01' => '2020-01-01',
            '-2020-01-02' => '-1457317',
            '-5877641-06-24' => '-2147483647',
            '-5877641-06-23' => '-2147483648', // min date
            '0001-01-01' => '0001-01-01',
            '9999-07-10' => '9999-07-10',
            '10000-07-10' => '2933088',
            '5181580-07-10' => '1891813896',
            '5881580-07-11' => '2147483647', // max date
        ];

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

            $this->assertSame($testValue, $retrievedValue, "Date value $testValue should round-trip correctly");
        }

        $this->compareWithCqlsh('test_date', 'id', 'value', array_values($testValues), 'date');
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

            $this->assertEqualsWithDelta($testValue, $retrievedValue, 0.000000001, "Double value $testValue should round-trip correctly");

        }

        $this->compareWithCqlsh('test_double', 'id', 'value', $testValues, 'double');
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
            $this->assertEqualsWithDelta($testValue, $retrievedValue, 0.0001, "Float32 value $testValue should round-trip correctly");

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

    public function testTimestampRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_timestamp (id int PRIMARY KEY, value timestamp)'
        );

        $testValues = [
            0, // Unix epoch
            1609459200000, // 2021-01-01 00:00:00 UTC (milliseconds)
            1234567890123, // Random timestamp
            -62135596800000, // Year 1 AD
            253402300799999, // Year 9999
            time() * 1000, // Current time in milliseconds
        ];

        foreach ($testValues as $index => $testValue) {
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

            $this->assertSame($testValue, $retrievedValue,
                "Timestamp value $testValue should round-trip correctly");

        }
        $this->compareWithCqlsh('test_timestamp', 'id', 'value', $testValues, 'timestamp');
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
                'float' => $this->assertEqualsWithDelta($phpValue, (float) $cqlshValue, 0.0001, 'PHP float value should match cqlsh output'),
                'double' => $this->assertEqualsWithDelta($phpValue, (float) $cqlshValue, 0.0000001, 'PHP float value should match cqlsh output'),
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
            'ESCAPE' => '\\',
            'NULL' => '__NULL__',
            'DATETIMEFORMAT' => '%Y-%m-%d %H:%M:%S%z',
            'DECIMALSEP' => '.',
            'PAGESIZE' => '100',
            'ENCODING' => 'UTF8',
            'NUMPROCESSES' => '1',
        ];

        $optionsString = implode(' AND ', array_map(fn($k, $v) => "{$k} = '{$v}'", array_keys($options), $options));

        $columnList = [$idColumn, $valueColumn];
        $query = "COPY {$keyspace}.{$tableName} (" . implode(',', $columnList) . ") TO '/tmp/table_dump.csv' WITH " . $optionsString . ' ;';
        $escapedQuery = escapeshellarg($query);
        $command = "docker exec {$containerName} cqlsh -k {$this->testKeyspace} -e {$escapedQuery} 2>&1";

        $output = shell_exec($command);
        if ($output === null) {
            $this->fail("Failed to execute cqlsh command in container: {$command}");
        }

        if (str_contains($output, 'Connection error') || str_contains($output, 'SyntaxException') || str_contains($output, 'InvalidRequest')) {
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
            ], [
                null, // Replace NULL placeholder with PHP null
                $options['QUOTE'],
                $options['ESCAPE'],
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
}
