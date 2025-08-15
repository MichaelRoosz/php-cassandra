<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Type;
use PHPUnit\Framework\TestCase;
use Throwable;

/**
 * Integration test to verify that data types round-trip correctly through Cassandra
 * Tests that values inserted via PHP match when selected back, ensuring compatibility
 * with cqlsh and proper serialization/deserialization
 */
final class DataTypeRoundtripTest extends TestCase {
    private Connection $connection;
    private string $testKeyspace = 'datatype_test';

    protected function setUp(): void {
        parent::setUp();
        $this->connection = $this->newConnection('system');

        // Create test keyspace if it doesn't exist
        $this->connection->querySync(
            "CREATE KEYSPACE IF NOT EXISTS {$this->testKeyspace} WITH REPLICATION = " .
            "{'class': 'SimpleStrategy', 'replication_factor': 1}"
        );

        // Switch to test keyspace
        $this->connection = $this->newConnection($this->testKeyspace);
    }

    protected function tearDown(): void {
        try {
            // Clean up test keyspace
            $this->connection = $this->newConnection('system');
            $this->connection->querySync("DROP KEYSPACE IF EXISTS {$this->testKeyspace}");
        } catch (Throwable $e) {
            // Ignore cleanup errors
        }
        parent::tearDown();
    }

    public function testAsciiRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_ascii (id int PRIMARY KEY, value ascii)'
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
        ];

        foreach ($testValues as $index => $testValue) {
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_ascii (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Ascii($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_ascii WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                'ASCII value should round-trip correctly');

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_ascii', $index, $testValue);
        }
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
            -9223372036854775808, // min bigint
            1000000000000000,
            -1000000000000000,
        ];

        foreach ($testValues as $index => $testValue) {
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_bigint (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Bigint($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_bigint WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Bigint value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_bigint', $index, $testValue);
        }
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_blob (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Blob($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_blob WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                'Blob value should round-trip correctly');

            // Also compare with cqlsh output (blob values are hex-encoded in cqlsh)
            $this->compareWithCqlsh('test_blob', $index, $testValue, 'blob');
        }
    }

    public function testBooleanRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_boolean (id int PRIMARY KEY, value boolean)'
        );

        $testValues = [true, false];

        foreach ($testValues as $index => $testValue) {
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_boolean (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Boolean($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_boolean WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                'Boolean value ' . ($testValue ? 'true' : 'false') . ' should round-trip correctly');

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_boolean', $index, $testValue);
        }
    }

    /**
     * Demonstration test showing cqlsh comparison functionality
     */
    public function testCqlshComparisonDemo(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_demo (id int PRIMARY KEY, name varchar, active boolean, score double)'
        );

        // Insert test data
        $testData = [
            ['id' => 1, 'name' => 'Alice', 'active' => true, 'score' => 95.5],
            ['id' => 2, 'name' => 'Bob', 'active' => false, 'score' => 87.2],
            ['id' => 3, 'name' => 'Charlie', 'active' => true, 'score' => 92.8],
        ];

        foreach ($testData as $row) {
            $this->connection->querySync(
                'INSERT INTO test_demo (id, name, active, score) VALUES (?, ?, ?, ?)',
                [
                    new Type\Integer($row['id']),
                    new Type\Varchar($row['name']),
                    new Type\Boolean($row['active']),
                    new Type\Double($row['score']),
                ]
            );
        }

        // Test that PHP and cqlsh return the same results
        foreach ($testData as $row) {
            // Test varchar field
            $phpResult = $this->connection->querySync(
                'SELECT name FROM test_demo WHERE id = ?',
                [new Type\Integer($row['id'])]
            )->asRowsResult();

            $phpValue = $phpResult->fetch()['name'];
            $this->assertSame($row['name'], $phpValue);

            // Compare with cqlsh
            $this->compareWithCqlsh('test_demo', $row['id'], $row['name'], 'name');

            // Test boolean field
            $phpResult = $this->connection->querySync(
                'SELECT active FROM test_demo WHERE id = ?',
                [new Type\Integer($row['id'])]
            )->asRowsResult();

            $phpValue = $phpResult->fetch()['active'];
            $this->assertSame($row['active'], $phpValue);

            // Compare with cqlsh
            $this->compareWithCqlsh('test_demo', $row['id'], $row['active'], 'active');
        }
    }

    public function testDateRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_date (id int PRIMARY KEY, value date)'
        );

        $baseDate = Type\Date::VALUE_2_31; // 1970-01-01

        $testValues = [
            $baseDate, // 1970-01-01 (epoch)
            $baseDate + 1, // 1970-01-02
            $baseDate - 1, // 1969-12-31
            $baseDate + 365, // 1971-01-01
            $baseDate - 365, // 1969-01-01
            Type\Date::VALUE_MIN, // Minimum date
            Type\Date::VALUE_MAX, // Maximum date
            $baseDate + 18262, // 2020-01-01 (approximately)
        ];

        foreach ($testValues as $index => $testValue) {
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_date (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Date($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_date WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Date value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_date', $index, $testValue);
        }
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_double (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Double($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_double WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            // Use delta comparison for floats
            $this->assertEqualsWithDelta($testValue, $retrievedValue, 0.000000001,
                "Double value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_double', $index, $testValue);
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_float32 (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Float32($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_float32 WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            // Use delta comparison for floats (float32 has less precision)
            $this->assertEqualsWithDelta($testValue, $retrievedValue, 0.0001,
                "Float32 value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_float32', $index, $testValue);
        }
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
            'fe80::1%lo0',
        ];

        foreach ($testValues as $index => $testValue) {
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_inet (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Inet($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_inet WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Inet value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_inet', $index, $testValue);
        }
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_integer (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Integer($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_integer WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Integer value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_integer', $index, $testValue);
        }
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_smallint (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Smallint($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_smallint WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Smallint value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_smallint', $index, $testValue);
        }
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_timestamp (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Timestamp($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_timestamp WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Timestamp value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_timestamp', $index, $testValue);
        }
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_tinyint (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Tinyint($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_tinyint WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                "Tinyint value $testValue should round-trip correctly");

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_tinyint', $index, $testValue);
        }
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
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_uuid (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Uuid($testValue)]
            );

            // Select value back
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

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_uuid', $index, $testValue, 'uuid');
        }
    }

    public function testVarcharRoundtrip(): void {
        $this->connection->querySync(
            'CREATE TABLE IF NOT EXISTS test_varchar (id int PRIMARY KEY, value varchar)'
        );

        $testValues = [
            '',
            'a',
            'Hello, World!',
            'Unicode: 🚀 中文 العربية русский',
            'Special chars: !@#$%^&*()_+-=[]{}|;:\'",.<>?/~`',
            str_repeat('x', 1000), // Long string
            'Newlines\nand\ttabs',
            'NULL and null and None',
            json_encode(['key' => 'value', 'number' => 42]),
        ];

        foreach ($testValues as $index => $testValue) {
            // Insert value
            $this->connection->querySync(
                'INSERT INTO test_varchar (id, value) VALUES (?, ?)',
                [new Type\Integer($index), new Type\Varchar($testValue)]
            );

            // Select value back
            $result = $this->connection->querySync(
                'SELECT value FROM test_varchar WHERE id = ?',
                [new Type\Integer($index)]
            )->asRowsResult();

            $row = $result->fetch();
            $this->assertNotNull($row, "Row should exist for index $index");
            $retrievedValue = $row['value'];

            $this->assertSame($testValue, $retrievedValue,
                'Varchar value should round-trip correctly');

            // Also compare with cqlsh output
            $this->compareWithCqlsh('test_varchar', $index, $testValue);
        }
    }

    private function compareWithCqlsh(string $tableName, array $columnList, string $idColumn, mixed $phpValue): void {

        $cqlshResults = $this->dumpTableWithCqlsh($this->testKeyspace, $tableName, $columnList);

        $this->assertCount(1, $cqlshResults, 'Expected exactly one result from cqlsh');
        $cqlshValue = $cqlshResults[0][$idColumn];

        if (is_float($phpValue)) {
            $this->assertEqualsWithDelta($phpValue, (float) $cqlshValue, 0.0001,
                'PHP float value should match cqlsh output');
        } elseif (is_bool($phpValue)) {
            $expectedCqlsh = $phpValue ? 'True' : 'False';
            $this->assertSame($expectedCqlsh, $cqlshValue,
                'PHP boolean value should match cqlsh output');
        } elseif (is_string($phpValue) && str_contains($valueType, 'uuid')) {
            $this->assertSame(strtolower($phpValue), strtolower($cqlshValue),
                'PHP UUID value should match cqlsh output');
        } elseif (is_string($phpValue) && str_contains($valueType, 'blob')) {
            $expectedHex = '0x' . bin2hex($phpValue);
            $this->assertSame($expectedHex, $cqlshValue,
                'PHP blob value should match cqlsh hex output');
        } else {
            $this->assertSame((string) $phpValue, $cqlshValue,
                'PHP value should match cqlsh output');
        }
    }

    /**
     * @param string[] $columnList
     */
    private function dumpTableWithCqlsh(string $keyspace, string $tableName, array $columnList): array {

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
            'NULL' => '',
            'DATETIMEFORMAT' => '%Y-%m-%d %H:%M:%S%z',
            'DECIMALSEP' => '.',
            'PAGESIZE' => '1000',
            'ENCODING' => 'UTF8',
        ];
        $optionsString = implode(' AND ', array_map(fn($k, $v) => "{$k} = {$v}", array_keys($options), $options));

        $query = "COPY {$keyspace}.{$tableName} (" . implode(',', $columnList) . ") TO 'table_dump.csv' WITH " . $optionsString . ' ;';
        $escapedQuery = escapeshellarg($query);
        $command = "docker exec {$containerName} cqlsh -k {$this->testKeyspace} -e {$escapedQuery} 2>&1";

        $output = shell_exec($command);
        if ($output === null) {
            $this->fail("Failed to execute cqlsh command in container: {$command}");
        }

        if (str_contains($output, 'Connection error') || str_contains($output, 'SyntaxException') || str_contains($output, 'InvalidRequest')) {
            $this->fail("cqlsh command failed: {$command}\nOutput: {$output}");
        }

        // read the csv file with csv fgetcsv

        $rows = [];
        $handle = fopen('table_dump.csv', 'r');
        $header = fgetcsv($handle, 0, '|', '"', '\\');
        while (($row = fgetcsv($handle, 0, '|', '"', '\\')) !== false) {
            $rows[] = array_combine($header, $row);
        }
        fclose($handle);

        var_dump($rows);

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

    /**
     * Parse cqlsh output to extract row values
     */
    private function parseCqlshOutput(string $output): array {

        var_dump($output);
        $lines = explode("\n", trim($output));
        $rows = [];
        $foundHeader = false;
        $foundSeparator = false;

        foreach ($lines as $line) {
            $line = trim($line);

            // Skip empty lines
            if (empty($line)) {
                continue;
            }

            // Look for column header (contains column name like 'value')
            if (!$foundHeader && (str_contains($line, 'value') || str_contains($line, '|'))) {
                $foundHeader = true;

                continue;
            }

            // Look for separator line (dashes)
            if ($foundHeader && !$foundSeparator && (str_contains($line, '-') || str_starts_with($line, '+'))) {
                $foundSeparator = true;

                continue;
            }

            // Skip summary lines and warnings
            if (str_contains($line, 'rows)') || str_contains($line, 'Warnings:')
                || (str_contains($line, '(') && str_contains($line, 'rows'))) {
                break;
            }

            // This should be a data row if we've found header and separator
            if ($foundHeader && $foundSeparator && !empty($line)) {
                // Handle different output formats
                if (str_contains($line, '|')) {
                    // Pipe-separated format: | value |
                    $parts = array_map('trim', explode('|', $line));
                    // Find the non-empty part (the actual value)
                    foreach ($parts as $part) {
                        if (!empty($part)) {
                            $value = $part;

                            break;
                        }
                    }
                } else {
                    // Plain format: just the value
                    $value = $line;
                }

                // Handle special cqlsh representations
                if (isset($value)) {
                    if ($value === 'null') {
                        $value = null;
                    } elseif ($value === 'True') {
                        $value = true;
                    } elseif ($value === 'False') {
                        $value = false;
                    }
                    // Keep other values as strings for proper comparison

                    $rows[] = $value;
                }
            }
        }

        return $rows;
    }
}
