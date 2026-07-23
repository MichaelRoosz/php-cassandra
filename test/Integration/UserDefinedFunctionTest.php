<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Response\Result\SchemaChangeResult;

/**
 * Integration tests for user-defined functions (UDF) and user-defined
 * aggregates (UDA).
 *
 * See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useCreateFunction.html
 *
 * Both servers must be started with UDFs enabled (see docker-compose.yml and
 * docker-compose.scylladb.yml). Cassandra executes Java bodies, ScyllaDB Lua
 * ones, so only the function body differs between backends - everything the
 * driver does is identical.
 */
final class UserDefinedFunctionTest extends AbstractIntegrationTestCase {
    public function testAggregate(): void {
        $row = $this->connection->query(
            "SELECT udf_sum(n) AS total FROM {$this->keyspace}.udf_numbers WHERE pk = 1"
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(60, $row['total'], 'The aggregate should fold every row of the partition');
    }

    public function testAggregateWithFinalFunction(): void {
        $row = $this->connection->query(
            "SELECT udf_avg(n) AS mean FROM {$this->keyspace}.udf_numbers WHERE pk = 1"
        )->asRowsResult()->fetch();

        // Four rows in the partition: 10 + 20 + 30 + 0 = 60, so the mean is 15.
        $this->assertIsArray($row);
        $this->assertSame(15.0, $row['mean'], 'FINALFUNC should convert the tuple state into a double');
    }

    public function testDropFunctionAndAggregate(): void {
        $language = self::udfLanguage();

        $this->connection->query(
            "CREATE OR REPLACE FUNCTION {$this->keyspace}.udf_temp(a int) "
            . 'CALLED ON NULL INPUT RETURNS int '
            . "LANGUAGE {$language} AS '" . self::body('doubleValue') . "'"
        );

        $row = $this->connection->query(
            "SELECT udf_temp(n) AS d FROM {$this->keyspace}.udf_numbers WHERE pk = 1 AND ck = 1"
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(20, $row['d']);

        $dropped = $this->connection->query("DROP FUNCTION IF EXISTS {$this->keyspace}.udf_temp");
        $this->assertInstanceOf(SchemaChangeResult::class, $dropped);

        // Dropping a function that no longer exists is a no-op with IF EXISTS.
        $this->connection->query("DROP FUNCTION IF EXISTS {$this->keyspace}.udf_temp");
    }

    public function testFunctionReturningCollection(): void {
        $row = $this->connection->query(
            "SELECT udf_pair(n, n) AS pair FROM {$this->keyspace}.udf_numbers WHERE pk = 1 AND ck = 1"
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame([10, 10], $row['pair'], 'A UDF may return a collection type');
    }

    public function testFunctionWithBoundArgument(): void {
        if (!self::isBindMarkerInFunctionArgumentSupported()) {
            // ScyllaDB rejects a bind marker in a function call argument with
            // "Bind variables cannot be used for keyspace names", with or
            // without a cast or a keyspace-qualified function name. Cassandra
            // only added support in 3.6 (CASSANDRA-10783).
            $this->markTestSkipped('Bind markers as function arguments require Cassandra 3.6+ (unsupported by ScyllaDB)');
        }

        $row = $this->connection->query(
            "SELECT udf_len(?) AS len FROM {$this->keyspace}.udf_numbers WHERE pk = 1 AND ck = 1",
            ['abcd']
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(4, $row['len'], 'Function arguments are ordinary bind values');
    }

    public function testFunctionWithCalledOnNullInput(): void {
        $row = $this->connection->query(
            "SELECT udf_len(v) AS len FROM {$this->keyspace}.udf_numbers WHERE pk = 1 AND ck = 4"
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(0, $row['len'], 'CALLED ON NULL INPUT invokes the body with a null argument');
    }

    public function testFunctionWithReturnsNullOnNullInput(): void {
        $row = $this->connection->query(
            "SELECT udf_strict_len(v) AS len FROM {$this->keyspace}.udf_numbers WHERE pk = 1 AND ck = 4"
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertNull($row['len'], 'RETURNS NULL ON NULL INPUT short-circuits to null');
    }

    public function testScalarFunction(): void {
        $row = $this->connection->query(
            "SELECT udf_len(v) AS len FROM {$this->keyspace}.udf_numbers WHERE pk = 1 AND ck = 1"
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(5, $row['len'], 'The scalar UDF should return the length of the column value');
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS udf_numbers (pk int, ck int, v varchar, n int, PRIMARY KEY (pk, ck))'
        );

        foreach ([[1, 'alpha', 10], [2, 'beta', 20], [3, 'gamma', 30]] as [$ck, $v, $n]) {
            $conn->query('INSERT INTO udf_numbers (pk, ck, v, n) VALUES (1, ?, ?, ?)', [$ck, $v, $n]);
        }

        // A row whose text column is null, to exercise the null-input variants.
        $conn->query('INSERT INTO udf_numbers (pk, ck, n) VALUES (1, 4, 0)');

        $language = self::udfLanguage();

        $conn->query(
            'CREATE OR REPLACE FUNCTION udf_len(input text) CALLED ON NULL INPUT RETURNS int '
            . "LANGUAGE {$language} AS '" . self::body('length') . "'"
        );
        $conn->query(
            'CREATE OR REPLACE FUNCTION udf_strict_len(input text) RETURNS NULL ON NULL INPUT RETURNS int '
            . "LANGUAGE {$language} AS '" . self::body('strictLength') . "'"
        );
        $conn->query(
            'CREATE OR REPLACE FUNCTION udf_pair(a int, b int) CALLED ON NULL INPUT RETURNS list<int> '
            . "LANGUAGE {$language} AS '" . self::body('pair') . "'"
        );
        $conn->query(
            'CREATE OR REPLACE FUNCTION udf_accumulate(state int, val int) CALLED ON NULL INPUT RETURNS int '
            . "LANGUAGE {$language} AS '" . self::body('accumulate') . "'"
        );
        $conn->query(
            'CREATE OR REPLACE AGGREGATE udf_sum(int) SFUNC udf_accumulate STYPE int INITCOND 0'
        );
        $conn->query(
            'CREATE OR REPLACE FUNCTION udf_avg_state(state tuple<int, bigint>, val int) '
            . 'CALLED ON NULL INPUT RETURNS tuple<int, bigint> '
            . "LANGUAGE {$language} AS '" . self::body('avgState') . "'"
        );
        $conn->query(
            'CREATE OR REPLACE FUNCTION udf_avg_final(state tuple<int, bigint>) '
            . 'CALLED ON NULL INPUT RETURNS double '
            . "LANGUAGE {$language} AS '" . self::body('avgFinal') . "'"
        );
        $conn->query(
            'CREATE OR REPLACE AGGREGATE udf_avg(int) SFUNC udf_avg_state STYPE tuple<int, bigint> '
            . 'FINALFUNC udf_avg_final INITCOND (0, 0)'
        );

        $conn->disconnect();
    }

    /**
     * Function bodies, expressed once per supported UDF language.
     */
    private static function body(string $name): string {
        $bodies = [
            'length' => [
                'java' => 'return input == null ? 0 : input.length();',
                'lua' => 'if input == nil then return 0 end return #input;',
            ],
            'strictLength' => [
                'java' => 'return input.length();',
                'lua' => 'return #input;',
            ],
            'pair' => [
                'java' => 'return java.util.Arrays.asList(a, b);',
                'lua' => 'return {a, b};',
            ],
            'doubleValue' => [
                'java' => 'return a == null ? 0 : a * 2;',
                'lua' => 'if a == nil then return 0 end return a * 2;',
            ],
            'accumulate' => [
                'java' => 'return (state == null ? 0 : state) + (val == null ? 0 : val);',
                'lua' => 'if state == nil then state = 0 end if val == nil then return state end return state + val;',
            ],
            'avgState' => [
                'java' => 'if (val != null) { state.setInt(0, state.getInt(0) + 1); '
                    . 'state.setLong(1, state.getLong(1) + val.intValue()); } return state;',
                'lua' => 'if val == nil then return state end return {state[1] + 1, state[2] + val};',
            ],
            'avgFinal' => [
                'java' => 'if (state.getInt(0) == 0) return null; '
                    . 'double r = state.getLong(1); r /= state.getInt(0); return Double.valueOf(r);',
                'lua' => 'if state[1] == 0 then return nil end return state[2] / state[1];',
            ],
        ];

        return $bodies[$name][self::udfLanguage()];
    }

    private static function udfLanguage(): string {
        return self::isScyllaDb() ? 'lua' : 'java';
    }
}
