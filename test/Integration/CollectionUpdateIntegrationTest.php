<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Exception\ServerException\InvalidException;
use Cassandra\Type;
use Cassandra\Value;

/**
 * Integration tests for CQL collection updates on set, map and list columns.
 *
 * Covers the operations described in the DataStax CQL documentation:
 * - https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertSet.html
 * - https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertList.html
 * - https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertMap.html
 */
final class CollectionUpdateIntegrationTest extends AbstractIntegrationTestCase {
    public function testCollectionUpdateWithConditionalUpdate(): void {
        $id = 60;
        $table = "{$this->keyspace}.collection_update_set";

        $rejected = $this->connection->query(
            "UPDATE {$table} SET tags = tags + ? WHERE id = ? IF EXISTS",
            [['cassandra'], $id]
        )->asRowsResult()->fetch();

        $this->assertIsArray($rejected);
        $this->assertFalse($rejected['[applied]'], 'IF EXISTS must not create a row');
        $this->assertFalse(
            $this->connection->query("SELECT tags FROM {$table} WHERE id = ?", [$id])
                ->asRowsResult()->fetch(),
            'No row must exist after a rejected IF EXISTS update'
        );

        $this->connection->query("INSERT INTO {$table} (id, tags) VALUES (?, ?)", [$id, ['php']]);

        $applied = $this->connection->query(
            "UPDATE {$table} SET tags = tags + ? WHERE id = ? IF EXISTS",
            [['cassandra'], $id]
        )->asRowsResult()->fetch();

        $this->assertIsArray($applied);
        $this->assertTrue($applied['[applied]'], 'Conditional collection update should be applied');
        $this->assertSetEquals(['cassandra', 'php'], $this->fetchSet($table, $id));
    }

    public function testCollectionUpdateWithTtl(): void {
        // TTL()/WRITETIME() selection on a non-frozen (multi-cell) collection
        // is rejected by the server (CASSANDRA-17628); only the write side of
        // "USING TTL" against a collection is exercised here. Frozen
        // collections do support TTL()/WRITETIME(), see
        // testFrozenCollectionSupportsFullAssignment().
        $id = 61;
        $table = "{$this->keyspace}.collection_update_set";

        $this->connection->query("INSERT INTO {$table} (id, tags) VALUES (?, ?)", [$id, ['php']]);

        $this->connection->query(
            "UPDATE {$table} USING TTL ? SET tags = tags + ? WHERE id = ?",
            [3600, ['cassandra'], $id]
        );

        $this->assertSetEquals(['cassandra', 'php'], $this->fetchSet($table, $id));
    }

    public function testFrozenCollectionRejectsIncrementalUpdate(): void {
        $id = 50;
        $table = "{$this->keyspace}.collection_update_frozen";

        $this->connection->query(
            "INSERT INTO {$table} (id, tags) VALUES (?, ?)",
            [$id, Value\SetCollection::fromValue(['php'], Type::VARCHAR, isFrozen: true)]
        );

        $this->assertSetEquals(['php'], $this->fetchColumn($table, 'tags', $id));

        $this->expectException(InvalidException::class);
        $this->connection->query(
            "UPDATE {$table} SET tags = tags + ? WHERE id = ?",
            [Value\SetCollection::fromValue(['cassandra'], Type::VARCHAR, isFrozen: true), $id]
        );
    }

    public function testFrozenCollectionSupportsFullAssignment(): void {
        $id = 51;
        $table = "{$this->keyspace}.collection_update_frozen";

        $this->connection->query(
            "INSERT INTO {$table} (id, tags) VALUES (?, ?)",
            [$id, Value\SetCollection::fromValue(['php'], Type::VARCHAR, isFrozen: true)]
        );

        $this->connection->query(
            "UPDATE {$table} SET tags = ? WHERE id = ?",
            [Value\SetCollection::fromValue(['cassandra', 'php'], Type::VARCHAR, isFrozen: true), $id]
        );

        $this->assertSetEquals(
            ['cassandra', 'php'],
            $this->fetchColumn($table, 'tags', $id),
            'Frozen collections must be replaced as a whole'
        );
    }

    public function testListAppendAndPrepend(): void {
        $id = 10;
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0002']]
        );

        // Append: list name first, then the new element.
        $this->connection->query(
            "UPDATE {$table} SET phones = phones + ? WHERE id = ?",
            [['555-0003'], $id]
        );

        // Prepend: new element first, then the list name.
        $this->connection->query(
            "UPDATE {$table} SET phones = ? + phones WHERE id = ?",
            [['555-0001'], $id]
        );

        $this->assertSame(
            ['555-0001', '555-0002', '555-0003'],
            $this->fetchList($table, $id),
            'Prepend must place elements in front, append behind, preserving list order'
        );
    }

    public function testListIncrementalUpdateWithNativeArrays(): void {
        $id = 2;
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0001']]
        );

        $this->connection->query(
            "UPDATE {$table} SET phones = phones + ? WHERE id = ?",
            [['555-0002'], $id]
        );

        $this->assertSame(
            ['555-0001', '555-0002'],
            $this->fetchList($table, $id),
            'List add with native array bind should append elements'
        );

        $this->connection->query(
            "UPDATE {$table} SET phones = phones - ? WHERE id = ?",
            [['555-0001'], $id]
        );

        $this->assertSame(
            ['555-0002'],
            $this->fetchList($table, $id),
            'List subtract with native array bind should remove matching elements'
        );
    }

    public function testListIncrementalUpdateWithValueObjects(): void {
        $id = Value\Int32::fromValue(1);
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, Value\ListCollection::fromValue(['555-0001'], Type::VARCHAR)]
        );

        $this->connection->query(
            "UPDATE {$table} SET phones = phones + ? WHERE id = ?",
            [Value\ListCollection::fromValue(['555-0002'], Type::VARCHAR), $id]
        );

        $this->assertSame(
            ['555-0001', '555-0002'],
            $this->fetchList($table, $id),
            'List add should append elements'
        );

        $this->connection->query(
            "UPDATE {$table} SET phones = phones - ? WHERE id = ?",
            [Value\ListCollection::fromValue(['555-0001'], Type::VARCHAR), $id]
        );

        $this->assertSame(
            ['555-0002'],
            $this->fetchList($table, $id),
            'List subtract should remove matching elements'
        );
    }

    public function testListInsertReplacesEntireList(): void {
        $id = 11;
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0001', '555-0002']]
        );

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0009']]
        );

        $this->assertSame(
            ['555-0009'],
            $this->fetchList($table, $id),
            'INSERT (and plain assignment) replaces the whole list'
        );
    }

    public function testListRemoveByIndexUsingDelete(): void {
        $id = 12;
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0001', '555-0002', '555-0003']]
        );

        // Literal index.
        $this->connection->query("DELETE phones[1] FROM {$table} WHERE id = ?", [$id]);

        $this->assertSame(['555-0001', '555-0003'], $this->fetchList($table, $id));

        // Bound index.
        $this->connection->query("DELETE phones[?] FROM {$table} WHERE id = ?", [0, $id]);

        $this->assertSame(
            ['555-0003'],
            $this->fetchList($table, $id),
            'DELETE list[i] removes by position, not by value'
        );
    }

    public function testListRemoveByValueRemovesAllOccurrences(): void {
        $id = 13;
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0001', '555-0002', '555-0001']]
        );

        $this->connection->query(
            "UPDATE {$table} SET phones = phones - ? WHERE id = ?",
            [['555-0001'], $id]
        );

        $this->assertSame(
            ['555-0002'],
            $this->fetchList($table, $id),
            'List subtraction matches by value and removes every occurrence'
        );
    }

    public function testListSetToEmptyIsStoredAsNull(): void {
        $id = 14;
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0001']]
        );

        $this->connection->query(
            "UPDATE {$table} SET phones = ? WHERE id = ?",
            [Value\ListCollection::fromValue([], Type::VARCHAR), $id]
        );

        $this->assertNull(
            $this->fetchList($table, $id),
            'An empty list is stored as null'
        );
    }

    public function testListUpdateAtIndexPosition(): void {
        $id = 15;
        $table = "{$this->keyspace}.collection_update_list";

        $this->connection->query(
            "INSERT INTO {$table} (id, phones) VALUES (?, ?)",
            [$id, ['555-0001', '555-0002', '555-0003']]
        );

        // Literal index.
        $this->connection->query(
            "UPDATE {$table} SET phones[1] = ? WHERE id = ?",
            ['555-9002', $id]
        );

        // Bound index.
        $this->connection->query(
            "UPDATE {$table} SET phones[?] = ? WHERE id = ?",
            [2, '555-9003', $id]
        );

        $this->assertSame(
            ['555-0001', '555-9002', '555-9003'],
            $this->fetchList($table, $id),
            'Indexed assignment overwrites the element at that position'
        );
    }

    public function testMapDeleteSingleKey(): void {
        $id = 20;
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, [2011 => 'Team A', 2012 => 'Team B', 2013 => 'Team C']]
        );

        // Literal key.
        $this->connection->query("DELETE teams[2012] FROM {$table} WHERE id = ?", [$id]);

        $this->assertMapEquals([2011 => 'Team A', 2013 => 'Team C'], $this->fetchMap($table, $id));

        // Bound key.
        $this->connection->query("DELETE teams[?] FROM {$table} WHERE id = ?", [2013, $id]);

        $this->assertMapEquals(
            [2011 => 'Team A'],
            $this->fetchMap($table, $id),
            'DELETE map[key] removes a single entry'
        );
    }

    public function testMapIncrementalUpdateWithNativeArrays(): void {
        $id = 2;
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, [2020 => 'Team A']]
        );

        $this->connection->query(
            "UPDATE {$table} SET teams = teams + ? WHERE id = ?",
            [[2021 => 'Team B'], $id]
        );

        $this->assertMapEquals(
            [2020 => 'Team A', 2021 => 'Team B'],
            $this->fetchMap($table, $id),
            'Map add with native array bind should merge new entries'
        );

        $this->connection->query(
            "UPDATE {$table} SET teams = teams - ? WHERE id = ?",
            [[2020], $id]
        );

        $this->assertMapEquals(
            [2021 => 'Team B'],
            $this->fetchMap($table, $id),
            'Map subtract with native array bind should remove keys'
        );
    }

    public function testMapIncrementalUpdateWithValueObjects(): void {
        $id = Value\Int32::fromValue(1);
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, Value\MapCollection::fromValue([2020 => 'Team A'], Type::INT, Type::VARCHAR)]
        );

        $this->connection->query(
            "UPDATE {$table} SET teams = teams + ? WHERE id = ?",
            [Value\MapCollection::fromValue([2021 => 'Team B'], Type::INT, Type::VARCHAR), $id]
        );

        $this->assertMapEquals(
            [2020 => 'Team A', 2021 => 'Team B'],
            $this->fetchMap($table, $id),
            'Map add should merge new entries'
        );

        $this->connection->query(
            "UPDATE {$table} SET teams = teams - ? WHERE id = ?",
            [Value\SetCollection::fromValue([2020], Type::INT), $id]
        );

        $this->assertMapEquals(
            [2021 => 'Team B'],
            $this->fetchMap($table, $id),
            'Map subtract should remove keys (values in bind are ignored)'
        );
    }

    public function testMapInsertReplacesEntireMap(): void {
        $id = 21;
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, [2011 => 'Team A', 2012 => 'Team B']]
        );

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, [2020 => 'Team Z']]
        );

        $this->assertMapEquals(
            [2020 => 'Team Z'],
            $this->fetchMap($table, $id),
            'INSERT replaces the entire map'
        );
    }

    public function testMapRemoveKeysWithNativeListOfKeys(): void {
        $id = 22;
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, [2011 => 'Team A', 2012 => 'Team B', 2013 => 'Team C']]
        );

        // The right-hand operand of map subtraction is a set<keyType>, so a plain
        // PHP list of keys is the natural bind - not a key => value map.
        $this->connection->query(
            "UPDATE {$table} SET teams = teams - ? WHERE id = ?",
            [[2011, 2012], $id]
        );

        $this->assertMapEquals(
            [2013 => 'Team C'],
            $this->fetchMap($table, $id),
            'Map subtraction takes a set of keys'
        );
    }

    public function testMapSetKeyToNullDeletesEntry(): void {
        $id = 23;
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, [2011 => 'Team A', 2012 => 'Team B']]
        );

        $this->connection->query(
            "UPDATE {$table} SET teams[?] = ? WHERE id = ?",
            [2011, null, $id]
        );

        $this->assertMapEquals(
            [2012 => 'Team B'],
            $this->fetchMap($table, $id),
            'Assigning null to a map key deletes that entry'
        );
    }

    public function testMapSetToEmptyIsStoredAsNull(): void {
        $id = 24;
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, [2011 => 'Team A']]
        );

        $this->connection->query(
            "UPDATE {$table} SET teams = ? WHERE id = ?",
            [Value\MapCollection::fromValue([], Type::INT, Type::VARCHAR), $id]
        );

        $this->assertNull(
            $this->fetchMap($table, $id),
            'An empty map is stored as null'
        );
    }

    public function testMapSingleKeyUpdate(): void {
        $id = Value\Int32::fromValue(3);
        $table = "{$this->keyspace}.collection_update_map";

        $this->connection->query(
            "INSERT INTO {$table} (id, teams) VALUES (?, ?)",
            [$id, Value\MapCollection::fromValue([], Type::INT, Type::VARCHAR)]
        );

        // Literal key.
        $this->connection->query(
            "UPDATE {$table} SET teams[2006] = ? WHERE id = ?",
            ['Team DSB', $id]
        );

        // Bound key.
        $this->connection->query(
            "UPDATE {$table} SET teams[?] = ? WHERE id = ?",
            [2007, 'Team DSB - Ballast Nedam', $id]
        );

        $this->assertMapEquals(
            [2006 => 'Team DSB', 2007 => 'Team DSB - Ballast Nedam'],
            $this->fetchMap($table, $id),
            'Map indexed assignment should set a single key'
        );
    }

    public function testNestedCollectionIncrementalUpdate(): void {
        $id = 40;
        $table = "{$this->keyspace}.collection_update_nested";

        $this->connection->query(
            "INSERT INTO {$table} (id, races, groups) VALUES (?, ?, ?)",
            [$id, [2015 => ['Tour de Suisse']], [[1, 2]]]
        );

        $this->connection->query(
            "UPDATE {$table} SET races = races + ? WHERE id = ?",
            [[2016 => ['Ronde van Gelderland']], $id]
        );

        $this->connection->query(
            "UPDATE {$table} SET groups = groups + ? WHERE id = ?",
            [[[3, 4]], $id]
        );

        $row = $this->connection->query(
            "SELECT races, groups FROM {$table} WHERE id = ?",
            [$id]
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertMapEquals(
            [2015 => ['Tour de Suisse'], 2016 => ['Ronde van Gelderland']],
            $row['races'],
            'map<int, frozen<list<varchar>>> supports incremental merge'
        );
        $this->assertIsArray($row['groups']);
        $this->assertCount(2, $row['groups'], 'set<frozen<list<int>>> supports incremental add');
        $this->assertContains([1, 2], $row['groups']);
        $this->assertContains([3, 4], $row['groups']);

        $this->connection->query(
            "UPDATE {$table} SET groups = groups - ? WHERE id = ?",
            [[[1, 2]], $id]
        );

        $groups = $this->fetchColumn($table, 'groups', $id);
        $this->assertSame([[3, 4]], $groups, 'set<frozen<list<int>>> supports incremental remove');
    }

    public function testSetClearWithEmptyLiteralAndDelete(): void {
        $id = 30;
        $table = "{$this->keyspace}.collection_update_set";

        $this->connection->query("INSERT INTO {$table} (id, tags) VALUES (?, ?)", [$id, ['php', 'cassandra']]);

        $this->connection->query("UPDATE {$table} SET tags = {} WHERE id = ?", [$id]);

        $this->assertNull(
            $this->fetchSet($table, $id),
            'Clearing a set with the empty literal stores null'
        );

        $this->connection->query("INSERT INTO {$table} (id, tags) VALUES (?, ?)", [$id, ['php']]);
        $this->connection->query("DELETE tags FROM {$table} WHERE id = ?", [$id]);

        $this->assertNull(
            $this->fetchSet($table, $id),
            'Deleting the column clears the set'
        );
    }

    public function testSetIncrementalUpdateWithNativeArrays(): void {
        $id = 2;
        $table = "{$this->keyspace}.collection_update_set";

        $this->connection->query(
            "INSERT INTO {$table} (id, tags) VALUES (?, ?)",
            [$id, ['php']]
        );

        $this->connection->query(
            "UPDATE {$table} SET tags = tags + ? WHERE id = ?",
            [['cassandra'], $id]
        );

        $this->assertSetEquals(
            ['cassandra', 'php'],
            $this->fetchSet($table, $id),
            'Set add with native array bind should merge new members'
        );

        $this->connection->query(
            "UPDATE {$table} SET tags = tags - ? WHERE id = ?",
            [['php'], $id]
        );

        $this->assertSetEquals(
            ['cassandra'],
            $this->fetchSet($table, $id),
            'Set subtract with native array bind should remove members'
        );
    }

    public function testSetIncrementalUpdateWithValueObjects(): void {
        $id = Value\Int32::fromValue(1);
        $table = "{$this->keyspace}.collection_update_set";

        $this->connection->query(
            "INSERT INTO {$table} (id, tags) VALUES (?, ?)",
            [$id, Value\SetCollection::fromValue(['php'], Type::VARCHAR)]
        );

        $this->connection->query(
            "UPDATE {$table} SET tags = tags + ? WHERE id = ?",
            [Value\SetCollection::fromValue(['cassandra'], Type::VARCHAR), $id]
        );

        $this->assertSetEquals(
            ['cassandra', 'php'],
            $this->fetchSet($table, $id),
            'Set add should merge new members'
        );

        $this->connection->query(
            "UPDATE {$table} SET tags = tags - ? WHERE id = ?",
            [Value\SetCollection::fromValue(['php'], Type::VARCHAR), $id]
        );

        $this->assertSetEquals(
            ['cassandra'],
            $this->fetchSet($table, $id),
            'Set subtract should remove members'
        );
    }

    public function testSetInsertReplacesEntireSet(): void {
        $id = 31;
        $table = "{$this->keyspace}.collection_update_set";

        $this->connection->query("INSERT INTO {$table} (id, tags) VALUES (?, ?)", [$id, ['php', 'cassandra']]);
        $this->connection->query("INSERT INTO {$table} (id, tags) VALUES (?, ?)", [$id, ['rust']]);

        $this->assertSetEquals(
            ['rust'],
            $this->fetchSet($table, $id),
            'INSERT replaces the entire set'
        );
    }

    public function testUdtElementsInCollection(): void {
        $id = 41;
        $table = "{$this->keyspace}.collection_update_udt";

        $this->connection->query(
            "INSERT INTO {$table} (id, races) VALUES (?, ?)",
            [$id, [['race_title' => 'Rabobank 7-Dorpenomloop Aalburg', 'race_year' => 2015]]]
        );

        $this->connection->query(
            "UPDATE {$table} SET races = races + ? WHERE id = ?",
            [[['race_title' => 'Ronde van Gelderland', 'race_year' => 2016]], $id]
        );

        $this->assertSame(
            [
                ['race_title' => 'Rabobank 7-Dorpenomloop Aalburg', 'race_year' => 2015],
                ['race_title' => 'Ronde van Gelderland', 'race_year' => 2016],
            ],
            $this->fetchColumn($table, 'races', $id),
            'list<frozen<udt>> supports incremental append'
        );
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS collection_update_set (id int PRIMARY KEY, tags set<varchar>)'
        );
        $conn->query(
            'CREATE TABLE IF NOT EXISTS collection_update_map (id int PRIMARY KEY, teams map<int, varchar>)'
        );
        $conn->query(
            'CREATE TABLE IF NOT EXISTS collection_update_list (id int PRIMARY KEY, phones list<varchar>)'
        );
        $conn->query(
            'CREATE TABLE IF NOT EXISTS collection_update_frozen (id int PRIMARY KEY, tags frozen<set<varchar>>)'
        );
        $conn->query(
            'CREATE TABLE IF NOT EXISTS collection_update_nested ('
            . 'id int PRIMARY KEY, '
            . 'races map<int, frozen<list<varchar>>>, '
            . 'groups set<frozen<list<int>>>)'
        );
        $conn->query(
            'CREATE TYPE IF NOT EXISTS collection_update_race (race_title varchar, race_year int)'
        );
        $conn->query(
            'CREATE TABLE IF NOT EXISTS collection_update_udt ('
            . 'id int PRIMARY KEY, races list<frozen<collection_update_race>>)'
        );
        $conn->disconnect();
    }

    /**
     * @param array<int, mixed> $expected
     */
    private function assertMapEquals(array $expected, mixed $actual, string $message = ''): void {
        $this->assertIsArray($actual, $message);
        ksort($expected);
        ksort($actual);
        $this->assertSame($expected, $actual, $message);
    }

    /**
     * @param list<string> $expected
     */
    private function assertSetEquals(array $expected, mixed $actual, string $message = ''): void {
        $this->assertIsArray($actual, $message);
        $actualSorted = $actual;
        sort($actualSorted);
        $expectedSorted = $expected;
        sort($expectedSorted);
        $this->assertSame($expectedSorted, $actualSorted, $message);
    }

    private function fetchColumn(string $table, string $column, Value\Int32|int $id): mixed {
        $bindId = $id instanceof Value\Int32 ? $id : Value\Int32::fromValue($id);

        $result = $this->connection->query(
            "SELECT {$column} FROM {$table} WHERE id = ?",
            [$bindId]
        )->asRowsResult();

        $row = $result->fetch();
        $this->assertIsArray($row);

        return $row[$column];
    }

    private function fetchList(string $table, Value\Int32|int $id): mixed {
        return $this->fetchColumn($table, 'phones', $id);
    }

    private function fetchMap(string $table, Value\Int32|int $id): mixed {
        return $this->fetchColumn($table, 'teams', $id);
    }

    private function fetchSet(string $table, Value\Int32|int $id): mixed {
        return $this->fetchColumn($table, 'tags', $id);
    }
}
