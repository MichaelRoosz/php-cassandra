<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

/**
 * Cassandra distinguishes a null cell ([bytes] length -1) from an *empty* one
 * (length 0). Empty is a legal value for every type, including the fixed-length
 * ones, and is produced by e.g. blobAsInt(0x) or by legacy compact-storage
 * tables.
 *
 * A decoder that reads its type's fixed size regardless of the declared length
 * consumes bytes belonging to the following cell, which corrupts every
 * remaining value in the row — and, because rows are read from one continuous
 * buffer, in the rest of the page too. These tests pin the length down as
 * authoritative.
 */
final class EmptyValueIntegrationTest extends AbstractIntegrationTestCase {
    public function testEmptyFixedLengthValueDecodesToNull(): void {
        $this->connection->query(
            'CREATE TABLE IF NOT EXISTS test_empty_fixed (
                id int PRIMARY KEY,
                bigint_value bigint,
                boolean_value boolean,
                decimal_value decimal,
                double_value double,
                float_value float,
                inet_value inet,
                int_value int,
                timestamp_value timestamp,
                timeuuid_value timeuuid,
                uuid_value uuid,
                varint_value varint
            )'
        );

        // Note that not every type has a blobAs* that accepts 0x — Cassandra
        // rejects it for smallint/tinyint/date/time — so those are covered by
        // the unit tests instead.
        $this->connection->query(
            'INSERT INTO test_empty_fixed (
                id, bigint_value, boolean_value, decimal_value, double_value, float_value,
                inet_value, int_value, timestamp_value, timeuuid_value, uuid_value, varint_value
            ) VALUES (
                1, blobAsBigint(0x), blobAsBoolean(0x), blobAsDecimal(0x), blobAsDouble(0x), blobAsFloat(0x),
                blobAsInet(0x), blobAsInt(0x), blobAsTimestamp(0x), blobAsTimeuuid(0x), blobAsUuid(0x), blobAsVarint(0x)
            )'
        );

        $rows = $this->connection->query(
            'SELECT id, bigint_value, boolean_value, decimal_value, double_value, float_value,
                    inet_value, int_value, timestamp_value, timeuuid_value, uuid_value, varint_value
             FROM test_empty_fixed WHERE id = 1'
        )->asRowsResult()->fetchAll();

        $this->assertCount(1, $rows);

        $row = $rows[0];

        // The id is a normal value; every column after it depends on each empty
        // cell having consumed exactly zero bytes of body.
        $this->assertSame(1, $row['id']);

        foreach ([
            'bigint_value',
            'boolean_value',
            'decimal_value',
            'double_value',
            'float_value',
            'inet_value',
            'int_value',
            'timestamp_value',
            'timeuuid_value',
            'uuid_value',
            'varint_value',
        ] as $column) {
            $this->assertNull($row[$column], $column . ' should decode to null');
        }
    }

    public function testEmptyValueDoesNotCorruptFollowingColumns(): void {
        $this->connection->query(
            'CREATE TABLE IF NOT EXISTS test_empty_neighbours (
                id int PRIMARY KEY,
                empty_value int,
                text_value text,
                int_value int
            )'
        );

        $this->connection->query(
            "INSERT INTO test_empty_neighbours (id, empty_value, text_value, int_value)
             VALUES (1, blobAsInt(0x), 'hello world', 4242)"
        );

        $rows = $this->connection->query(
            'SELECT id, empty_value, text_value, int_value FROM test_empty_neighbours WHERE id = 1'
        )->asRowsResult()->fetchAll();

        $this->assertSame([[
            'id' => 1,
            'empty_value' => null,
            'text_value' => 'hello world',
            'int_value' => 4242,
        ]], $rows);
    }

    public function testEmptyValueDoesNotCorruptFollowingRows(): void {
        $this->connection->query(
            'CREATE TABLE IF NOT EXISTS test_empty_rows (
                partition int,
                id int,
                empty_value int,
                text_value text,
                PRIMARY KEY (partition, id)
            )'
        );

        for ($id = 1; $id <= 5; $id++) {
            $this->connection->query(
                "INSERT INTO test_empty_rows (partition, id, empty_value, text_value)
                 VALUES (1, {$id}, blobAsInt(0x), 'row {$id}')"
            );
        }

        $rows = $this->connection->query(
            'SELECT id, empty_value, text_value FROM test_empty_rows WHERE partition = 1'
        )->asRowsResult()->fetchAll();

        $this->assertCount(5, $rows);

        foreach ($rows as $index => $row) {
            $expectedId = $index + 1;
            $this->assertSame($expectedId, $row['id']);
            $this->assertNull($row['empty_value']);
            $this->assertSame('row ' . $expectedId, $row['text_value']);
        }
    }

    /**
     * An empty value on a variable-length type keeps its natural empty
     * representation rather than becoming null.
     */
    public function testEmptyVariableLengthValueDecodesToEmptyString(): void {
        $this->connection->query(
            'CREATE TABLE IF NOT EXISTS test_empty_varlen (
                id int PRIMARY KEY,
                blob_value blob,
                text_value text
            )'
        );

        $this->connection->query(
            "INSERT INTO test_empty_varlen (id, blob_value, text_value) VALUES (1, 0x, '')"
        );

        $rows = $this->connection->query(
            'SELECT blob_value, text_value FROM test_empty_varlen WHERE id = 1'
        )->asRowsResult()->fetchAll();

        $this->assertSame([[
            'blob_value' => '',
            'text_value' => '',
        ]], $rows);
    }

    /**
     * Rows written before an ALTER TYPE ... ADD keep the shorter encoding, so a
     * UDT value can carry fewer fields than the type declares. The missing
     * trailing fields are absent from the wire, not null-encoded — reading them
     * anyway consumes the following column.
     */
    public function testUdtWithFewerFieldsThanTypeDeclares(): void {
        $this->connection->query('CREATE TYPE IF NOT EXISTS test_short_udt_type (street text, city text)');
        $this->connection->query(
            'CREATE TABLE IF NOT EXISTS test_short_udt (
                id int PRIMARY KEY,
                address frozen<test_short_udt_type>,
                tail_value int
            )'
        );

        $this->connection->query(
            "INSERT INTO test_short_udt (id, address, tail_value)
             VALUES (1, {street: 'Main St', city: 'Berlin'}, 4242)"
        );

        $this->connection->query('ALTER TYPE test_short_udt_type ADD zip int');

        $rows = $this->connection->query(
            'SELECT address, tail_value FROM test_short_udt WHERE id = 1'
        )->asRowsResult()->fetchAll();

        $this->assertSame([[
            'address' => [
                'street' => 'Main St',
                'city' => 'Berlin',
                'zip' => null,
            ],
            'tail_value' => 4242,
        ]], $rows);
    }
}
