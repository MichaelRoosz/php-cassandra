<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Response\Result\RowClassInterface;
use Cassandra\Response\Result\FetchType;
use Cassandra\Type;
use Cassandra\Value;

final class ResultObjectMappingTest extends AbstractIntegrationTestCase {
    public function testCustomRowClassWithConstructorArgsAndFetchTypes(): void {
        $conn = $this->connection;

        $filename = 'omap_' . bin2hex(random_bytes(4));
        $pairs = [
            ['k' => 'ak', 'v' => '1'],
            ['k' => 'bk', 'v' => '2'],
        ];
        foreach ($pairs as $i => $p) {
            $conn->query(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    Value\Varchar::fromValue($filename),
                    Value\Varchar::fromValue($p['k']),
                    Value\MapCollection::fromValue(['a' => $p['v']], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }

        $rows = $conn->query(
            'SELECT filename, ukey FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)]
        )->asRowsResult();

        $rows->configureFetchObject(TestRow::class, ['suffix' => '_z'], FetchType::BOTH);

        $first = $rows->fetchObject();
        $this->assertInstanceOf(TestRow::class, $first);
        /** @var TestRow $first */
        $first = $first;
        $this->assertStringEndsWith('_z', $first->ukey());
        $this->assertNotSame('', $first->filename());

        $second = $rows->fetchObject();
        $this->assertInstanceOf(TestRow::class, $second);
        /** @var TestRow $second */
        $second = $second;
        $this->assertStringEndsWith('_z', $second->ukey());

        $this->assertFalse($rows->fetchObject());
    }
    public function testDefaultRowClassViaIteratorAndFetchObject(): void {
        $conn = $this->connection;

        $filename = 'omap_' . bin2hex(random_bytes(4));
        for ($i = 0; $i < 3; $i++) {
            $conn->query(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    Value\Varchar::fromValue($filename),
                    Value\Varchar::fromValue('k' . $i),
                    Value\MapCollection::fromValue(['a' => (string) $i], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }

        $rows = $conn->query(
            'SELECT filename, ukey FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)]
        )->asRowsResult();

        // Without configureFetchObject() iterator yields arrays; set configuration then iterator yields objects
        $rows->configureFetchObject(\Cassandra\Response\Result\RowClass::class, [], FetchType::ASSOC);

        $seen = [];
        foreach ($rows as $rowObj) {
            $this->assertInstanceOf(RowClassInterface::class, $rowObj);
            $seen[] = $rowObj->ukey;
        }

        sort($seen, SORT_STRING);
        $this->assertSame(['k0', 'k1', 'k2'], $seen);

        // Rewind and consume using fetchObject()
        $rows->rewind();
        $collected = [];
        while (($obj = $rows->fetchObject()) !== false) {
            $this->assertInstanceOf(RowClassInterface::class, $obj);
            $collected[] = $obj->ukey;
        }
        sort($collected, SORT_STRING);
        $this->assertSame(['k0', 'k1', 'k2'], $collected);
    }

    public function testFetchAllObjectsReturnsAllInstances(): void {
        $conn = $this->connection;

        $filename = 'omap_' . bin2hex(random_bytes(4));
        $count = 4;
        for ($i = 0; $i < $count; $i++) {
            $conn->query(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    Value\Varchar::fromValue($filename),
                    Value\Varchar::fromValue('f' . $i),
                    Value\MapCollection::fromValue(['a' => (string) $i], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }

        $rows = $conn->query(
            'SELECT filename, ukey FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)]
        )->asRowsResult();

        $rows->configureFetchObject(TestRow::class);
        $objects = $rows->fetchAllObjects();

        $this->assertCount($count, $objects);
        foreach ($objects as $o) {
            $this->assertInstanceOf(TestRow::class, $o);
        }

        // Exhausted
        $this->assertFalse($rows->fetchObject());
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS storage(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->disconnect();
    }
}

final class TestRow implements RowClassInterface {
    /** @var array<string,mixed> */
    private array $args;
    /** @var array<string,mixed> */
    private array $row;

    /**
     * @param array<mixed> $rowData
     * @param array<mixed> $additionalArguments
     */
    public function __construct(array $rowData, array $additionalArguments = []) {
        $this->row = $rowData;
        $this->args = $additionalArguments;
    }

    public function __get(string $name): mixed {
        return $this->row[$name] ?? null;
    }

    public function filename(): string {
        return (string) ($this->row['filename'] ?? '');
    }

    public function ukey(): string {
        $suffix = (string) ($this->args['suffix'] ?? '');

        return (string) ($this->row['ukey'] ?? '') . $suffix;
    }
}
