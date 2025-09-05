<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Response\Result\FetchType;
use Cassandra\Type;
use Cassandra\Value;

final class ResultFetchingTest extends AbstractIntegrationTestCase {
    public function testFetchVariantsAndIterator(): void {
        $conn = $this->connection;

        // Seed a few simple rows
        $filename = 'itest_' . bin2hex(random_bytes(4));
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
            'SELECT ukey FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)]
        )->asRowsResult();

        // Iterator
        $all = [];
        foreach ($rows as $row) {
            $this->assertIsArray($row);
            $all[] = $row['ukey'];
        }
        $sorted = $all;
        sort($sorted, SORT_STRING);
        $this->assertSame(['k0', 'k1', 'k2'], $sorted);

        // Rewind and fetch variants
        $rows->rewind();

        $first = $rows->fetch(FetchType::ASSOC);
        $this->assertIsArray($first);
        $this->assertArrayHasKey('ukey', $first);

        $secondNum = $rows->fetch(FetchType::NUM);
        $this->assertIsArray($secondNum);
        $this->assertArrayHasKey(0, $secondNum);

        $thirdBoth = $rows->fetch(FetchType::BOTH);
        $this->assertIsArray($thirdBoth);
        $this->assertArrayHasKey('ukey', $thirdBoth);
        $this->assertArrayHasKey(0, $thirdBoth);
        $this->assertSame($thirdBoth['ukey'], $thirdBoth[0]);

        $fetchedKeys = [$first['ukey'], $secondNum[0], $thirdBoth['ukey']];
        sort($fetchedKeys, SORT_STRING);
        $this->assertSame(['k0', 'k1', 'k2'], $fetchedKeys);

        // Fetch column on exhausted should be false
        $this->assertFalse($rows->fetchColumn(0));
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS storage(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->disconnect();
    }
}
