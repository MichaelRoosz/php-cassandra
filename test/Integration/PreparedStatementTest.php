<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Value;
use PHPUnit\Framework\TestCase;

final class PreparedStatementTest extends TestCase {
    public function testPrepareAndExecuteWithNamedBinds(): void {
        $conn = $this->newConnection();

        $conn->query('TRUNCATE users');

        $prepared = $conn->prepare('INSERT INTO users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)');
        $conn->execute(
            $prepared,
            [
                'id' => Value\Uuid::fromValue(self::uuidV4()),
                'org_id' => 7,
                'name' => 'alice',
                'age' => 28,
            ],
            Consistency::ONE,
            new ExecuteOptions(namesForValues: true)
        );

        $sel = $conn->prepare('SELECT count(*) FROM users WHERE org_id = :org_id');
        $rows = $conn->execute($sel, ['org_id' => 7], Consistency::ONE, new ExecuteOptions(namesForValues: true))->asRowsResult();
        $this->assertSame(1, (int) $rows->fetchColumn(0));
    }

    public function testPreparedPagingUsingPreviousResult(): void {
        $conn = $this->newConnection();

        // Seed deterministic number of rows for a unique org_id
        $orgId = random_int(10000, 99999);
        $ins = $conn->prepare('INSERT INTO users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)');
        $numRows = 60;
        for ($i = 0; $i < $numRows; $i++) {
            $conn->execute(
                $ins,
                [
                    'id' => Value\Uuid::fromValue(self::uuidV4()),
                    'org_id' => $orgId,
                    'name' => 'u' . $i,
                    'age' => 20 + ($i % 10),
                ],
                Consistency::ONE,
                new ExecuteOptions(namesForValues: true)
            );
        }

        // Prepared select with paging, assert exact count
        $prepared = $conn->prepare('SELECT id, name FROM users WHERE org_id = :org_id');
        $rows = $conn->execute(
            $prepared,
            ['org_id' => $orgId],
            Consistency::ONE,
            new ExecuteOptions(pageSize: 25, namesForValues: true)
        )->asRowsResult();

        $seen = 0;
        do {
            foreach ($rows as $row) {
                $this->assertArrayHasKey('name', $row);
                $this->assertMatchesRegularExpression('/^u\\d+$/', (string) $row['name']);
                $seen++;
            }
            $state = $rows->getRowsMetadata()->pagingState;
            if ($state === null) {
                break;
            }
            $rows = $conn->execute(
                $rows,
                ['org_id' => $orgId],
                Consistency::ONE,
                new ExecuteOptions(pageSize: 25, namesForValues: true, pagingState: $state)
            )->asRowsResult();
        } while (true);

        $this->assertSame($numRows, $seen);
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
        $conn->connect();
        $this->assertTrue($conn->isConnected());

        return $conn;
    }

    private static function uuidV4(): string {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
