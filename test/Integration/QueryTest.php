<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Value;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Exception\ServerException\InvalidException;
use Cassandra\Exception\ServerException\SyntaxErrorException;
use Cassandra\Response\Result\CachedPreparedResult;
use Cassandra\Response\Result\PreparedResult;

final class QueryTest extends AbstractIntegrationTestCase {
    public function testMissingTableRaisesException(): void {
        $conn = $this->connection;
        $this->expectException(InvalidException::class);
        $conn->query('SELECT * FROM does_not_exist_123');
    }
    public function testPositionalBindAndTypes(): void {

        $conn = $this->connection;
        $rows = $conn->query(
            'SELECT key FROM system.local WHERE key = ?',
            [Value\Ascii::fromValue('local')],
            Consistency::ONE,
            new QueryOptions()
        )->asRowsResult();

        $this->assertSame(1, $rows->getRowCount());
        $this->assertSame(['key' => 'local'], $rows->fetch());
    }

    public function testPreparedQueryCache(): void {

        $conn = $this->connection;
        $r1 = $conn->prepare('SELECT key FROM system.local WHERE key = ?');
        $r2 = $conn->prepare('SELECT key FROM system.local WHERE key = ?');
        $this->assertInstanceOf(PreparedResult::class, $r1);
        $this->assertInstanceOf(CachedPreparedResult::class, $r2);
    }

    public function testSimpleSelect(): void {

        $conn = $this->connection;
        $rows = $conn->query('SELECT key FROM system.local')->asRowsResult();
        $this->assertSame(1, $rows->getRowCount());
        $this->assertSame(['key' => 'local'], $rows->fetch());
    }

    public function testSyntaxErrorRaisesException(): void {
        $conn = $this->connection;
        $this->expectException(SyntaxErrorException::class);
        $conn->query('SEELECT * FROM system.local');
    }
}
