<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Query;
use Cassandra\Response\Result;
use Cassandra\Response\StreamReader;
use Cassandra\Statement;

/**
 * Regression: the wait read before it looked, so an empty list or an answered
 * statement waited for a response that never came.
 */
final class WaitForAnyStatementTest extends AbstractUnitTestCase {
    public function testAnAnsweredStatementIsReturnedWithoutReading(): void {
        // Unreachable node: a read would fail, so returning proves none was attempted.
        $connection = new Connection([new StreamNodeConfig(host: '127.0.0.1', port: 1)]);
        $statement = new Statement($connection, 1, new Query('SELECT 1'));
        $body = pack('N', 1);
        $statement->setResponse(new Result(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body)));

        $this->assertSame($statement, $connection->waitForAnyStatement([$statement]));
    }

    public function testAnEmptyListIsRefused(): void {
        $connection = new Connection([new StreamNodeConfig(host: '127.0.0.1', port: 1)]);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_WAIT_FOR_ANY_STATEMENT_EMPTY_LIST->value);
        $connection->waitForAnyStatement([]);
    }
}
