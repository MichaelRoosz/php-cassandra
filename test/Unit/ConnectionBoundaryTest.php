<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\IoNode;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeConnector;
use Cassandra\Connection\NodeSelector;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Exception\StatementException;
use Cassandra\Request\Request;
use ReflectionClass;
use stdClass;

final class ConnectionBoundaryTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{string}>
     */
    public static function statementListMethodProvider(): array {
        return [
            'try resolve' => ['tryResolveStatements'],
            'wait for any' => ['waitForAnyStatement'],
            'wait for all' => ['waitForStatements'],
        ];
    }

    public function testConnectionRejectsANonNodeConfiguration(): void {
        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_INVALID_NODE_CONFIG->value);

        (new ReflectionClass(Connection::class))->newInstanceArgs([[new stdClass()]]);
    }

    public function testConnectionWrapsAnInvalidNodeImplementation(): void {
        $config = new FailingIoNodeConfig();

        try {
            (new Connection([$config]))->connect();
            $this->fail('Expected the invalid implementation to be rejected');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_UNABLE_TO_CONNECT_ANY_NODE->value, $e->getCode());
            $previous = $e->getPrevious();
            $this->assertInstanceOf(NodeException::class, $previous);
            $this->assertSame(ExceptionCode::NODE_IMPLEMENTATION_FAILED->value, $previous->getCode());
            $this->assertNull($previous->getPrevious());
        }
    }

    public function testNodeConnectorPropagatesProjectExceptionFromSelector(): void {
        $failure = new NodeException('selector failed', ExceptionCode::NODE_IMPLEMENTATION_FAILED->value);
        $selector = new class($failure) implements NodeSelector {
            public function __construct(private NodeException $failure) {
            }

            public function order(array $nodes): array {
                throw $this->failure;
            }
        };

        try {
            (new NodeConnector([new FailingIoNodeConfig()], $selector))->open();
            $this->fail('Expected the selector exception to propagate');
        } catch (NodeException $e) {
            $this->assertSame($failure, $e);
        }
    }

    public function testNodeConnectorRejectsInvalidSelectorOutput(): void {
        $selector = $this->createMock(NodeSelector::class);
        $selector->method('order')->willReturn([new stdClass()]);

        $this->expectException(NodeException::class);
        $this->expectExceptionCode(ExceptionCode::NODE_IMPLEMENTATION_FAILED->value);

        (new NodeConnector([new FailingIoNodeConfig()], $selector))->open();
    }

    /**
     * @dataProvider statementListMethodProvider
     */
    public function testStatementListMethodsRejectNonStatementEntries(string $method): void {
        $connection = new Connection([]);

        $this->expectException(StatementException::class);
        $this->expectExceptionCode(ExceptionCode::STATEMENT_INVALID_LIST_ENTRY->value);

        $connection->$method([1]);
    }
}

final class FailingIoNodeConfig extends NodeConfig {
    public function getNodeClass(): string {
        return FailingIoNode::class;
    }
}

final class FailingIoNode implements IoNode {
    public function __construct(private NodeConfig $config) {
    }

    public function close(): void {
    }

    public function connect(): void {
        throw new NodeException('implementation failed', ExceptionCode::NODE_IMPLEMENTATION_FAILED->value);
    }

    public function getConfig(): NodeConfig {
        return $this->config;
    }

    public function getReceivedByteCount(): int {
        return 0;
    }

    public function read(int $length, ?float $readDeadline): string {
        return '';
    }

    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        return '';
    }

    public function write(string $data): void {
    }

    public function writeRequest(Request $request): void {
    }
}
