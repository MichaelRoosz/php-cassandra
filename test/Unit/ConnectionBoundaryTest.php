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
use Cassandra\Request\Request;
use ReflectionClass;
use stdClass;

final class ConnectionBoundaryTest extends AbstractUnitTestCase {
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
            $this->assertInstanceOf(\Error::class, $previous->getPrevious());
        }
    }

    public function testNodeConnectorRejectsInvalidSelectorOutput(): void {
        $selector = $this->createMock(NodeSelector::class);
        $selector->method('order')->willReturn([new stdClass()]);

        $this->assertSelectorFailureIsWrapped($selector, null);
    }

    public function testNodeConnectorWrapsSelectorFailure(): void {
        $selector = new class implements NodeSelector {
            public function order(array $nodes): array {
                throw new \Error('selector failed');
            }
        };

        $this->assertSelectorFailureIsWrapped($selector, \Error::class);
    }

    /**
     * @param ?class-string<\Throwable> $expectedUnderlyingFailure
     */
    private function assertSelectorFailureIsWrapped(NodeSelector $selector, ?string $expectedUnderlyingFailure): void {
        $connector = new NodeConnector([new FailingIoNodeConfig()], $selector);

        try {
            $connector->open();
            $this->fail('Expected the selector failure to be wrapped');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_UNABLE_TO_CONNECT_ANY_NODE->value, $e->getCode());
            $previous = $e->getPrevious();
            $this->assertInstanceOf(NodeException::class, $previous);
            $this->assertSame(ExceptionCode::NODE_IMPLEMENTATION_FAILED->value, $previous->getCode());

            if ($expectedUnderlyingFailure === null) {
                $this->assertNull($previous->getPrevious());
            } else {
                $this->assertInstanceOf($expectedUnderlyingFailure, $previous->getPrevious());
            }
        }
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
        throw new \Error('implementation failed');
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
