<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Lz4Extension;
use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\NodeHealth;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\RequestCompressor;
use Cassandra\Connection\RoundRobinSelector;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\EventListener;
use Cassandra\Exception\ServerException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Startup;
use Cassandra\Response\Event;
use Cassandra\Test\Support\Faults;
use Cassandra\Test\Support\ScriptedNode;
use Cassandra\Test\Support\ScriptedNodeConfig;
use Cassandra\Type;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Value;
use Cassandra\WarningsListener;
use ReflectionMethod;
use ReflectionProperty;

/**
 * Small branches of the connection layer and the value layer that no larger
 * scenario passes through.
 */
final class SmallRemainingPathsTest extends AbstractUnitTestCase {
    protected function tearDown(): void {
        Faults::reset();
        ScriptedNode::reset();
        (new ReflectionProperty(Lz4Extension::class, 'available'))->setValue(null, null);
        parent::tearDown();
    }

    public function testAHandshakeThatAsksTheServerToRefuseWhenOverloaded(): void {
        ScriptedNode::reset();
        $options = new ConnectionOptions(heartbeatIntervalInSeconds: null, throwOnOverload: true, allowedProtocolVersions: [ProtocolVersion::V4]);
        $this->assertSame('1', $options->asStartupOptions()['THROW_ON_OVERLOAD']);

        $connection = new Connection([new ScriptedNodeConfig()], '', $options);
        $connection->connect();
        $startup = array_values(array_filter(ScriptedNode::$received, static fn ($r): bool => $r instanceof Startup));
        $this->assertNotSame([], $startup);
        $this->assertStringContainsString('THROW_ON_OVERLOAD', (string) $startup[0]);
    }

    public function testAServerRefusingTheHandshakeIsReportedAsItself(): void {
        ScriptedNode::reset(static fn (\Cassandra\Request\Request $r) => $r instanceof Startup
            ? [\Cassandra\Protocol\Opcode::RESPONSE_ERROR, pack('N', 0x000A) . ScriptedNode::string('protocol error')]
            : null);
        $connection = new Connection([new ScriptedNodeConfig()], '', new ConnectionOptions(heartbeatIntervalInSeconds: null));

        $this->expectException(ServerException::class);
        $connection->connect();
    }

    public function testATimeoutThatRoundsUpToTheNextSecond(): void {
        $split = new ReflectionMethod(NodeImplementation::class, 'splitTimeout');
        $this->assertSame([2, 0], $split->invoke(new ScriptedNode(new ScriptedNodeConfig()), 1.9999999));
    }

    public function testAVectorOfValuesWithSeveralEncodings(): void {
        $type = new VectorInfo(new SimpleTypeInfo(Type::UUID), 2);
        $vector = Value\Vector::fromBinary(str_repeat("\x11", 16) . str_repeat("\x22", 16), $type);

        $this->assertCount(2, (array) $vector->getValue());
    }

    public function testListenersCanBeRemoved(): void {
        ScriptedNode::reset();
        $connection = new Connection([new ScriptedNodeConfig()], '', new ConnectionOptions(heartbeatIntervalInSeconds: null));
        $events = new class() implements EventListener {
            public function onEvent(Event $event): void {
            }
        };
        $warnings = new class() implements WarningsListener {
            public function onWarnings(array $warnings, \Cassandra\Request\Request $request, \Cassandra\Response\Response $response): void {
            }
        };
        $connection->registerEventListener($events);
        $connection->registerWarningsListener($warnings);
        $connection->unregisterEventListener($events);
        $connection->unregisterWarningsListener($warnings);

        // Before connecting both report the initial protocol version.
        $fromV5 = $connection->getProtocolVersion()->value >= ProtocolVersion::V5->value;
        $this->assertSame($fromV5, $connection->supportsKeyspaceRequestOption());
        $this->assertSame($fromV5, $connection->supportsNowInSecondsRequestOption());
    }

    public function testNodesThatRecentlyFailedAreSetApart(): void {
        $health = new NodeHealth();
        $failed = new StreamNodeConfig(host: 'a');
        $healthy = new StreamNodeConfig(host: 'b');
        $health->recordFailure($failed);

        $this->assertSame(['available' => [$healthy], 'unavailable' => [$failed]], $health->partitionByAvailability([$failed, $healthy]));
    }

    public function testRoundRobinWithAtMostOneNode(): void {
        $node = new StreamNodeConfig();
        $this->assertSame([], (new RoundRobinSelector())->order([]));
        $this->assertSame([$node], (new RoundRobinSelector())->order([$node]));
    }

    public function testTheCompressorPassesThroughToItsNode(): void {
        $inner = new BytesNode('abcd');
        $compressor = new RequestCompressor($inner, 'lz4');

        $this->assertSame('ab', $compressor->read(2, null));
        $this->assertSame($inner->getReceivedByteCount(), $compressor->getReceivedByteCount());
        $this->assertInstanceOf(StreamNodeConfig::class, $compressor->getConfig());
        $compressor->close();
        $this->assertTrue($inner->closed);
    }

    public function testTheLz4ExtensionIsNotUsedWhenItIsMissing(): void {
        (new ReflectionProperty(Lz4Extension::class, 'available'))->setValue(null, null);
        Faults::injectWhere('extension_loaded', static fn (mixed $name): bool => $name === 'lz4', false);

        $this->assertFalse(Lz4Extension::isAvailable());
    }
}
