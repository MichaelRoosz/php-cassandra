<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;

/**
 * Configuration a caller cannot have meant is refused where it was given.
 *
 * Each of these used to be accepted and then quietly turned into something
 * else — an empty node list into "unable to connect to any node", a negative
 * cache size into a disabled cache, a negative orphan limit into a connection
 * that replaces itself on the first request it gives up on.
 */
final class ConfigurationValidationTest extends AbstractUnitTestCase {
    public function testANegativeMaxOrphanedStreamsIsRefused(): void {

        try {
            new ConnectionOptions(maxOrphanedStreams: -1);
            $this->fail('expected the negative orphan limit to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_INVALID_OPTIONS->value, $e->getCode());
            $this->assertSame(-1, $e->getContext()['max_orphaned_streams'] ?? null);
        }
    }

    public function testANegativePreparedResultCacheSizeIsRefused(): void {

        try {
            new ConnectionOptions(preparedResultCacheSize: -1);
            $this->fail('expected the negative cache size to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_INVALID_OPTIONS->value, $e->getCode());
            $this->assertSame(-1, $e->getContext()['prepared_result_cache_size'] ?? null);
        }
    }

    public function testAnEmptyNodeListIsRefusedWhereItIsGiven(): void {
        // Not left to connect(): the node list is fixed for the life of the
        // object, so nothing the caller does later could make this work, and
        // reported from connect() it reads like an unreachable cluster.
        try {
            new Connection([]);
            $this->fail('expected the empty node list to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_NO_NODES_CONFIGURED->value, $e->getCode());
        }
    }

    public function testAnEntryThatIsNotANodeConfigIsStillRefusedSeparately(): void {
        // The empty-list check must not have swallowed the type check.
        try {
            /** @phpstan-ignore argument.type */
            new Connection(['not-a-node-config']);
            $this->fail('expected the invalid node configuration to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_INVALID_NODE_CONFIG->value, $e->getCode());
        }
    }

    public function testAValidSingleNodeListIsAccepted(): void {

        $connection = new Connection([new StreamNodeConfig(host: '127.0.0.1', port: 9042)]);

        $this->assertFalse($connection->isConnected());
    }

    public function testAZeroMaxOrphanedStreamsIsAccepted(): void {
        // A legitimate setting: replace the connection as soon as one stream id
        // is held back.
        $options = new ConnectionOptions(maxOrphanedStreams: 0);

        $this->assertSame(0, $options->maxOrphanedStreams);
    }

    public function testAZeroPreparedResultCacheSizeIsAccepted(): void {
        // A legitimate setting: prepare every statement afresh.
        $options = new ConnectionOptions(preparedResultCacheSize: 0);

        $this->assertSame(0, $options->preparedResultCacheSize);
    }
}
