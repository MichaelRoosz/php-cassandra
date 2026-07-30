<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options as RequestOptions;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Execute;
use Cassandra\Request\Prepare;
use Cassandra\Request\Query;
use Cassandra\Request\Register;
use Cassandra\Request\Startup;
use Cassandra\Response\Result\CachedPreparedResult;
use Cassandra\Response\Result\Data\PreparedData;
use Cassandra\Response\Result\PrepareMetadata;
use Cassandra\Response\Result\RowsMetadata;
use Cassandra\Response\StreamReader;

/**
 * Filling the connection's keyspace into a request that names none of its own.
 *
 * From protocol v5 the keyspace travels with each request rather than being a
 * property of the node's session, so a request sent without one runs against
 * whatever the coordinator defaults to — which is nothing, and the statement is
 * refused. The connection therefore puts its own in on the way to the wire, at
 * the one point where the negotiated version is known.
 *
 * What is pinned here is the request end of that: which requests take a keyspace
 * at all, and that one the caller named is never overwritten. Where it is applied
 * from, and the version check that guards it, is
 * {@see \Cassandra\Connection\RequestExecutor}.
 */
final class DefaultKeyspaceTest extends AbstractUnitTestCase {
    public function testABatchTakesTheDefaultKeyspace(): void {
        $request = new Batch(BatchType::LOGGED, Consistency::ONE, new BatchOptions());

        $request->applyDefaultKeyspace('app');

        $this->assertSame('app', $request->getOptions()->keyspace);
    }

    public function testAnExecuteTakesTheDefaultKeyspace(): void {
        $request = $this->executeRequest(new ExecuteOptions());

        $request->applyDefaultKeyspace('app');

        $this->assertSame('app', $request->getOptions()->keyspace);
    }

    public function testAPrepareTakesTheDefaultKeyspaceAndItReachesTheCacheKey(): void {
        // The prepared-result cache is keyed on the keyspace as well as the
        // query, so the same CQL prepared on two keyspaces has to hash apart.
        // That only holds if the keyspace is in before the key is taken.
        $request = new Prepare('SELECT * FROM t', new PrepareOptions());
        $withoutKeyspace = $request->getHash();

        $request->applyDefaultKeyspace('app');

        $this->assertSame('app', $request->getOptions()->keyspace);
        $this->assertNotSame($withoutKeyspace, $request->getHash());
        $this->assertSame(
            (new Prepare('SELECT * FROM t', new PrepareOptions(keyspace: 'app')))->getHash(),
            $request->getHash(),
        );
    }

    public function testAQueryTakesTheDefaultKeyspace(): void {
        $request = new Query('SELECT * FROM t', [], Consistency::ONE, new QueryOptions());

        $this->assertNull($request->getOptions()->keyspace);

        $request->applyDefaultKeyspace('app');

        $this->assertSame('app', $request->getOptions()->keyspace);
    }

    public function testAQueryTheCallerAddressedItselfIsLeftAlone(): void {
        // A keyspace on the request is the caller pointing this one statement
        // somewhere else, which the connection's default must not undo.
        $request = new Query('SELECT * FROM t', [], Consistency::ONE, new QueryOptions(keyspace: 'explicit'));

        $request->applyDefaultKeyspace('app');

        $this->assertSame('explicit', $request->getOptions()->keyspace);
    }

    public function testBuildingARequestDoesNotOpenAConnection(): void {
        // The keyspace is no longer decided while the request is being built, so
        // nothing here has to know the negotiated version yet — and a node that
        // refuses connections would make an attempt to find out impossible to
        // miss.
        $connection = new Connection(
            [new SocketNodeConfig(host: '127.0.0.1', port: 1, connectTimeoutInSeconds: 1.0)],
            'app',
        );

        $batchRequest = $connection->createBatchRequest();

        $this->assertNull($batchRequest->getOptions()->keyspace);
        $this->assertFalse($connection->isConnected());
        $this->assertSame('app', $connection->getKeyspace());
    }

    public function testTheOtherRequestsHaveNoKeyspaceToTake(): void {
        // STARTUP, OPTIONS, REGISTER and AUTH_RESPONSE carry no options at all,
        // so the base class no-op is what they get. Nothing to assert but that
        // they survive being asked.
        $requests = [
            new Startup(),
            new RequestOptions(),
            new Register([]),
        ];

        foreach ($requests as $request) {
            $request->applyDefaultKeyspace('app');
        }

        $this->assertCount(3, $requests);
    }

    /**
     * An EXECUTE for a prepared statement with no bind markers, which is the
     * least a request needs to exist.
     *
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    private function executeRequest(ExecuteOptions $options): Execute {
        $prepared = new CachedPreparedResult(
            new Header(version: ProtocolVersion::V5, flags: 0, stream: 0, opcode: Opcode::RESPONSE_RESULT, length: 0),
            new StreamReader(''),
            new PreparedData(
                id: 'prepared-id',
                prepareMetadata: new PrepareMetadata(flags: 0, bindMarkersCount: 0, bindMarkers: [], pkCount: null, pkIndex: null),
                rowsMetadata: new RowsMetadata(flags: 0, columnsCount: 0, pagingState: null, metadataId: null, columns: []),
            ),
        );

        return new Execute($prepared, [], Consistency::ONE, $options);
    }
}
