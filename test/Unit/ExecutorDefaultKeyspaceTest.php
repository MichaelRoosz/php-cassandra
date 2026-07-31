<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\RequestExecutor;
use Cassandra\Connection\ResponseDispatcher;
use Cassandra\Connection\Session;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Prepare;
use Cassandra\Request\Query;
use ReflectionMethod;
use ReflectionProperty;

/**
 * The connection end of the default keyspace: which keyspace a request is
 * addressed with on its way to the wire, given the version the connection
 * settled on and the keyspace it is currently on.
 *
 * The request end — which requests take a keyspace at all, and that one the
 * caller named is never overwritten — is {@see DefaultKeyspaceTest}. What is
 * pinned here is that a request object sent more than once is re-addressed
 * every time, which is what {@see \Cassandra\Connection::setKeyspace()}
 * promises: only a keyspace the caller put on the request survives a change on
 * the connection.
 *
 * Driven through the executor directly rather than over a socket, because the
 * question is what a request carries at the moment it would be encoded, and
 * nothing about that needs a node to answer.
 */
final class ExecutorDefaultKeyspaceTest extends AbstractUnitTestCase {
    public function testAnAutoPreparedPrepareGivesUpTheDefaultKeyspaceOnAV4Send(): void {
        // The PREPARE an auto-prepared query needs is built out of that query's
        // options, which the executor has already addressed — so the keyspace
        // among them is this connection's, and the derived request has to know
        // that. Told otherwise it would count as a keyspace the caller named,
        // which is never taken back, and a later v4 send would refuse to encode
        // the request instead of addressing it the way v4 does.
        $connection = self::connectionOn(ProtocolVersion::V5, 'ks_a');

        $request = new Query('SELECT * FROM t WHERE id = ?', [1]);
        self::address($connection, $request);
        $this->assertSame('ks_a', $request->getOptions()->keyspace);

        $prepare = self::autoPrepareFor($connection, $request);
        $this->assertNotNull($prepare, 'a query with an untyped bind value is auto-prepared');
        $this->assertSame('ks_a', $prepare->getOptions()->keyspace);

        $prepare->clearDefaultKeyspace();

        $this->assertNull($prepare->getOptions()->keyspace, 'the keyspace came from the connection, so a v4 send takes it back');
    }

    public function testAnAutoPreparedPrepareKeepsAKeyspaceTheCallerNamed(): void {
        $connection = self::connectionOn(ProtocolVersion::V5, 'ks_a');

        $request = new Query('SELECT * FROM t WHERE id = ?', [1], Consistency::ONE, new QueryOptions(keyspace: 'explicit'));
        self::address($connection, $request);

        $prepare = self::autoPrepareFor($connection, $request);
        $this->assertNotNull($prepare);
        $this->assertSame('explicit', $prepare->getOptions()->keyspace);

        $prepare->clearDefaultKeyspace();

        $this->assertSame('explicit', $prepare->getOptions()->keyspace, 'the caller addressed the query, so its PREPARE stays addressed too');
    }

    public function testASecondSendTakesTheKeyspaceTheConnectionIsOnNow(): void {
        $connection = self::connectionOn(ProtocolVersion::V5, 'ks_a');

        $request = new Query('SELECT * FROM t');

        self::address($connection, $request);
        $this->assertSame('ks_a', $request->getOptions()->keyspace);

        self::moveTo($connection, 'ks_b');
        self::address($connection, $request);

        $this->assertSame('ks_b', $request->getOptions()->keyspace, 'a default is replaced by the current default');
    }

    public function testAV4SendTakesBackAKeyspaceAV5SendPutOn(): void {
        // The same request object on a second connection that settled lower, or
        // on this one after it renegotiated down. The keyspace option does not
        // exist before v5, so left on it would not merely be ignored — encoding
        // the request would fail outright.
        $connection = self::connectionOn(ProtocolVersion::V5, 'ks_a');

        $request = new Query('SELECT * FROM t');
        self::address($connection, $request);
        $this->assertSame('ks_a', $request->getOptions()->keyspace);

        self::settleOn($connection, ProtocolVersion::V4);
        self::address($connection, $request);

        $this->assertNull($request->getOptions()->keyspace);
    }

    public function testMovingOffTheKeyspaceLeavesTheCallersOwnAlone(): void {
        // They pointed this one statement somewhere, and no later change on the
        // connection takes that back — including a change to no keyspace at all.
        $connection = self::connectionOn(ProtocolVersion::V5, 'ks_a');

        $request = new Query('SELECT * FROM t', [], Consistency::ONE, new QueryOptions(keyspace: 'explicit'));

        self::address($connection, $request);
        self::moveTo($connection, '');
        self::address($connection, $request);

        $this->assertSame('explicit', $request->getOptions()->keyspace);
    }

    public function testMovingOffTheKeyspaceTakesTheDefaultBackOffTheRequest(): void {
        // From v5 an empty keyspace is a setting like any other — the absence of
        // one on each request — so a request that took this connection's default
        // on an earlier send must not go on running against it. Left there, the
        // statement would reach ks_a while getKeyspace() named none.
        $connection = self::connectionOn(ProtocolVersion::V5, 'ks_a');

        $request = new Query('SELECT * FROM t');
        self::address($connection, $request);
        $this->assertSame('ks_a', $request->getOptions()->keyspace);

        self::moveTo($connection, '');
        self::address($connection, $request);

        $this->assertNull($request->getOptions()->keyspace, 'the connection is on no keyspace, so neither is the request');
    }

    /**
     * Address a request exactly as the executor does on its way to the wire.
     *
     * @throws \ReflectionException
     */
    private static function address(Connection $connection, Query $request): void {
        $executor = (new ReflectionProperty(Session::class, 'executor'))->getValue(self::sessionOf($connection));
        self::assertInstanceOf(RequestExecutor::class, $executor);

        (new ReflectionMethod(RequestExecutor::class, 'applyDefaultKeyspace'))->invoke($executor, $request);
    }

    /**
     * The PREPARE the driver would send for $request, or null where the query
     * needs none.
     *
     * @throws \Cassandra\Exception\RequestException
     * @throws \ReflectionException
     */
    private static function autoPrepareFor(Connection $connection, Query $request): ?Prepare {
        $dispatcher = (new ReflectionProperty(Session::class, 'dispatcher'))->getValue(self::sessionOf($connection));
        self::assertInstanceOf(ResponseDispatcher::class, $dispatcher);

        return $dispatcher->getAutoPrepareRequestIfNeeded($request);
    }

    /**
     * A connection that has settled on $version and is on $keyspace, without
     * anything having been opened: both are what the handshake would have left
     * behind.
     *
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \ReflectionException
     */
    private static function connectionOn(ProtocolVersion $version, string $keyspace): Connection {
        $connection = new Connection([new SocketNodeConfig(host: '127.0.0.1')], $keyspace);

        self::settleOn($connection, $version);

        return $connection;
    }

    /**
     * @throws \ReflectionException
     */
    private static function moveTo(Connection $connection, string $keyspace): void {
        // Set directly rather than through setKeyspace(), which would send a USE
        // on a connection there is no node behind.
        (new ReflectionProperty(Session::class, 'keyspace'))->setValue(self::sessionOf($connection), $keyspace);
    }

    /**
     * @throws \ReflectionException
     */
    private static function settleOn(Connection $connection, ProtocolVersion $version): void {
        (new ReflectionProperty(Session::class, 'version'))->setValue(self::sessionOf($connection), $version);
    }
}
