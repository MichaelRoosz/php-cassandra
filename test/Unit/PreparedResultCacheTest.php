<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\PreparedResultCache;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Prepare;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\StreamReader;

class PreparedResultCacheTest extends AbstractUnitTestCase {
    public function testAnEntryKeepsTheKeyspaceItWasStoredUnder(): void {
        // A request is addressed on its way to the wire and keeps what it was
        // given, so RequestExecutor::applyDefaultKeyspace() replaces the keyspace
        // of one that is sent again. Held by reference, an entry would follow the
        // request an application reuses across a Connection::setKeyspace() and
        // stop matching the key it is filed under — and the repreparation path
        // rebuilds its PREPARE out of exactly this request, so an UNPREPARED for
        // this statement id would prepare and execute the query somewhere else.
        $request = new Prepare('SELECT * FROM t');
        $request->applyDefaultKeyspace('ks1');

        $cache = new PreparedResultCache(10);
        $cache->store($request, self::preparedResult('pid1'), ProtocolVersion::V5);

        // The same object is sent again, on a connection that has since moved.
        $request->applyDefaultKeyspace('ks2');

        $lookup = new Prepare('SELECT * FROM t');
        $lookup->applyDefaultKeyspace('ks1');

        $entry = $cache->get($lookup);
        $this->assertNotNull($entry);

        $storedRequest = $entry->getRequest();
        $this->assertInstanceOf(Prepare::class, $storedRequest);
        $this->assertSame('ks1', $storedRequest->getOptions()->keyspace);
        $this->assertSame($lookup->getHash(), $storedRequest->getHash());
    }

    public function testTheTwoKeyspacesKeepSeparateEntries(): void {
        $cache = new PreparedResultCache(10);

        foreach (['ks1' => 'pid1', 'ks2' => 'pid2'] as $keyspace => $statementId) {
            $request = new Prepare('SELECT * FROM t');
            $request->applyDefaultKeyspace($keyspace);

            $cache->store($request, self::preparedResult($statementId), ProtocolVersion::V5);
        }

        foreach (['ks1' => 'pid1', 'ks2' => 'pid2'] as $keyspace => $statementId) {
            $lookup = new Prepare('SELECT * FROM t');
            $lookup->applyDefaultKeyspace($keyspace);

            $entry = $cache->get($lookup);
            $this->assertNotNull($entry, $keyspace);
            $this->assertSame($statementId, $entry->getPreparedData()->id, $keyspace);
        }
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    private static function preparedResult(string $statementId): PreparedResult {

        $header = new Header(
            version: ProtocolVersion::V5,
            flags: 0,
            stream: 1,
            opcode: Opcode::RESPONSE_RESULT,
            length: 0,
        );

        $body = pack('N', 4) // result kind: PREPARED
            . pack('n', strlen($statementId)) . $statementId
            . pack('n', 3) . 'mid' // result metadata id
            . pack('N', 0) // prepare metadata flags
            . pack('N', 0) // bind marker count
            . pack('N', 0) // partition key count
            . pack('N', 0) // rows metadata flags
            . pack('N', 0); // rows metadata column count

        return new PreparedResult($header, new StreamReader($body));
    }
}
