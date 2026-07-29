<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Prepare;
use Cassandra\Response\Result\CachedPreparedResult;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\StreamReader;

/**
 * The prepared statements this connection already knows the id of, so that
 * preparing the same query again costs no round trip.
 *
 * A prepared statement id is only valid on the node that issued it, so the
 * cache belongs to the connection and is emptied with it, see
 * {@see self::clear()}.
 *
 * Full, it evicts the least recently used entries. PHP arrays keep their
 * insertion order, so that order is the whole mechanism: a hit moves its entry
 * to the end ({@see self::get()}), which leaves the coldest at the front for
 * {@see self::store()} to drop. Evicting by age instead would be the wrong way
 * round for a query cache — it would throw out the hot statement an application
 * prepared at startup and keep the one-off prepared a moment ago.
 *
 * A quarter is dropped at a time rather than one entry, so a full cache costs a
 * trim once every quarter of its size rather than on every prepare. Losing an
 * entry costs one round trip to prepare it again, never a wrong answer.
 *
 * Keeping the order costs an unset and an append per hit — some tens of
 * nanoseconds, against the PREPARE round trip that a hit is there to avoid, and
 * only on the prepare path: executing a prepared statement never comes here.
 */
final class PreparedResultCache {
    /**
     * @var array<string, CachedPreparedResult> $cache
     */
    private array $cache = [];

    private int $size;

    private int $sizeToTrim;

    public function __construct(int $size) {
        $this->size = max(0, $size);
        $this->sizeToTrim = (int) ceil((float) $this->size * 0.25);
    }

    public function clear(): void {

        $this->cache = [];
    }

    public function get(Prepare $request): ?CachedPreparedResult {

        $key = $request->getHash();

        $result = $this->cache[$key] ?? null;
        if ($result === null) {
            return null;
        }

        // Taken out and put back so that it moves to the end of the insertion
        // order, which is what makes the front of the cache the least recently
        // used part of it.
        unset($this->cache[$key]);
        $this->cache[$key] = $result;

        return $result;
    }

    public function invalidate(Prepare $request): void {

        unset($this->cache[$request->getHash()]);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    public function store(Prepare $request, PreparedResult $result, ProtocolVersion $version): void {

        if ($this->size < 1) {
            return;
        }

        $cachedResult = new CachedPreparedResult(
            new Header(version: $version, flags: 0, stream: 0, opcode: Opcode::RESPONSE_RESULT, length: 0),
            new StreamReader(''),
            $result->getPreparedData(),
        );

        $cachedResult->setRequest($request);

        $key = $request->getHash();

        // Assigning to a key that is already there would leave it where it is,
        // so it is taken out first: what has just been prepared is the most
        // recently used thing in here, wherever an earlier copy of it sat.
        unset($this->cache[$key]);

        // Dropped from the front, which after {@see self::get()} has done its
        // work is the least recently used quarter.
        if (count($this->cache) >= $this->size) {
            $this->cache = array_slice($this->cache, $this->sizeToTrim);
        }

        $this->cache[$key] = $cachedResult;
    }
}
