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

        return $this->cache[$request->getHash()] ?? null;
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

        if (count($this->cache) >= $this->size) {
            $this->cache = array_slice($this->cache, $this->sizeToTrim);
        }

        $this->cache[$request->getHash()] = $cachedResult;
    }
}
