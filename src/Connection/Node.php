<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

interface Node {
    /**
     * A read deadline that has already passed, i.e. "take what is there and
     * come straight back".
     */
    public const DO_NOT_WAIT = 0.0;

    public function close(): void;

    public function getConfig(): NodeConfig;

    /**
     * Returns exactly $length bytes of data, or an empty string if not enough data is available.
     *
     * @param ?float $readDeadline how long this read may block, as an absolute
     * microtime (the scale {@see microtime()} returns for `true`):
     *   null   block for as long as the transport's own stall window allows
     *   future block until then, or until the stall window is over, whichever
     *          comes first
     *   past   do not block at all ({@see self::DO_NOT_WAIT})
     *
     * A deadline is a bound, never a guarantee of having waited: reaching it is
     * not a transport failure — the caller asked for no more time, not for the
     * connection to prove itself — so it returns empty-handed rather than
     * raising. Only the stall window elapsing means the transport itself has
     * gone quiet for too long, and that is what raises.
     *
     * @throws \Cassandra\Exception\NodeException
     */
    public function read(int $length, ?float $readDeadline): string;

    /**
     * Returns up to $maxLength bytes of data, or an empty string if no data is available.
     *
     * @param ?float $readDeadline see {@see self::read()}
     *
     * @throws \Cassandra\Exception\NodeException
     */
    public function readAvailableData(int $expectedLength, int $maxLength, ?float $readDeadline): string;

    /**
     * Returns some bytes of data, or an empty string if no data is available.
     * $upperBoundaryLength marks an upper boundary for the amount of data that will be returned, but more or less data may be returned.
     *
     * @param ?float $readDeadline see {@see self::read()}
     *
     * @throws \Cassandra\Exception\NodeException
     */
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function write(string $data): void;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function writeRequest(Request $request): void;
}
