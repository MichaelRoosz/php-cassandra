<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

interface IoNode {
    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function __construct(NodeConfig $config);

    public function close(): void;

    public function getConfig(): NodeConfig;

    /**
     * Returns exactly $length bytes of data, or an empty string if not enough data is available.
     * If $waitForData is true, it will block until $length bytes are available.
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    public function read(int $length, bool $waitForData): string;

    /**
     * Returns up to $maxLength bytes of data, or an empty string if no data is available.
     * If $waitForData is true, it will block until at least one byte is available.
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    public function readAvailableData(int $expectedLength, int $maxLength, bool $waitForData): string;

    /**
     * Returns some bytes of data, or an empty string if no data is available.
     * $upperBoundaryLength marks an upper boundary for the amount of data that will be returned, but more or less data may be returned.
     * If $waitForData is true, it will block until at least one byte is available.
     *
     * @throws \Cassandra\Exception\NodeException
     */
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function write(string $data): void;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function writeRequest(Request $request): void;
}
