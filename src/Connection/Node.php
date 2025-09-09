<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

interface Node {
    public function close(): void;

    public function getConfig(): NodeConfig;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function read(int $length): string;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function readOnce(int $length): string;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    public function writeRequest(Request $request): void;
}
