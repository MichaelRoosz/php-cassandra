<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

interface Node {
    public function close(): void;

    public function getConfig(): NodeConfig;

    public function read(int $length): string;

    public function readOnce(int $length): string;

    public function writeRequest(Request $request): void;
}
