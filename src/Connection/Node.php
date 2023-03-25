<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

interface Node {
    public function close(): void;

    /**
     * @return array{
     *  class: string,
     *  host: ?string,
     *  port: int,
     *  username: ?string,
     *  password: ?string,
     * } & array<string, mixed>
     */
    public function getOptions(): array;

    public function read(int $length): string;

    public function readOnce(int $length): string;

    public function writeRequest(Request $request): void;
}
