<?php

declare(strict_types=1);

namespace Cassandra\Connection;

/**
 * @psalm-consistent-constructor
 */
interface NodeImplementation extends Node {
    /**
     * @param array{
     *  class?: string,
     *  host?: ?string,
     *  port?: int,
     *  username?: ?string,
     *  password?: ?string,
     * } & array<string, mixed> $options
     */
    public function __construct(array $options);

    public function write(string $binary): void;
}
