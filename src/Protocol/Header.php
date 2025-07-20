<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

final class Header {
    public function __construct(
        public readonly int $version,
        public readonly int $flags,
        public readonly int $stream,
        public readonly Opcode $opcode,
        public readonly int $length,
    ) {
    }
}
