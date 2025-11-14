<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Frame {
    public function getBody(): string;

    public function getFlags(): int;

    public function getOpcode(): Opcode;

    public function getStream(): int;

    /**
     * @deprecated Use getProtocolVersion() instead.
     */
    public function getVersion(): int;

    public function getProtocolVersion(): ProtocolVersion;
}
