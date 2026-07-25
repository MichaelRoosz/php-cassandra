<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Frame {
    public function getBody(): string;

    public function getFlags(): int;

    public function getOpcode(): Opcode;

    public function getProtocolVersion(): ProtocolVersion;

    /**
     * The stream id this frame belongs to, or null for a request that has not
     * been assigned one yet. A response always has one, so
     * {@see \Cassandra\Response\Response::getStream()} narrows this to int.
     */
    public function getStream(): ?int;

    /**
     * @deprecated Use getProtocolVersion() instead.
     */
    public function getVersion(): int;
}
