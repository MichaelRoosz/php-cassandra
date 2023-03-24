<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Frame {
    public const FLAG_COMPRESSION = 0x01; // deprecated in v5
    public const FLAG_TRACING = 0x02;
    public const FLAG_CUSTOM_PAYLOAD = 0x04;
    public const FLAG_WARNING = 0x08;
    public const FLAG_USE_BETA = 0x10;

    public const OPCODE_ERROR = 0x00;
    public const OPCODE_STARTUP = 0x01;
    public const OPCODE_READY = 0x02;
    public const OPCODE_AUTHENTICATE = 0x03;
    public const OPCODE_CREDENTIALS = 0x04;
    public const OPCODE_OPTIONS = 0x05;
    public const OPCODE_SUPPORTED = 0x06;
    public const OPCODE_QUERY = 0x07;
    public const OPCODE_RESULT = 0x08;
    public const OPCODE_PREPARE = 0x09;
    public const OPCODE_EXECUTE = 0x0A;
    public const OPCODE_REGISTER = 0x0B;
    public const OPCODE_EVENT = 0x0C;
    public const OPCODE_BATCH = 0x0D;
    public const OPCODE_AUTH_CHALLENGE = 0x0E;
    public const OPCODE_AUTH_RESPONSE = 0x0F;
    public const OPCODE_AUTH_SUCCESS = 0x10;

    public function getVersion(): int;

    public function getFlags(): int;

    public function getStream(): int;

    public function getOpcode(): int;

    public function getBody(): string;
}
