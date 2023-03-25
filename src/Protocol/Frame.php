<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Frame {
    public final const FLAG_COMPRESSION = 0x01; // deprecated in v5
    public final const FLAG_CUSTOM_PAYLOAD = 0x04;
    public final const FLAG_TRACING = 0x02;
    public final const FLAG_USE_BETA = 0x10;
    public final const FLAG_WARNING = 0x08;

    public final const OPCODE_AUTH_CHALLENGE = 0x0E;
    public final const OPCODE_AUTH_RESPONSE = 0x0F;
    public final const OPCODE_AUTH_SUCCESS = 0x10;
    public final const OPCODE_AUTHENTICATE = 0x03;
    public final const OPCODE_BATCH = 0x0D;
    public final const OPCODE_CREDENTIALS = 0x04;
    public final const OPCODE_ERROR = 0x00;
    public final const OPCODE_EVENT = 0x0C;
    public final const OPCODE_EXECUTE = 0x0A;
    public final const OPCODE_OPTIONS = 0x05;
    public final const OPCODE_PREPARE = 0x09;
    public final const OPCODE_QUERY = 0x07;
    public final const OPCODE_READY = 0x02;
    public final const OPCODE_REGISTER = 0x0B;
    public final const OPCODE_RESULT = 0x08;
    public final const OPCODE_STARTUP = 0x01;
    public final const OPCODE_SUPPORTED = 0x06;

    public function getBody(): string;

    public function getFlags(): int;

    public function getOpcode(): int;

    public function getStream(): int;

    public function getVersion(): int;
}
