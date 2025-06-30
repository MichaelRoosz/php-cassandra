<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Opcode {
    public const REQUEST_AUTH_RESPONSE = 0x0F;
    public const REQUEST_BATCH = 0x0D;
    public const REQUEST_EXECUTE = 0x0A;
    public const REQUEST_OPTIONS = 0x05;
    public const REQUEST_PREPARE = 0x09;
    public const REQUEST_QUERY = 0x07;
    public const REQUEST_REGISTER = 0x0B;
    public const REQUEST_STARTUP = 0x01;

    public const RESPONSE_AUTH_CHALLENGE = 0x0E;
    public const RESPONSE_AUTH_SUCCESS = 0x10;
    public const RESPONSE_AUTHENTICATE = 0x03;
    public const RESPONSE_CREDENTIALS = 0x04;
    public const RESPONSE_ERROR = 0x00;
    public const RESPONSE_EVENT = 0x0C;
    public const RESPONSE_READY = 0x02;
    public const RESPONSE_RESULT = 0x08;
    public const RESPONSE_SUPPORTED = 0x06;
}
