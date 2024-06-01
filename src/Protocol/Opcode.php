<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Opcode {
    final public const REQUEST_AUTH_RESPONSE = 0x0F;
    final public const REQUEST_BATCH = 0x0D;
    final public const REQUEST_EXECUTE = 0x0A;
    final public const REQUEST_OPTIONS = 0x05;
    final public const REQUEST_PREPARE = 0x09;
    final public const REQUEST_QUERY = 0x07;
    final public const REQUEST_REGISTER = 0x0B;
    final public const REQUEST_STARTUP = 0x01;

    final public const RESPONSE_AUTH_CHALLENGE = 0x0E;
    final public const RESPONSE_AUTH_SUCCESS = 0x10;
    final public const RESPONSE_AUTHENTICATE = 0x03;
    final public const RESPONSE_CREDENTIALS = 0x04;
    final public const RESPONSE_ERROR = 0x00;
    final public const RESPONSE_EVENT = 0x0C;
    final public const RESPONSE_READY = 0x02;
    final public const RESPONSE_RESULT = 0x08;
    final public const RESPONSE_SUPPORTED = 0x06;
}
