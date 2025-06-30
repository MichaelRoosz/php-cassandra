<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

enum Opcode: int {
    case REQUEST_AUTH_RESPONSE = 0x0F;
    case REQUEST_BATCH = 0x0D;
    case REQUEST_EXECUTE = 0x0A;
    case REQUEST_OPTIONS = 0x05;
    case REQUEST_PREPARE = 0x09;
    case REQUEST_QUERY = 0x07;
    case REQUEST_REGISTER = 0x0B;
    case REQUEST_STARTUP = 0x01;

    case RESPONSE_AUTH_CHALLENGE = 0x0E;
    case RESPONSE_AUTH_SUCCESS = 0x10;
    case RESPONSE_AUTHENTICATE = 0x03;
    case RESPONSE_CREDENTIALS = 0x04;
    case RESPONSE_ERROR = 0x00;
    case RESPONSE_EVENT = 0x0C;
    case RESPONSE_READY = 0x02;
    case RESPONSE_RESULT = 0x08;
    case RESPONSE_SUPPORTED = 0x06;
}
