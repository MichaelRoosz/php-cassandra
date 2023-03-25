<?php

declare(strict_types=1);

namespace Cassandra\Protocol;

interface Opcode {
    public final const REQUEST_AUTH_RESPONSE = 0x0F;
    public final const REQUEST_BATCH = 0x0D;
    public final const REQUEST_EXECUTE = 0x0A;
    public final const REQUEST_OPTIONS = 0x05;
    public final const REQUEST_PREPARE = 0x09;
    public final const REQUEST_QUERY = 0x07;
    public final const REQUEST_REGISTER = 0x0B;
    public final const REQUEST_STARTUP = 0x01;

    public final const RESPONSE_AUTH_CHALLENGE = 0x0E;
    public final const RESPONSE_AUTH_SUCCESS = 0x10;
    public final const RESPONSE_AUTHENTICATE = 0x03;
    public final const RESPONSE_CREDENTIALS = 0x04;
    public final const RESPONSE_ERROR = 0x00;
    public final const RESPONSE_EVENT = 0x0C;
    public final const RESPONSE_READY = 0x02;
    public final const RESPONSE_RESULT = 0x08;
    public final const RESPONSE_SUPPORTED = 0x06;
}
