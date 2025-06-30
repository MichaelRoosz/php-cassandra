<?php

declare(strict_types=1);

namespace Cassandra\Response;

enum ErrorType: int {
    case ALREADY_EXISTS = 0x2400;
    case AUTHENTICATION_ERROR = 0x0100;
    case CAS_WRITE_UNKNOWN = 0x1700;
    case CDC_WRITE_FAILURE = 0x1600;
    case CONFIG_ERROR = 0x2300;
    case FUNCTION_FAILURE = 0x1400;
    case INVALID = 0x2200;
    case IS_BOOTSTRAPPING = 0x1002;
    case OVERLOADED = 0x1001;
    case PROTOCOL_ERROR = 0x000A;
    case READ_FAILURE = 0x1300;
    case READ_TIMEOUT = 0x1200;
    case SERVER_ERROR = 0x0000;
    case SYNTAX_ERROR = 0x2000;
    case TRUNCATE_ERROR = 0x1003;
    case UNAUTHORIZED = 0x2100;
    case UNAVAILABLE_EXCEPTION = 0x1000;
    case UNPREPARED = 0x2500;
    case WRITE_FAILURE = 0x1500;
    case WRITE_TIMEOUT = 0x1100;
}
