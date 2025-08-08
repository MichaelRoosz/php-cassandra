<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception;

class NodeException extends Exception {
    // Frame/codec-level errors (1101-1199)
    public const CODE_UNSUPPORTED_COMPRESSION = 1101;
    public const CODE_DECODE_FRAME_HEADER_FAILED = 1102;
    public const CODE_INVALID_HEADER_CRC24 = 1103;
    public const CODE_DECODE_PAYLOAD_CRC32_FAILED = 1104;
    public const CODE_DECODE_COMPUTED_CRC32_FAILED = 1105;
    public const CODE_INVALID_PAYLOAD_CRC32 = 1106;
    public const CODE_DECOMPRESSOR_NOT_INITIALIZED = 1107;
    public const CODE_INVALID_UNCOMPRESSED_LENGTH = 1108;
    public const CODE_PAYLOAD_EXCEEDS_MAX = 1109;
    public const CODE_ENCODE_PAYLOAD_CRC32_FAILED = 1110;
}
