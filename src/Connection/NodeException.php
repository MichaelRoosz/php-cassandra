<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception;

class NodeException extends Exception {
    // code range 30000 to 39999
    public const CODE_DECODE_COMPUTED_CRC32_FAILED = 30000;
    public const CODE_DECODE_FRAME_HEADER_FAILED = 30001;
    public const CODE_DECODE_PAYLOAD_CRC32_FAILED = 30002;
    public const CODE_DECOMPRESSOR_NOT_INITIALIZED = 30003;
    public const CODE_ENCODE_PAYLOAD_CRC32_FAILED = 30004;
    public const CODE_INVALID_HEADER_CRC24 = 30005;
    public const CODE_INVALID_PAYLOAD_CRC32 = 30006;
    public const CODE_INVALID_UNCOMPRESSED_LENGTH = 30007;
    public const CODE_PAYLOAD_EXCEEDS_MAX = 30008;
    public const CODE_UNSUPPORTED_COMPRESSION = 30009;
}
