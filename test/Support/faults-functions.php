<?php

// Generated: pass-through overrides of PHP functions the library calls unqualified.
// See Cassandra\Test\Support\Faults. Loaded before any library code runs.

declare(strict_types=1);

namespace Cassandra {
    use Cassandra\Test\Support\Faults;

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }

    function json_encode(mixed ...$arguments): mixed {
        return Faults::call('json_encode', $arguments, static fn (mixed ...$a): mixed => \json_encode(...$a));
    }
}

namespace Cassandra\Value {
    use Cassandra\Test\Support\Faults;

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }

    function inet_ntop(mixed ...$arguments): mixed {
        return Faults::call('inet_ntop', $arguments, static fn (mixed ...$a): mixed => \inet_ntop(...$a));
    }

    function inet_pton(mixed ...$arguments): mixed {
        return Faults::call('inet_pton', $arguments, static fn (mixed ...$a): mixed => \inet_pton(...$a));
    }

    function json_encode(mixed ...$arguments): mixed {
        return Faults::call('json_encode', $arguments, static fn (mixed ...$a): mixed => \json_encode(...$a));
    }
}

namespace Cassandra\Response {
    use Cassandra\Test\Support\Faults;

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }

    function inet_ntop(mixed ...$arguments): mixed {
        return Faults::call('inet_ntop', $arguments, static fn (mixed ...$a): mixed => \inet_ntop(...$a));
    }
}

namespace Cassandra\Response\Result {
    use Cassandra\Test\Support\Faults;

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }
}

namespace Cassandra\Response\Error {
    use Cassandra\Test\Support\Faults;

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }
}

namespace Cassandra\Request {
    use Cassandra\Test\Support\Faults;

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }
}

namespace Cassandra\Compression {
    use Cassandra\Test\Support\Faults;

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }

    function extension_loaded(mixed ...$arguments): mixed {
        return Faults::call('extension_loaded', $arguments, static fn (mixed ...$a): mixed => \extension_loaded(...$a));
    }

    if (function_exists('\\lz4_compress')) {
        function lz4_compress(mixed ...$arguments): mixed {
            return Faults::call('lz4_compress', $arguments, static fn (mixed ...$a): mixed => \lz4_compress(...$a));
        }
    }

    if (function_exists('\\lz4_uncompress')) {
        function lz4_uncompress(mixed ...$arguments): mixed {
            return Faults::call('lz4_uncompress', $arguments, static fn (mixed ...$a): mixed => \lz4_uncompress(...$a));
        }
    }
}

namespace Cassandra\StringMath {
    use Cassandra\Test\Support\Faults;

    function extension_loaded(mixed ...$arguments): mixed {
        return Faults::call('extension_loaded', $arguments, static fn (mixed ...$a): mixed => \extension_loaded(...$a));
    }
}

namespace Cassandra\StringMath\DecimalCalculator {
    use Cassandra\Test\Support\Faults;

    function extension_loaded(mixed ...$arguments): mixed {
        return Faults::call('extension_loaded', $arguments, static fn (mixed ...$a): mixed => \extension_loaded(...$a));
    }

    function bcdiv(mixed ...$arguments): mixed {
        return Faults::call('bcdiv', $arguments, static fn (mixed ...$a): mixed => \bcdiv(...$a));
    }

    function bcmod(mixed ...$arguments): mixed {
        return Faults::call('bcmod', $arguments, static fn (mixed ...$a): mixed => \bcmod(...$a));
    }
}

namespace Cassandra\Connection {
    use Cassandra\Test\Support\Faults;

    function function_exists(mixed ...$arguments): mixed {
        return Faults::call('function_exists', $arguments, static fn (mixed ...$a): mixed => \function_exists(...$a));
    }

    function hrtime(mixed ...$arguments): mixed {
        return Faults::call('hrtime', $arguments, static fn (mixed ...$a): mixed => \hrtime(...$a));
    }

    function unpack(mixed ...$arguments): mixed {
        return Faults::call('unpack', $arguments, static fn (mixed ...$a): mixed => \unpack(...$a));
    }

    function extension_loaded(mixed ...$arguments): mixed {
        return Faults::call('extension_loaded', $arguments, static fn (mixed ...$a): mixed => \extension_loaded(...$a));
    }

    function fread(mixed ...$arguments): mixed {
        return Faults::call('fread', $arguments, static fn (mixed ...$a): mixed => \fread(...$a));
    }

    function fwrite(mixed ...$arguments): mixed {
        return Faults::call('fwrite', $arguments, static fn (mixed ...$a): mixed => \fwrite(...$a));
    }

    function feof(mixed ...$arguments): mixed {
        return Faults::call('feof', $arguments, static fn (mixed ...$a): mixed => \feof(...$a));
    }

    function fclose(mixed ...$arguments): mixed {
        return Faults::call('fclose', $arguments, static fn (mixed ...$a): mixed => \fclose(...$a));
    }

    function stream_get_meta_data(mixed ...$arguments): mixed {
        return Faults::call('stream_get_meta_data', $arguments, static fn (mixed ...$a): mixed => \stream_get_meta_data(...$a));
    }

    function stream_set_blocking(mixed ...$arguments): mixed {
        return Faults::call('stream_set_blocking', $arguments, static fn (mixed ...$a): mixed => \stream_set_blocking(...$a));
    }

    function stream_set_timeout(mixed ...$arguments): mixed {
        return Faults::call('stream_set_timeout', $arguments, static fn (mixed ...$a): mixed => \stream_set_timeout(...$a));
    }

    function stream_context_create(mixed ...$arguments): mixed {
        return Faults::call('stream_context_create', $arguments, static fn (mixed ...$a): mixed => \stream_context_create(...$a));
    }

    function socket_create(mixed ...$arguments): mixed {
        return Faults::call('socket_create', $arguments, static fn (mixed ...$a): mixed => \socket_create(...$a));
    }

    function socket_connect(mixed ...$arguments): mixed {
        return Faults::call('socket_connect', $arguments, static fn (mixed ...$a): mixed => \socket_connect(...$a));
    }

    function socket_read(mixed ...$arguments): mixed {
        return Faults::call('socket_read', $arguments, static fn (mixed ...$a): mixed => \socket_read(...$a));
    }

    function socket_write(mixed ...$arguments): mixed {
        return Faults::call('socket_write', $arguments, static fn (mixed ...$a): mixed => \socket_write(...$a));
    }

    function socket_set_option(mixed ...$arguments): mixed {
        return Faults::call('socket_set_option', $arguments, static fn (mixed ...$a): mixed => \socket_set_option(...$a));
    }

    function socket_get_option(mixed ...$arguments): mixed {
        return Faults::call('socket_get_option', $arguments, static fn (mixed ...$a): mixed => \socket_get_option(...$a));
    }

    function socket_set_nonblock(mixed ...$arguments): mixed {
        return Faults::call('socket_set_nonblock', $arguments, static fn (mixed ...$a): mixed => \socket_set_nonblock(...$a));
    }

    function socket_set_block(mixed ...$arguments): mixed {
        return Faults::call('socket_set_block', $arguments, static fn (mixed ...$a): mixed => \socket_set_block(...$a));
    }

    function socket_shutdown(mixed ...$arguments): mixed {
        return Faults::call('socket_shutdown', $arguments, static fn (mixed ...$a): mixed => \socket_shutdown(...$a));
    }

    function socket_close(mixed ...$arguments): mixed {
        return Faults::call('socket_close', $arguments, static fn (mixed ...$a): mixed => \socket_close(...$a));
    }

    function socket_last_error(mixed ...$arguments): mixed {
        return Faults::call('socket_last_error', $arguments, static fn (mixed ...$a): mixed => \socket_last_error(...$a));
    }

    function socket_clear_error(mixed ...$arguments): mixed {
        return Faults::call('socket_clear_error', $arguments, static fn (mixed ...$a): mixed => \socket_clear_error(...$a));
    }

    function socket_strerror(mixed ...$arguments): mixed {
        return Faults::call('socket_strerror', $arguments, static fn (mixed ...$a): mixed => \socket_strerror(...$a));
    }

    function socket_addrinfo_lookup(mixed ...$arguments): mixed {
        return Faults::call('socket_addrinfo_lookup', $arguments, static fn (mixed ...$a): mixed => \socket_addrinfo_lookup(...$a));
    }

    function socket_addrinfo_explain(mixed ...$arguments): mixed {
        return Faults::call('socket_addrinfo_explain', $arguments, static fn (mixed ...$a): mixed => \socket_addrinfo_explain(...$a));
    }

    function stream_select(array|null &$read, array|null &$write, array|null &$except, ?int $seconds, ?int $microseconds = null) {
        $arguments = [&$read, &$write, &$except, &$seconds, &$microseconds];

        return Faults::call('stream_select', $arguments, static fn (mixed &...$a): mixed => \stream_select(...$a));
    }

    function socket_select(array|null &$read, array|null &$write, array|null &$except, ?int $seconds, int $microseconds = 0) {
        $arguments = [&$read, &$write, &$except, &$seconds, &$microseconds];

        return Faults::call('socket_select', $arguments, static fn (mixed &...$a): mixed => \socket_select(...$a));
    }

    function stream_socket_client(string $address, &$error_code = null, &$error_message = null, ?float $timeout = null, int $flags = STREAM_CLIENT_CONNECT, $context = null) {
        $arguments = [&$address, &$error_code, &$error_message, &$timeout, &$flags, &$context];

        return Faults::call('stream_socket_client', $arguments, static fn (mixed &...$a): mixed => \stream_socket_client(...$a));
    }
}
