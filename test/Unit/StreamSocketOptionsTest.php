<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use ReflectionProperty;
use Socket;

final class StreamSocketOptionsTest extends AbstractUnitTestCase {
    #[RequiresPhpExtension('sockets')]
    public function testStreamConnectionsDisableNagle(): void {
        // Regression: only the socket transport set TCP_NODELAY, so a stream
        // connection held small requests back waiting for an ACK.
        $server = stream_socket_server('tcp://127.0.0.1:0');
        $this->assertNotFalse($server);
        $name = stream_socket_get_name($server, false);
        $this->assertNotFalse($name);

        $node = new Stream(new StreamNodeConfig(host: '127.0.0.1', port: (int) substr((string) strrchr($name, ':'), 1)));
        $node->connect();

        /** @var resource $stream */
        $stream = (new ReflectionProperty(Stream::class, 'stream'))->getValue($node);
        $socket = socket_import_stream($stream);
        $this->assertInstanceOf(Socket::class, $socket);

        $this->assertSame(1, socket_get_option($socket, SOL_TCP, TCP_NODELAY));

        $node->close();
        fclose($server);
    }
}
