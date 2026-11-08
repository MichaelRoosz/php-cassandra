<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\StreamException;

final class StreamTlsOptionsTest extends AbstractUnitTestCase {
    /** @var ?resource */
    private $server = null;

    protected function tearDown(): void {
        if (is_resource($this->server)) {
            fclose($this->server);
        }
        parent::tearDown();
    }

    public function testAFailedConnectKeepsTheKeyPassphraseOutOfTheException(): void {
        // Regression: the whole sslOptions array, passphrase included, went into the exception context.
        $node = new Stream(new StreamNodeConfig(host: 'tls://127.0.0.1', port: 1, connectTimeoutInSeconds: 1, sslOptions: ['passphrase' => 'secret-passphrase']));

        try {
            @$node->connect();
            $this->fail('expected the connect to fail');
        } catch (StreamException $e) {
            $this->assertStringNotContainsString('secret-passphrase', serialize($e->getContext()));
        }
    }

    public function testTheDefaultOptionsKeepAPlainHostInPlaintext(): void {
        $node = new Stream(new StreamNodeConfig(host: '127.0.0.1', port: $this->plaintextListener()));
        $node->connect();
        $this->addToAssertionCount(1);
    }

    public function testTlsOptionsOnAHostWithoutASchemeConnectOverTls(): void {
        // Regression: they were ignored, and the connection went out in plaintext.
        $port = $this->plaintextListener();
        $node = new Stream(new StreamNodeConfig(host: '127.0.0.1', port: $port, connectTimeoutInSeconds: 0.5, sslOptions: ['verify_peer' => false, 'verify_peer_name' => false]));

        // The listener never answers the TLS handshake.
        $this->expectException(StreamException::class);
        @$node->connect();
    }

    private function plaintextListener(): int {
        $server = stream_socket_server('tcp://127.0.0.1:0');
        $this->assertNotFalse($server);
        $this->server = $server;

        return (int) substr((string) strrchr((string) stream_socket_get_name($server, false), ':'), 1);
    }
}
