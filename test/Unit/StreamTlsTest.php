<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\Stream;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\StreamException;
use Cassandra\Response\Result;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;

/**
 * TLS against the fake server listening on ssl://, with a certificate made here.
 */
#[RequiresPhpExtension('openssl')]
final class StreamTlsTest extends AbstractUnitTestCase {
    private string $certificateFile = '';

    /** @var ?resource */
    private $server = null;

    /** @var ?array<int, resource> */
    private ?array $serverPipes = null;

    protected function setUp(): void {
        parent::setUp();

        $this->certificateFile = (string) tempnam(sys_get_temp_dir(), 'php-cassandra-tls');
        file_put_contents($this->certificateFile, self::selfSignedPem());
    }

    protected function tearDown(): void {
        foreach ($this->serverPipes ?? [] as $pipe) {
            fclose($pipe);
        }

        if ($this->server !== null) {
            proc_terminate($this->server);
            proc_close($this->server);
        }

        unlink($this->certificateFile);

        parent::tearDown();
    }

    public function testACertificateFromAnUntrustedAuthorityIsRefused(): void {
        $connection = new Connection(
            [new StreamNodeConfig(
                host: '127.0.0.1',
                port: $this->startServer('idle'),
                sslOptions: ['verify_peer' => true, 'peer_name' => 'localhost', 'capture_peer_cert' => false],
            )],
            options: new ConnectionOptions(heartbeatIntervalInSeconds: null),
        );

        $this->expectException(ConnectionException::class);
        $connection->connect();
    }

    public function testAFailedConnectKeepsTheKeyPassphraseOutOfTheException(): void {
        $this->assertStringNotContainsString('hunter2', print_r(new StreamNodeConfig(password: 'hunter2', sslOptions: ['passphrase' => 'hunter2']), true));

        $node = new Stream(new StreamNodeConfig(
            host: '127.0.0.1',
            port: 1,
            connectTimeoutInSeconds: 0.5,
            sslOptions: ['passphrase' => 'hunter2', 'cafile' => '/ca.pem'],
        ));

        try {
            $node->connect();
            $this->fail('expected the connect to fail');
        } catch (StreamException $e) {
            $this->assertSame(['cafile' => '/ca.pem'], $e->getContext()['ssl_options'] ?? null);
        }
    }

    public function testAQueryRunsOverVerifiedTls(): void {
        $connection = $this->verifiedConnection('idle');

        $connection->connect();

        $this->assertInstanceOf(Result::class, $connection->query('SELECT 1'));
    }

    public function testAResponseSpanningManyTlsRecordsArrivesWhole(): void {
        // Decrypted bytes wait inside the stream where select() cannot see them,
        // so a read that selects before it reads would stall on this response.
        $connection = $this->verifiedConnection('trickle-result', 0.5);

        $connection->connect();

        $this->assertInstanceOf(Result::class, $connection->query('SELECT 1'));
    }

    private static function selfSignedPem(): string {
        $key = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
        $request = openssl_csr_new(['commonName' => 'localhost'], $key);
        if (!$key instanceof \OpenSSLAsymmetricKey || !$request instanceof \OpenSSLCertificateSigningRequest) {
            self::fail('could not create a certificate request');
        }

        $certificate = openssl_csr_sign($request, null, $key, 1);
        if (!$certificate instanceof \OpenSSLCertificate) {
            self::fail('could not sign the certificate');
        }

        $certificatePem = '';
        $keyPem = '';
        openssl_x509_export($certificate, $certificatePem);
        openssl_pkey_export($key, $keyPem);

        if (!is_string($certificatePem) || !is_string($keyPem)) {
            self::fail('could not export the certificate');
        }

        return $certificatePem . $keyPem;
    }

    private function startServer(string $mode, float $delaySeconds = 0.0): int {
        $process = proc_open(
            ['php', __DIR__ . '/Support/fake-cassandra-server.php', $mode, (string) $delaySeconds, $this->certificateFile],
            [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
            $pipes
        );
        if (!is_resource($process)) {
            $this->fail('could not start the fake Cassandra server');
        }

        $this->server = $process;
        $this->serverPipes = $pipes;

        $line = fgets($pipes[1]);
        if ($line === false || !str_starts_with($line, 'ready ')) {
            $this->fail('the fake Cassandra server did not become ready');
        }

        return (int) substr(trim($line), strlen('ready '));
    }

    private function verifiedConnection(string $mode, float $delaySeconds = 0.0): Connection {
        return new Connection(
            [new StreamNodeConfig(
                host: '127.0.0.1',
                port: $this->startServer($mode, $delaySeconds),
                sslOptions: [
                    'verify_peer' => true,
                    'peer_name' => 'localhost',
                    'cafile' => $this->certificateFile,
                ],
            )],
            options: new ConnectionOptions(requestTimeoutInSeconds: 10.0, heartbeatIntervalInSeconds: null),
        );
    }
}
