<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\Authenticator;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\PasswordAuthenticator;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;

/**
 * The SASL exchange against the fake server's "authenticate-challenge" mode,
 * which sends one challenge and accepts only the tokens asserted here.
 */
final class AuthenticatorTest extends AbstractUnitTestCase {
    /** @var ?resource */
    private $server = null;

    /** @var ?array<int, resource> */
    private ?array $serverPipes = null;

    protected function tearDown(): void {
        if ($this->serverPipes !== null) {
            foreach ($this->serverPipes as $pipe) {
                fclose($pipe);
            }
        }

        if ($this->server !== null) {
            proc_terminate($this->server);
            proc_close($this->server);
        }

        parent::tearDown();
    }

    public function testACredentialPairBecomesAPasswordAuthenticator(): void {
        $node = new StreamNodeConfig(username: 'user', password: 'secret');

        $this->assertSame("\0user\0secret", $node->getAuthenticator()?->initialResponse('any'));
    }

    public function testACustomAuthenticatorCompletesAMultiRoundExchange(): void {
        $authenticator = new class() implements Authenticator {
            /** @var list<?string> */
            public array $seen = [];

            public function authenticationSucceeded(?string $token): void {
                $this->seen[] = $token;
            }

            public function evaluateChallenge(?string $challenge): string {
                $this->seen[] = $challenge;

                return 'client-final';
            }

            public function initialResponse(string $serverAuthenticator): string {
                $this->seen[] = $serverAuthenticator;

                return 'client-first';
            }
        };

        $connection = $this->connection($authenticator);
        $connection->connect();

        $this->assertTrue($connection->isConnected());
        $this->assertSame(['ChallengingAuthenticator', 'server-challenge', 'server-final'], $authenticator->seen);
    }

    public function testACustomAuthenticatorTakesPrecedenceOverCredentials(): void {
        $node = new StreamNodeConfig(username: 'user', password: 'secret', authenticator: $custom = new PasswordAuthenticator('other', 'other'));

        $this->assertSame($custom, $node->getAuthenticator());
    }

    public function testNoCredentialsMeansNoAuthenticator(): void {
        $this->assertNull((new StreamNodeConfig())->getAuthenticator());
        $this->assertNull((new StreamNodeConfig(username: 'user'))->getAuthenticator());
    }

    public function testPasswordAuthenticationReportsAChallengeAsUnsupported(): void {
        $connection = $this->connection(new PasswordAuthenticator('user', 'secret'));

        try {
            $connection->connect();
            $this->fail('expected the challenge to be refused');
        } catch (ConnectionException $e) {
            $previous = $e;
            while ($previous !== null && $previous->getCode() !== ExceptionCode::CONNECTION_AUTH_CHALLENGE_UNSUPPORTED->value) {
                $previous = $previous->getPrevious();
            }

            $this->assertNotNull($previous, 'the unsupported challenge must stay discoverable: ' . $e->getMessage());
        }
    }

    public function testThePasswordStaysOutOfDebugOutput(): void {
        $dump = print_r(new PasswordAuthenticator('user', 'hunter2'), true);

        $this->assertStringNotContainsString('hunter2', $dump);
    }

    private function connection(Authenticator $authenticator): Connection {
        return new Connection(
            nodes: [new SocketNodeConfig(
                host: '127.0.0.1',
                port: $this->startServer(),
                authenticator: $authenticator,
            )],
            options: new ConnectionOptions(
                requestTimeoutInSeconds: 5.0,
                heartbeatIntervalInSeconds: null,
            ),
        );
    }

    private function startServer(): int {
        $process = proc_open(
            ['php', __DIR__ . '/Support/fake-cassandra-server.php', 'authenticate-challenge', '0'],
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
}
