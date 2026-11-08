<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;

/**
 * Single-round SASL PLAIN, as spoken by Cassandra's and ScyllaDB's
 * PasswordAuthenticator. Used whenever a node has a username and a password.
 */
final class PasswordAuthenticator implements Authenticator {
    private string $serverAuthenticator = '';

    public function __construct(
        private readonly string $username,
        #[\SensitiveParameter]
        private readonly string $password,
    ) {
    }

    /**
     * Hides the password from var_dump() and print_r().
     *
     * @return array{username: string, password: string}
     */
    public function __debugInfo(): array {
        return ['username' => $this->username, 'password' => '***'];
    }

    #[\Override]
    public function authenticationSucceeded(?string $token): void {
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     */
    #[\Override]
    public function evaluateChallenge(?string $challenge): string {
        // Reported apart from a rejected password: the node never judged these
        // credentials, so nobody should go looking for a typo in them.
        throw new ConnectionException(
            'The node answered with an authentication challenge, which password authentication cannot answer. Configure an Authenticator for this node.',
            ExceptionCode::CONNECTION_AUTH_CHALLENGE_UNSUPPORTED->value,
            [
                'operation' => 'connect/authenticate',
                'username' => $this->username,
                'authenticator' => $this->serverAuthenticator,
            ]
        );
    }

    #[\Override]
    public function initialResponse(string $serverAuthenticator): string {
        $this->serverAuthenticator = $serverAuthenticator;

        return "\0" . $this->username . "\0" . $this->password;
    }
}
