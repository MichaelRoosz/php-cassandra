<?php

declare(strict_types=1);

namespace Cassandra\Connection;

abstract class NodeConfig {
    /**
     * @param ?Authenticator $authenticator answers the node's authentication
     * exchange; takes precedence over $username and $password
     */
    public function __construct(
        public readonly string $host = 'localhost',
        public readonly int $port = 9042,
        public readonly string $username = '',
        #[\SensitiveParameter]
        public readonly string $password = '',
        public readonly ?Authenticator $authenticator = null,
    ) {

    }

    /**
     * Hides the password from var_dump() and print_r().
     *
     * @return array<mixed>
     */
    public function __debugInfo(): array {
        $properties = get_object_vars($this);
        $properties['password'] = $this->password === '' ? '' : '***';

        return $properties;
    }

    /**
     * @return ?Authenticator null when the node has nothing to authenticate with
     */
    public function getAuthenticator(): ?Authenticator {
        if ($this->authenticator !== null) {
            return $this->authenticator;
        }

        return $this->username === '' || $this->password === ''
            ? null
            : new PasswordAuthenticator($this->username, $this->password);
    }

    /**
     * @return class-string<IoNode>
     */
    abstract public function getNodeClass(): string;
}
