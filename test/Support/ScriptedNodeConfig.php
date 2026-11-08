<?php

declare(strict_types=1);

namespace Cassandra\Test\Support;

use Cassandra\Connection\NodeConfig;

/**
 * Points a connection at a {@see ScriptedNode}, or at any other class, which is
 * how a test configures an implementation that is not an IoNode at all.
 */
final class ScriptedNodeConfig extends NodeConfig {
    /**
     * @param class-string $nodeClass
     */
    public function __construct(
        private readonly string $nodeClass = ScriptedNode::class,
        string $username = '',
        string $password = '',
        ?\Cassandra\Connection\Authenticator $authenticator = null,
    ) {
        parent::__construct(host: 'scripted', port: 1, username: $username, password: $password, authenticator: $authenticator);
    }

    #[\Override]
    public function getNodeClass(): string {
        /** @var class-string<\Cassandra\Connection\IoNode> */
        return $this->nodeClass;
    }
}
