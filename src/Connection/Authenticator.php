<?php

declare(strict_types=1);

namespace Cassandra\Connection;

/**
 * Answers a node's SASL exchange: the first token, each challenge in turn, and
 * the node's final token.
 *
 * One instance serves every connection attempt of a node, so an implementation
 * that keeps per-exchange state must start over in {@see self::initialResponse()}.
 * Failures must be raised as a {@see \Cassandra\Exception\ConnectionException}.
 */
interface Authenticator {
    /**
     * Called when the node's token completes the exchange, so that a mechanism
     * with mutual authentication can verify the node. Throw to refuse the node.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function authenticationSucceeded(?string $token): void;

    /**
     * @return string the token for the next AUTH_RESPONSE
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function evaluateChallenge(?string $challenge): string;

    /**
     * @param string $serverAuthenticator the node's authenticator class, for
     * example `org.apache.cassandra.auth.PasswordAuthenticator`
     *
     * @return string the token for the first AUTH_RESPONSE
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function initialResponse(string $serverAuthenticator): string;
}
