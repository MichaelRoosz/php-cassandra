<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

final class Prepare extends Request {
    final public const FLAG_WITH_KEYSPACE = 0x01;

    protected int $opcode = Opcode::REQUEST_PREPARE;

    /**
     * @var array{
     *  keyspace?: string,
     * } $options
     */
    protected array $options;

    protected string $query;

    /**
     * @param array{
     *  keyspace?: string,
     * } $options
     */
    public function __construct(string $query, array $options = []) {
        $this->query = $query;
        $this->options = $options;
    }

    /**
     * @throws \Cassandra\Request\Exception
     */
    #[\Override]
    public function getBody(): string {
        $flags = 0;
        $optional = '';

        $body = pack('N', strlen($this->query)) . $this->query;

        if (isset($this->options['keyspace'])) {
            if ($this->version >= 5) {
                $flags |= Query::FLAG_WITH_KEYSPACE;
                $optional .= pack('n', strlen($this->options['keyspace'])) . $this->options['keyspace'];
            } else {
                throw new Exception('Option "keyspace" not supported by server');
            }
        }

        if ($this->version < 5) {
            return $body;
        } else {
            return $body . pack('N', $flags) . $optional;
        }
    }

    /**
     * @return array{
     *  keyspace?: string,
     * } $options
     */
    public function getOptions(): array {
        return $this->options;
    }

    public function getQuery(): string {
        return $this->query;
    }
}
