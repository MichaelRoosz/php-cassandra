<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;

class Prepare extends Request {
    final public const FLAG_WITH_KEYSPACE = 0x01;

    protected string $cql;

    protected int $opcode = Opcode::REQUEST_PREPARE;

    /**
     * @var array{
     *  keyspace?: string,
     * } $options
     */
    protected array $options;

    /**
     * @param array{
     *  keyspace?: string,
     * } $options
     */
    public function __construct(string $cql, array $options = []) {
        $this->cql = $cql;
        $this->options = $options;
    }

    /**
     * @throws \Cassandra\Request\Exception
     */
    public function getBody(): string {
        $flags = 0;
        $optional = '';

        $body = pack('N', strlen($this->cql)) . $this->cql;

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
}
