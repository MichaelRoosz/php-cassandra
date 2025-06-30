<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\PrepareOptions;

final class Prepare extends Request {
    final public const FLAG_WITH_KEYSPACE = 0x01;

    protected int $opcode = Opcode::REQUEST_PREPARE;

    protected PrepareOptions $options;

    protected string $query;

    public function __construct(string $query, PrepareOptions $options = new PrepareOptions()) {
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

        if ($this->options->keyspace !== null) {
            if ($this->version >= 5) {
                $flags |= Query::FLAG_WITH_KEYSPACE;
                $optional .= pack('n', strlen($this->options->keyspace)) . $this->options->keyspace;
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

    public function getOptions(): PrepareOptions {
        return $this->options;
    }

    public function getQuery(): string {
        return $this->query;
    }
}
