<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\PrepareOptions;

final class Prepare extends Request {
    public function __construct(
        protected string $query,
        protected PrepareOptions $options = new PrepareOptions()
    ) {
        parent::__construct(Opcode::REQUEST_PREPARE);
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
                $flags |= PrepareFlag::WITH_KEYSPACE->value;
                $optional .= pack('n', strlen($this->options->keyspace)) . $this->options->keyspace;
            } else {
                throw new Exception('Option "keyspace" not supported by server', 0, [
                    'required_protocol' => 'v5',
                    'actual_protocol' => $this->version,
                    'keyspace' => $this->options->keyspace,
                ]);
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
