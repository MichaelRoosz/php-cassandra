<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Exception\RequestException;

final class Prepare extends Request {
    public function __construct(
        private string $query,
        private PrepareOptions $options = new PrepareOptions()
    ) {
        parent::__construct(Opcode::REQUEST_PREPARE);
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        $flags = 0;
        $optional = '';

        $body = pack('N', strlen($this->query)) . $this->query;

        if ($this->options->keyspace !== null) {
            if ($this->version->value >= ProtocolVersion::V5->value) {
                $flags |= PrepareFlag::WITH_KEYSPACE;
                $optional .= pack('n', strlen($this->options->keyspace)) . $this->options->keyspace;
            } else {
                throw new RequestException(
                    message: 'Server protocol version does not support request option "keyspace"',
                    code: ExceptionCode::REQUEST_UNSUPPORTED_OPTION_KEYSPACE->value,
                    context: [
                        'request' => 'PREPARE',
                        'option' => 'keyspace',
                        'required_protocol_version' => ProtocolVersion::V5->toOptionFormat(),
                        'actual_protocol_version' => $this->version->toOptionFormat(),
                        'keyspace' => $this->options->keyspace,
                    ]
                );
            }
        }

        if ($this->version->value < ProtocolVersion::V5->value) {
            return $body;
        } else {
            return $body . pack('N', $flags) . $optional;
        }
    }

    public function getHash(): string {
        return hash('sha256', $this->query . ($this->options->keyspace ?? ''));
    }

    public function getOptions(): PrepareOptions {
        return $this->options;
    }

    public function getQuery(): string {
        return $this->query;
    }
}
