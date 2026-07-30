<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Consistency;

final class Query extends Request {
    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    public function __construct(
        private string $query,
        private array $values = [],
        private Consistency $consistency = Consistency::ONE,
        private QueryOptions $options = new QueryOptions()
    ) {
        parent::__construct(Opcode::REQUEST_QUERY);

        if ($this->options->namesForValues === null && !array_is_list($values)) {
            $this->options = $this->options->withNamesForValues(true);
        }
    }

    /**
     * See {@see Request::applyDefaultKeyspace()}.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function applyDefaultKeyspace(string $keyspace): void {

        if ($this->options->keyspace !== null) {
            return;
        }

        $this->options = $this->options->withKeyspace($keyspace);
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        $body = pack('N', strlen($this->query)) . $this->query;
        $body .= self::encodeQueryParametersAsBinary($this->consistency, $this->values, $this->options, $this->version);

        return $body;
    }

    public function getConsistency(): Consistency {
        return $this->consistency;
    }

    public function getOptions(): QueryOptions {
        return $this->options;
    }

    public function getQuery(): string {
        return $this->query;
    }

    #[\Override]
    public function getRequestTimeout(): ?float {
        return $this->options->requestTimeoutInSeconds;
    }

    /**
     * @return array<mixed> $values
     */
    public function getValues(): array {
        return $this->values;
    }
}
