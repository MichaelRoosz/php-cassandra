<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;

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

        if ($this->options->namesForValues === null) {
            if (!array_is_list($values)) {
                $this->options = $this->options->withNamesForValues(true);
            } else {
                $ambiguousNames = self::ambiguousNumericBindMarkerNames($this->query, $values);
                if ($ambiguousNames !== []) {
                    throw new RequestException(
                        'Numeric named bind markers are ambiguous with positional PHP array keys; set namesForValues explicitly',
                        ExceptionCode::REQUEST_VALUES_AMBIGUOUS_NUMERIC_NAMES->value,
                        [
                            'stage' => 'values_encoding_mode',
                            'bind_markers' => $ambiguousNames,
                            'hint' => 'Set QueryOptions::namesForValues to true for named values or false for positional values',
                        ]
                    );
                }
            }
        }
    }

    /**
     * See {@see Request::applyDefaultKeyspace()}.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function applyDefaultKeyspace(string $keyspace): void {

        if (!$this->acceptsDefaultKeyspace($this->options->keyspace)) {
            return;
        }

        if ($this->options->keyspace !== $keyspace) {
            $this->options = $this->options->withKeyspace($keyspace);
        }

        $this->markKeyspaceAsConnectionDefault();
    }

    /**
     * See {@see Request::clearDefaultKeyspace()}.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function clearDefaultKeyspace(): void {

        if (!$this->carriesDefaultKeyspace()) {
            return;
        }

        $this->options = $this->options->withoutKeyspace();

        $this->forgetDefaultKeyspace();
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        $body = pack('N', strlen($this->query)) . $this->query;
        $body .= self::encodeQueryParametersAsBinary(
            $this->consistency,
            $this->values,
            $this->options,
            $this->version,
            exactValueNames: ($this->options->namesForValues === true && $this->values !== [])
                ? self::quotedBindMarkerNames($this->query)
                : [],
        );

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
