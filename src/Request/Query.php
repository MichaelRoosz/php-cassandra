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
            exactValueNames: self::quotedBindMarkerNames($this->query),
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

    /**
     * Names of quoted bind markers, keyed for an exact-name lookup while the
     * QUERY values are encoded. CQL strings, quoted identifiers and comments
     * are skipped so a colon inside any of them is not mistaken for a marker.
     *
     * @return array<string, true>
     */
    private static function quotedBindMarkerNames(string $query): array {
        $names = [];
        $length = strlen($query);

        for ($offset = 0; $offset < $length; ++$offset) {
            $character = $query[$offset];

            if ($character === "'") {
                self::skipQuoted($query, $offset, "'");

                continue;
            }

            if ($character === '-' && ($query[$offset + 1] ?? '') === '-') {
                self::skipLineComment($query, $offset);

                continue;
            }

            if ($character === '/' && ($query[$offset + 1] ?? '') === '/') {
                self::skipLineComment($query, $offset);

                continue;
            }

            if ($character === '/' && ($query[$offset + 1] ?? '') === '*') {
                $closing = strpos($query, '*/', $offset + 2);
                $offset = $closing === false ? $length : $closing + 1;

                continue;
            }

            if ($character === ':' && ($query[$offset + 1] ?? '') === '"') {
                ++$offset;
                $name = self::readQuoted($query, $offset);
                $names[$name] = true;

                continue;
            }

            if ($character === '"') {
                self::skipQuoted($query, $offset, '"');
            }
        }

        return $names;
    }

    private static function readQuoted(string $query, int &$offset, string $quote = '"'): string {
        $value = '';
        $length = strlen($query);

        for (++$offset; $offset < $length; ++$offset) {
            $character = $query[$offset];
            if ($character !== $quote) {
                $value .= $character;

                continue;
            }

            if (($query[$offset + 1] ?? '') === $quote) {
                $value .= $quote;
                ++$offset;

                continue;
            }

            return $value;
        }

        return $value;
    }

    private static function skipLineComment(string $query, int &$offset): void {
        $newline = strpos($query, "\n", $offset + 2);
        $offset = $newline === false ? strlen($query) : $newline;
    }

    private static function skipQuoted(string $query, int &$offset, string $quote): void {
        self::readQuoted($query, $offset, $quote);
    }
}
