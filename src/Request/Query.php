<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Consistency;

final class Query extends Request {
    /**
     * @param array<mixed> $values
     */
    public function __construct(
        protected string $query,
        protected array $values = [],
        protected Consistency $consistency = Consistency::ONE,
        protected QueryOptions $options = new QueryOptions()
    ) {
        parent::__construct(Opcode::REQUEST_QUERY);

        if ($this->options->namesForValues === null && !array_is_list($values)) {
            $this->options = $this->options->withNamesForValues(true);
        }
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    #[\Override]
    public function getBody(): string {
        $body = pack('N', strlen($this->query)) . $this->query;
        $body .= self::queryParametersAsBinary($this->consistency, $this->values, $this->options, $this->version);

        return $body;
    }
}
