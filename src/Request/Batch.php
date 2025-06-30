<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Exception;
use Cassandra\Response\Result;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Consistency;
use Cassandra\Request\BatchType;
use Cassandra\Response\ResultKind;

final class Batch extends Request {
    /**
     * @var array<string> $queryArray
     */
    protected array $queryArray = [];

    public function __construct(
        protected BatchType $type = BatchType::LOGGED,
        protected Consistency $consistency = Consistency::ONE,
        protected BatchOptions $options = new BatchOptions()
    ) {
        parent::__construct(Opcode::REQUEST_BATCH);
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    public function appendPreparedStatement(Result $prepareResult, array $values = []): static {

        if ($prepareResult->getKind() !== ResultKind::PREPARED) {
            throw new Exception('Invalid prepared statement');
        }

        $prepareData = $prepareResult->getPreparedData();

        $queryId = $prepareData['id'];

        if (!isset($prepareData['metadata']['columns'])) {
            throw new Exception('missing query metadata');
        }

        $values = self::strictTypeValues($values, $prepareData['metadata']['columns']);

        $binary = chr(1);

        $binary .= pack('n', strlen($queryId)) . $queryId;
        $binary .= Request::valuesBinary($values, namesForValues: false);

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     */
    public function appendQuery(string $query, array $values = []): static {

        $binary = chr(0);

        $binary .= pack('N', strlen($query)) . $query;
        $binary .= Request::valuesBinary($values, namesForValues: false);

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @throws \Cassandra\Exception
     */
    #[\Override]
    public function getBody(): string {
        return chr($this->type->value)
            . pack('n', count($this->queryArray)) . implode('', $this->queryArray)
            . Query::queryParameters($this->consistency, [], $this->options, $this->version);
    }
}
