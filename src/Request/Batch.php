<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Exception;
use Cassandra\Response\Result;
use Cassandra\Request\Options\BatchOptions;

final class Batch extends Request {
    final public const TYPE_COUNTER = 2;
    final public const TYPE_LOGGED = 0;
    final public const TYPE_UNLOGGED = 1;

    protected int $batchType;

    protected int $consistency;

    protected int $opcode = Opcode::REQUEST_BATCH;

    protected BatchOptions $options;

    /**
     * @var array<string> $queryArray
     */
    protected array $queryArray = [];

    public function __construct(?int $type = null, ?int $consistency = null, BatchOptions $options = new BatchOptions()) {
        $this->batchType = $type === null ? Batch::TYPE_LOGGED : $type;
        $this->consistency = $consistency === null ? Request::CONSISTENCY_ONE : $consistency;
        $this->options = $options;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    public function appendPreparedStatement(Result $prepareResult, array $values = []): static {

        if ($prepareResult->getKind() !== Result::PREPARED) {
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
    public function appendQuery(string $cql, array $values = []): static {

        $binary = chr(0);

        $binary .= pack('N', strlen($cql)) . $cql;
        $binary .= Request::valuesBinary($values, namesForValues: false);

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @throws \Cassandra\Exception
     */
    #[\Override]
    public function getBody(): string {
        return chr($this->batchType)
            . pack('n', count($this->queryArray)) . implode('', $this->queryArray)
            . self::queryParameters($this->consistency, [], $this->options, $this->version);
    }
}
