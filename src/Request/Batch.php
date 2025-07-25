<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Consistency;
use Cassandra\Request\BatchType;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Type;

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
    public function appendPreparedStatement(PreparedResult $prepareResult, array $values = []): self {

        $prepareData = $prepareResult->getPreparedData();

        $queryId = $prepareData->id;

        if ($prepareData->metadata->columns !== null) {
            $values = self::enocdeValuesForColumnType($values, $prepareData->metadata->columns);
        }

        $binary = chr(1);

        $binary .= pack('n', strlen($queryId)) . $queryId;
        $binary .= self::valuesAsBinary($values, namesForValues: false);

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     */
    public function appendQuery(string $query, array $values = []): self {

        $binary = chr(0);

        $binary .= pack('N', strlen($query)) . $query;
        $binary .= self::valuesAsBinary($values, namesForValues: false);

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
            . self::batchParametersAsBinary($this->consistency, [], $this->options, $this->version);
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    protected function batchParametersAsBinary(Consistency $consistency, array $values = [], BatchOptions $options = new BatchOptions(), int $version = 3): string {
        $flags = 0;
        $optional = '';

        if ($values) {
            $flags |= QueryFlag::VALUES->value;
            $optional .= self::valuesAsBinary($values, namesForValues: false);
        }

        if ($options->serialConsistency !== null) {
            $flags |= QueryFlag::WITH_SERIAL_CONSISTENCY->value;
            $optional .= pack('n', $options->serialConsistency);
        }

        if ($options->defaultTimestamp !== null) {
            $flags |= QueryFlag::WITH_DEFAULT_TIMESTAMP->value;
            $optional .= (new Type\Bigint($options->defaultTimestamp))->getBinary();
        }

        if ($options->keyspace !== null) {
            if ($version >= 5) {
                $flags |= QueryFlag::WITH_KEYSPACE->value;
                $optional .= pack('n', strlen($options->keyspace)) . $options->keyspace;
            } else {
                throw new Exception('Option "keyspace" not supported by server');
            }
        }

        if ($options->nowInSeconds !== null) {
            if ($version >= 5) {
                $flags |= QueryFlag::WITH_NOW_IN_SECONDS->value;
                $optional .= pack('N', $options->nowInSeconds);
            } else {
                throw new Exception('Option "now_in_seconds" not supported by server');
            }
        }

        if ($version < 5) {
            return pack('n', $consistency->value) . chr($flags) . $optional;
        } else {
            return pack('n', $consistency->value) . pack('N', $flags) . $optional;
        }
    }
}
