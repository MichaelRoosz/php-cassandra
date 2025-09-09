<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Consistency;
use Cassandra\Exception\RequestException;
use Cassandra\Response\Result\PreparedResult;

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
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function appendPreparedStatement(PreparedResult $prepareResult, array $values = []): self {

        $prepareData = $prepareResult->getPreparedData();

        $queryId = $prepareData->id;

        $values = self::encodeQueryValuesForBindMarkerTypes(
            $values,
            $prepareData->prepareMetadata->bindMarkers,
            false
        );

        $binary = chr(1);

        $binary .= pack('n', strlen($queryId)) . $queryId;
        $binary .= self::encodeQueryValuesAsBinary($values, namesForValues: false);

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    public function appendQuery(string $query, array $values = []): self {

        $binary = chr(0);

        $binary .= pack('N', strlen($query)) . $query;
        $binary .= self::encodeQueryValuesAsBinary($values, namesForValues: false);

        $this->queryArray[] = $binary;

        return $this;
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {
        return chr($this->type->value)
            . pack('n', count($this->queryArray)) . implode('', $this->queryArray)
            . self::encodeBatchParametersAsBinary($this->consistency, [], $this->options, $this->version);
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    protected function encodeBatchParametersAsBinary(
        Consistency $consistency,
        array $values = [],
        BatchOptions $options = new BatchOptions(),
        int $version = 3
    ): string {

        $flags = 0;
        $optional = '';

        if ($values) {
            $flags |= QueryFlag::VALUES;
            $optional .= self::encodeQueryValuesAsBinary($values, namesForValues: false);
        }

        if ($options->serialConsistency !== null) {
            $flags |= QueryFlag::WITH_SERIAL_CONSISTENCY;
            $optional .= pack('n', $options->serialConsistency->value);
        }

        if ($options->defaultTimestamp !== null) {
            $flags |= QueryFlag::WITH_DEFAULT_TIMESTAMP;
            $optional .= pack('J', $options->defaultTimestamp);
        }

        if ($options->keyspace !== null) {
            if ($version >= 5) {
                $flags |= QueryFlag::WITH_KEYSPACE;
                $optional .= pack('n', strlen($options->keyspace)) . $options->keyspace;
            } else {
                throw new RequestException(
                    message: 'Server protocol version does not support request option "keyspace"',
                    code: ExceptionCode::REQUEST_UNSUPPORTED_OPTION_KEYSPACE->value,
                    context: [
                        'request' => 'BATCH',
                        'option' => 'keyspace',
                        'required_protocol' => 'v5',
                        'actual_protocol' => $version,
                        'keyspace' => $options->keyspace,
                    ]
                );
            }
        }

        if ($options->nowInSeconds !== null) {
            if ($version >= 5) {
                $flags |= QueryFlag::WITH_NOW_IN_SECONDS;
                $optional .= pack('N', $options->nowInSeconds);
            } else {
                throw new RequestException(
                    message: 'Server protocol version does not support request option "now_in_seconds"',
                    code: ExceptionCode::REQUEST_UNSUPPORTED_OPTION_NOW_IN_SECONDS->value,
                    context: [
                        'request' => 'BATCH',
                        'option' => 'now_in_seconds',
                        'required_protocol' => 'v5',
                        'actual_protocol' => $version,
                        'now_in_seconds' => $options->nowInSeconds,
                    ]
                );
            }
        }

        if ($version < 5) {
            return pack('n', $consistency->value) . chr($flags) . $optional;
        } else {
            return pack('n', $consistency->value) . pack('N', $flags) . $optional;
        }
    }
}
