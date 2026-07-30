<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\ProtocolVersion;
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
    private array $queryArray = [];

    public function __construct(
        private BatchType $type = BatchType::LOGGED,
        private Consistency $consistency = Consistency::ONE,
        private BatchOptions $options = new BatchOptions()
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
        return chr($this->type->value)
            . pack('n', count($this->queryArray)) . implode('', $this->queryArray)
            . self::encodeBatchParametersAsBinary($this->consistency, [], $this->options, $this->version);
    }

    public function getOptions(): BatchOptions {
        return $this->options;
    }

    #[\Override]
    public function getRequestTimeout(): ?float {
        return $this->options->requestTimeoutInSeconds;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    private function encodeBatchParametersAsBinary(
        Consistency $consistency,
        array $values = [],
        BatchOptions $options = new BatchOptions(),
        ProtocolVersion $version = ProtocolVersion::V3
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
            if ($version->value >= ProtocolVersion::V5->value) {
                $flags |= QueryFlag::WITH_KEYSPACE;
                $optional .= pack('n', strlen($options->keyspace)) . $options->keyspace;
            } else {
                throw new RequestException(
                    message: 'Server protocol version does not support request option "keyspace"',
                    code: ExceptionCode::REQUEST_UNSUPPORTED_OPTION_KEYSPACE->value,
                    context: [
                        'request' => 'BATCH',
                        'option' => 'keyspace',
                        'required_protocol_version' => ProtocolVersion::V5->inOptionFormat(),
                        'actual_protocol_version' => $version->inOptionFormat(),
                        'keyspace' => $options->keyspace,
                    ]
                );
            }
        }

        if ($options->nowInSeconds !== null) {
            if ($version->value >= ProtocolVersion::V5->value) {
                $flags |= QueryFlag::WITH_NOW_IN_SECONDS;
                $optional .= pack('N', $options->nowInSeconds);
            } else {
                throw new RequestException(
                    message: 'Server protocol version does not support request option "now_in_seconds"',
                    code: ExceptionCode::REQUEST_UNSUPPORTED_OPTION_NOW_IN_SECONDS->value,
                    context: [
                        'request' => 'BATCH',
                        'option' => 'now_in_seconds',
                        'required_protocol_version' => ProtocolVersion::V5->inOptionFormat(),
                        'actual_protocol_version' => $version->inOptionFormat(),
                        'now_in_seconds' => $options->nowInSeconds,
                    ]
                );
            }
        }

        if ($version->value < ProtocolVersion::V5->value) {
            return pack('n', $consistency->value) . chr($flags & 0xFF) . $optional;
        } else {
            return pack('n', $consistency->value) . pack('N', $flags) . $optional;
        }
    }
}
