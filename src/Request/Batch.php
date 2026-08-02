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
     * The prepared statements this batch was built with, keyed by their
     * position in {@see self::$queryArray}: the result each was appended with,
     * and the values it was given before they were encoded.
     *
     * Kept because a batch is the one request that can be answered with
     * UNPREPARED without carrying the statement the node has forgotten:
     * {@see \Cassandra\Request\Execute} keeps the prepared result it was built
     * from, but a batch encodes each entry down to a statement id and a value
     * list the moment it is appended. Without this there would be nothing left
     * to prepare again, and nowhere to put the new statement id — the whole
     * batch would fail on a statement the node merely forgot.
     *
     * The values are kept unencoded rather than the encoded bytes being
     * patched, because a repreparation is exactly the case where the bind
     * marker types may have moved under them: an ALTER is one of the reasons a
     * node stops recognising a statement id. They are encoded again against the
     * new metadata by {@see self::replacePreparedStatement()}.
     *
     * @var array<int, array{result: PreparedResult, values: array<mixed>}> $preparedStatements
     */
    private array $preparedStatements = [];

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

        // Encoded before either array is touched, so that a value this batch
        // cannot encode leaves it exactly as it was rather than half appended.
        $binary = $this->encodePreparedStatement($prepareResult, $values);

        $this->preparedStatements[count($this->queryArray)] = [
            'result' => $prepareResult,
            'values' => $values,
        ];

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
     * The prepared statement this batch was appended with under $id, if it
     * holds one.
     *
     * Asked when a node answers UNPREPARED, which names the statement id it did
     * not recognise; what comes back is what that id was prepared from, and so
     * what has to be prepared again.
     */
    public function findPreparedStatement(string $id): ?PreparedResult {

        foreach ($this->preparedStatements as $entry) {
            if ($entry['result']->getPreparedData()->id === $id) {
                return $entry['result'];
            }
        }

        return null;
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function getBody(): string {

        $statementCount = count($this->queryArray);
        if ($statementCount > self::MAX_SHORT_COUNT) {
            throw new RequestException(
                message: 'Too many statements in one batch; the protocol counts them in two bytes',
                code: ExceptionCode::REQUEST_BATCH_TOO_MANY_STATEMENTS->value,
                context: [
                    'stage' => 'batch_encoding',
                    'statement_count' => $statementCount,
                    'max_statement_count' => self::MAX_SHORT_COUNT,
                ]
            );
        }

        return chr($this->type->value)
            . pack('n', $statementCount) . implode('', $this->queryArray)
            . self::encodeBatchParametersAsBinary($this->consistency, [], $this->options, $this->version);
    }

    /**
     * How many distinct prepared statements this batch carries.
     *
     * The repreparation budget is measured against this: a node answers
     * UNPREPARED for one statement at a time, so a batch whose statements the
     * node has all forgotten needs one round per distinct statement to be
     * recovered. See {@see \Cassandra\Connection\ResponseDispatcher::MAX_REPREPARATIONS}.
     *
     * Counted by what each entry was prepared from rather than by the statement
     * id it currently carries, which is what {@see self::replacePreparedStatement()}
     * matches on and therefore what a round actually costs. The two only differ
     * where a statement id says less than the query behind it does; the id is
     * the fallback for an entry whose PREPARE was not kept.
     */
    public function getDistinctPreparedStatementCount(): int {

        $statements = [];
        foreach ($this->preparedStatements as $entry) {
            $request = $entry['result']->getRequest();

            $statements[$request instanceof Prepare ? $request->getHash() : $entry['result']->getPreparedData()->id] = true;
        }

        return count($statements);
    }

    public function getOptions(): BatchOptions {
        return $this->options;
    }

    #[\Override]
    public function getRequestTimeout(): ?float {
        return $this->options->requestTimeoutInSeconds;
    }

    /**
     * Put a freshly prepared statement in place of the one it was prepared to
     * replace, and return how many entries that came to.
     *
     * The entries to replace are found by matching what each was prepared from
     * against what $newResult was prepared from — the query together with the
     * keyspace, which is what {@see Prepare::getHash()} is — rather than by the
     * statement id the node reported. That is deliberate: the id is what the
     * node has just told us it no longer knows, whereas the PREPARE this result
     * answers was built from the very entry being replaced, so the two match by
     * construction and nothing has to be carried across the round trip. It also
     * settles the case the id cannot: the same statement appended to a batch
     * more than once is several entries sharing one id, and all of them need
     * the new one.
     *
     * Zero means nothing matched, which is a repreparation that has lost its
     * way rather than a batch that needed none — the caller reports it.
     *
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    public function replacePreparedStatement(PreparedResult $newResult): int {

        $newRequest = $newResult->getRequest();
        if (!($newRequest instanceof Prepare)) {
            return 0;
        }

        $hash = $newRequest->getHash();

        $replaced = 0;

        foreach ($this->preparedStatements as $index => $entry) {
            $request = $entry['result']->getRequest();
            if (!($request instanceof Prepare) || $request->getHash() !== $hash) {
                continue;
            }

            // Encoded before either array is touched, as in
            // {@see self::appendPreparedStatement()}: a value the new bind
            // marker types cannot take must not leave the batch holding a
            // statement id whose values were encoded for another one.
            $binary = $this->encodePreparedStatement($newResult, $entry['values']);

            $this->preparedStatements[$index] = [
                'result' => $newResult,
                'values' => $entry['values'],
            ];

            $this->queryArray[$index] = $binary;

            $replaced++;
        }

        return $replaced;
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
                self::assertShortString($options->keyspace, 'batch keyspace');
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

    /**
     * One prepared entry of the batch body: the kind byte, the statement id and
     * the values, encoded against that statement's own bind marker types.
     *
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    private function encodePreparedStatement(PreparedResult $prepareResult, array $values): string {

        $prepareData = $prepareResult->getPreparedData();

        $queryId = $prepareData->id;

        $encodedValues = $this->encodeQueryValuesForBindMarkerTypes(
            $values,
            $prepareData->prepareMetadata->bindMarkers,
            false
        );

        return chr(1)
            . pack('n', strlen($queryId)) . $queryId
            . $this->encodeQueryValuesAsBinary($encodedValues, namesForValues: false);
    }
}
