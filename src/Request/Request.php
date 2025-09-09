<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Protocol\Frame;
use Cassandra\Protocol\Flag;
use Cassandra\ValueFactory;
use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Value\NotSet;
use Cassandra\Value\ValueBase;
use DateTimeInterface;
use Stringable;

abstract class Request implements Frame, Stringable {
    /**
     * @param ?array<string,string> $payload
     */
    public function __construct(
        protected Opcode $opcode,
        protected int $stream = 0,
        protected int $flags = 0,
        protected ?array $payload = null,
        protected int $version = 3
    ) {
    }

    #[\Override]
    public function __toString(): string {
        $body = $this->getBody();

        if ($this->flags & Flag::CUSTOM_PAYLOAD) {
            if ($this->payload === null) {
                $this->flags &= ~Flag::CUSTOM_PAYLOAD;
            } else {
                $payloadData = pack('n', count($this->payload));

                foreach ($this->payload as $key => $val) {
                    $payloadData .= pack('n', strlen($key)) . $key;
                    $payloadData .= pack('N', strlen($val)) . $val;
                }

                $body = $payloadData . $body;
            }
        }

        return pack(
            'CCnCN',
            $this->version,
            $this->flags,
            $this->stream,
            $this->opcode->value,
            strlen($body)
        ) . $body;
    }

    public function enableTracing(): void {
        $this->flags |= Flag::TRACING;
    }

    #[\Override]
    public function getBody(): string {
        return '';
    }

    #[\Override]
    public function getFlags(): int {
        return $this->flags;
    }

    #[\Override]
    public function getOpcode(): Opcode {
        return $this->opcode;
    }

    /**
     * @return ?array<string,string>
     */
    public function getPayload(): ?array {
        return $this->payload;
    }

    #[\Override]
    public function getStream(): int {
        return $this->stream;
    }

    #[\Override]
    public function getVersion(): int {
        return $this->version;
    }

    public function setFlags(int $flags): void {
        $this->flags = $flags;
    }

    /**
     * @param array<string,string> $payload
     */
    public function setPayload(array $payload): void {
        $this->payload = $payload;
        $this->flags |= Flag::CUSTOM_PAYLOAD;
    }

    public function setStream(int $stream): void {
        $this->stream = $stream;
    }

    public function setVersion(int $version): void {
        $this->version = $version;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    protected function encodeQueryParametersAsBinary(Consistency $consistency, array $values = [], QueryOptions $options = new QueryOptions(), int $version = 3): string {
        $flags = 0;
        $optional = '';

        if ($values) {
            $flags |= QueryFlag::VALUES;
            $optional .= self::encodeQueryValuesAsBinary($values, $options->namesForValues === true);
        }

        if (($options instanceof ExecuteOptions) && $options->skipMetadata) {
            $flags |= QueryFlag::SKIP_METADATA;
        }

        if ($options->pageSize !== null) {
            $flags |= QueryFlag::PAGE_SIZE;
            $optional .= pack('N', max(100, $options->pageSize));
        }

        if ($options->pagingState !== null) {
            $flags |= QueryFlag::WITH_PAGING_STATE;
            $optional .= pack('N', strlen($options->pagingState)) . $options->pagingState;
        }

        if ($options->serialConsistency !== null) {
            $flags |= QueryFlag::WITH_SERIAL_CONSISTENCY;
            $optional .= pack('n', $options->serialConsistency);
        }

        if ($options->defaultTimestamp !== null) {
            $flags |= QueryFlag::WITH_DEFAULT_TIMESTAMP;
            $optional .= pack('J', $options->defaultTimestamp);
        }

        if ($options->namesForValues === true) {
            $flags |= QueryFlag::WITH_NAMES_FOR_VALUES;
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
                        'request' => 'QUERY',
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
                        'request' => 'QUERY',
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

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    protected function encodeQueryValuesAsBinary(array $values, bool $namesForValues = false): string {
        $valuesBinary = pack('n', count($values));

        /** @psalm-suppress MixedAssignment */
        foreach ($values as $name => $value) {
            switch (true) {
                case $value instanceof ValueBase:
                    $binary = $value->getBinary();

                    break;

                case $value instanceof NotSet:
                    $binary = $value;

                    break;

                case $value === null:
                    $binary = null;

                    break;

                case is_int($value):
                    $binary = pack('N', $value);

                    break;

                case is_string($value):
                    $binary = $value;

                    break;

                case is_bool($value):
                    $binary = $value ? chr(1) : chr(0);

                    break;

                case is_float($value):
                    $binary = pack('E', $value);

                    break;

                case $value instanceof DateTimeInterface:
                    $binary = $value->format('Y-m-d H:i:s.vO');

                    break;

                default:
                    throw new RequestException(
                        message: 'Unsupported bound value type',
                        code: ExceptionCode::REQUEST_VALUES_UNSUPPORTED_VALUE_TYPE->value,
                        context: [
                            'stage' => 'values_encoding',
                            'php_type' => gettype($value),
                            'name' => $name,
                        ]
                    );
            }

            if ($namesForValues) {
                if (is_string($name)) {
                    // strtolower is okay to use here, since column names are defined
                    // as identifiers, which consist of: [A-Za-z0-9_]+.
                    $valuesBinary .= pack('n', strlen($name)) . strtolower($name);
                } else {
                    throw new RequestException(
                        message: 'Invalid values format: sequential array provided while names_for_values=true expects associative array',
                        code: ExceptionCode::REQUEST_VALUES_NAMES_FOR_VALUES_EXPECTS_ASSOCIATIVE->value,
                        context: [
                            'stage' => 'values_encoding',
                            'names_for_values' => true,
                            'provided_key_type' => gettype($name),
                        ]
                    );
                }
            } elseif (is_string($name)) {
                /**
                * @see https://github.com/duoshuo/php-cassandra/issues/29
                */
                throw new RequestException(
                    message: 'Invalid values format: associative array provided while names_for_values=false expects sequential array',
                    code: ExceptionCode::REQUEST_VALUES_NAMES_FOR_VALUES_EXPECTS_SEQUENTIAL->value,
                    context: [
                        'stage' => 'values_encoding',
                        'names_for_values' => false,
                        'provided_key_type' => 'string',
                    ]
                );
            }

            if ($binary === null) {
                $valuesBinary .= "\xff\xff\xff\xff";
            } elseif ($binary instanceof NotSet) {
                $valuesBinary .= "\xff\xff\xff\xfe";
            } else {
                $valuesBinary .= pack('N', strlen($binary)) . $binary;
            }
        }

        return $valuesBinary;
    }

    /**
     * @param array<mixed> $values
     * @param array<\Cassandra\Response\Result\ColumnInfo> $bindMarkers
     * @return array<mixed>
     *
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    protected function encodeQueryValuesForBindMarkerTypes(array $values, array $bindMarkers, bool $namesForValues): array {
        $encodedValues = [];
        foreach ($bindMarkers as $index => $bindMarker) {

            if ($namesForValues) {
                $key = $bindMarker->name;
            } else {
                $key = $index;
            }

            if (!isset($values[$key])) {
                $encodedValues[$key] = null;
            } elseif (
                ($values[$key] instanceof ValueBase)
                || ($values[$key] instanceof NotSet)
            ) {
                $encodedValues[$key] = $values[$key];
            } else {
                $encodedValues[$key] = ValueFactory::getValueObjectFromValue($bindMarker->type, $values[$key]);
            }
        }

        return $encodedValues;
    }
}
