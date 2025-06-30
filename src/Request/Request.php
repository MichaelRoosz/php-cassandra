<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;
use Cassandra\Protocol\Flag;
use Cassandra\Request\Options\RequestOptions;
use Cassandra\Type;
use Cassandra\Value;
use Stringable;

abstract class Request implements Frame, Stringable {
    final public const CONSISTENCY_ALL = 0x0005;
    final public const CONSISTENCY_ANY = 0x0000;
    final public const CONSISTENCY_EACH_QUORUM = 0x0007;
    final public const CONSISTENCY_LOCAL_ONE = 0x000A;
    final public const CONSISTENCY_LOCAL_QUORUM = 0x0006;
    final public const CONSISTENCY_LOCAL_SERIAL = 0x0009;
    final public const CONSISTENCY_ONE = 0x0001;
    final public const CONSISTENCY_QUORUM = 0x0004;
    final public const CONSISTENCY_SERIAL = 0x0008;
    final public const CONSISTENCY_THREE = 0x0003;
    final public const CONSISTENCY_TWO = 0x0002;

    protected int $flags = 0;

    protected int $opcode;

    /**
     * @var ?array<string,string> $payload
     */
    protected ?array $payload = null;

    protected int $stream = 0;

    protected int $version = 3;

    /**
     * @param ?array<string,string> $payload
     */
    public function __construct(int $opcode, int $stream = 0, int $flags = 0, ?array $payload = null, int $version = 3) {
        $this->opcode = $opcode;
        $this->stream = $stream;
        $this->flags = $flags;
        $this->payload = $payload;
        $this->version = $version;
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
            $this->opcode,
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
    public function getOpcode(): int {
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

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    public static function queryParameters(int $consistency, array $values = [], RequestOptions $options = new RequestOptions(), int $version = 3): string {
        $flags = 0;
        $optional = '';

        $opt = $options->toArray();

        if ($values) {
            $flags |= Query::FLAG_VALUES;
            $optional .= Request::valuesBinary($values, !empty($opt['names_for_values']));
        }

        if (!empty($opt['skip_metadata'])) {
            $flags |= Query::FLAG_SKIP_METADATA;
        }

        if (isset($opt['page_size'])) {
            $flags |= Query::FLAG_PAGE_SIZE;
            $optional .= pack('N', $opt['page_size']);
        }

        if (isset($opt['paging_state'])) {
            $flags |= Query::FLAG_WITH_PAGING_STATE;
            $optional .= pack('N', strlen($opt['paging_state'])) . $opt['paging_state'];
        }

        if (isset($opt['serial_consistency'])) {
            $flags |= Query::FLAG_WITH_SERIAL_CONSISTENCY;
            $optional .= pack('n', $opt['serial_consistency']);
        }

        if (isset($opt['default_timestamp'])) {
            $flags |= Query::FLAG_WITH_DEFAULT_TIMESTAMP;
            $optional .= (new Type\Bigint($opt['default_timestamp']))->getBinary();
        }

        if (!empty($opt['names_for_values'])) {
            $flags |= Query::FLAG_WITH_NAMES_FOR_VALUES;
        }

        if (isset($opt['keyspace'])) {
            if ($version >= 5) {
                $flags |= Query::FLAG_WITH_KEYSPACE;
                $optional .= pack('n', strlen($opt['keyspace'])) . $opt['keyspace'];
            } else {
                throw new Exception('Option "keyspace" not supported by server');
            }
        }

        if (isset($opt['now_in_seconds'])) {
            if ($version >= 5) {
                $flags |= Query::FLAG_WITH_NOW_IN_SECONDS;
                $optional .= pack('N', $opt['now_in_seconds']);
            } else {
                throw new Exception('Option "now_in_seconds" not supported by server');
            }
        }

        if ($version < 5) {
            return pack('n', $consistency) . chr($flags) . $optional;
        } else {
            return pack('n', $consistency) . pack('N', $flags) . $optional;
        }
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
     * @param array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     * }> $columns
     * @return array<mixed>
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function strictTypeValues(array $values, array $columns): array {
        $strictTypeValues = [];
        foreach ($columns as $index => $column) {
            $key = array_key_exists($column['name'], $values) ? $column['name'] : $index;

            if (!isset($values[$key])) {
                $strictTypeValues[$key] = null;
            } elseif ($values[$key] instanceof Type\TypeBase) {
                $strictTypeValues[$key] = $values[$key];
            } else {
                $strictTypeValues[$key] = Type::getTypeObjectForValue($column['type'], $values[$key]);
            }
        }

        return $strictTypeValues;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function valuesBinary(array $values, bool $namesForValues = false): string {
        $valuesBinary = pack('n', count($values));

        /** @var mixed $value */
        foreach ($values as $name => $value) {
            switch (true) {
                case $value instanceof Type\TypeBase:
                    $binary = $value->getBinary();

                    break;

                case $value instanceof Value\NotSet:
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

                default:
                    throw new Type\Exception('Unknown type.');
            }

            if ($namesForValues) {
                if (is_string($name)) {
                    $valuesBinary .= pack('n', strlen($name)) . strtolower($name);
                } else {
                    throw new Type\Exception('$values should be an associative array given, sequential array given. Or you can set "names_for_values" option to false.');
                }
            } elseif (is_string($name)) {
                /**
                * @see https://github.com/duoshuo/php-cassandra/issues/29
                */
                throw new Type\Exception('$values should be an sequential array, associative array given. Or you can set "names_for_values" option to true.');
            }

            if ($binary === null) {
                $valuesBinary .= "\xff\xff\xff\xff";
            } elseif ($binary instanceof Value\NotSet) {
                $valuesBinary .= "\xff\xff\xff\xfe";
            } else {
                $valuesBinary .= pack('N', strlen($binary)) . $binary;
            }
        }

        return $valuesBinary;
    }
}
