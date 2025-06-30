<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;
use Cassandra\Protocol\Flag;
use Cassandra\TypeFactory;
use Cassandra\Protocol\Opcode;
use Cassandra\Type;
use Cassandra\Value;
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

        if ($this->flags & Flag::CUSTOM_PAYLOAD->value) {
            if ($this->payload === null) {
                $this->flags &= ~Flag::CUSTOM_PAYLOAD->value;
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
        $this->flags |= Flag::TRACING->value;
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
        $this->flags |= Flag::CUSTOM_PAYLOAD->value;
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
                $strictTypeValues[$key] = TypeFactory::getTypeObjectForValue($column['type'], $values[$key]);
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
