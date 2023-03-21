<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;

class Date extends Base
{
    protected ?int $_value = null;

    public function __construct(?int $value = null)
    {
        $this->_value = $value;
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    protected static function create(mixed $value, null|int|array $definition): self
    {
        if ($value !== null && !is_int($value)) {
            throw new Exception('Invalid value type');
        }

        return new self($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function binaryOfValue(): string
    {
        if ($this->_value === null) {
            throw new Exception('value is null');
        }

        return static::binary($this->_value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function parseValue(): ?int
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    public static function fromDateTime(DateTimeInterface $value): self
    {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = $baseDate->diff($value);
        $days = pow(2, 31) + intval($interval->format('%r%a'));

        return new self((int)$days);
    }

    /**
     * @throws \Exception
     */
    public static function fromString(string $value): self
    {
        $inputDate = new DateTimeImmutable($value);

        return self::fromDateTime($inputDate);
    }

    /**
     * @throws \Exception
     */
    public static function toDateTime(int $value): DateTimeImmutable
    {
        $value -= pow(2, 31);
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = new DateInterval('P' . abs($value) . 'D');

        if ($value >= 0) {
            return $baseDate->add($interval);
        } else {
            return $baseDate->sub($interval);
        }
    }

    /**
     * @throws \Exception
     */
    public static function toString(int $value): string
    {
        return self::toDateTime($value)->format('Y-m-d');
    }

    /**
     * @throws \Exception
     */
    public function __toString(): string
    {
        if ($this->_value === null) {
            return '(null)';
        }

        return self::toString($this->_value);
    }

    public static function binary(int $value): string
    {
        return pack('N', $value);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): int
    {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return $unpacked[1];
    }
}
