<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Decimal extends TypeBase {
    protected string $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    public final function __construct(string $value) {
        if (!is_numeric($value)) {
            throw new Exception('Value must be a numeric string');
        }

        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        $length = strlen($binary);

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N1scale/C*', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary');
        }

        $varintLen = $length - 4;
        $varint = 0;
        for ($i = 1; $i <= $varintLen; ++$i) {
            $varint |= $unpacked[$i] << (($varintLen - $i) * 8);
        }

        $shift = (PHP_INT_SIZE - $varintLen) * 8;
        $varint = (string) ($varint << $shift >> $shift);

        if ($unpacked['scale'] === 0) {
            $value = $varint;
        } elseif (strlen($varint) > $unpacked['scale']) {
            $value = substr($varint, 0, -$unpacked['scale']) . '.' . substr($varint, -$unpacked['scale']);
        } else {
            $value = $varint >= 0 ? sprintf("0.%0$unpacked[scale]d", $varint) : sprintf("-0.%0$unpacked[scale]d", -$varint);
        }

        return new static($value);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function getBinary(): string {
        $pos = strpos($this->value, '.');
        $scaleLen = $pos === false ? 0 : strlen($this->value) - $pos - 1;
        if ($scaleLen) {
            $numericValue = (int) (((float) $this->value) * pow(10, $scaleLen));
        } else {
            $numericValue = (int) $this->value;
        }

        $binary = pack('N', $scaleLen) . (new Varint($numericValue))->getBinary();

        return $binary;
    }

    public function getValue(): string {
        return $this->value;
    }
}
