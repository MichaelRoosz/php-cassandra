<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeInfo\TypeInfo;

final class Decimal extends TypeBase {
    protected readonly string $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(string|int|float $value) {
        if (!is_numeric($value)) {
            throw new Exception('Value must be a numeric value', ExceptionCode::TYPE_DECIMAL_NON_NUMERIC->value, [
                'value' => $value,
            ]);
        }

        $this->value = (string) $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        $length = strlen($binary);

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N1scale/C*', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack decimal binary data', ExceptionCode::TYPE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'note' => 'expected >= 4 bytes (scale + varint)',
            ]);
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
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_numeric($value)) {
            throw new Exception('Invalid decimal value; expected numeric value', ExceptionCode::TYPE_DECIMAL_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        $pos = strpos($this->value, '.');
        $scaleLen = $pos === false ? 0 : strlen($this->value) - $pos - 1;
        if ($scaleLen) {
            $numericValue = (int) (((float) $this->value) * (float) pow(10, $scaleLen));
        } else {
            $numericValue = (int) $this->value;
        }

        $binary = pack('N', $scaleLen) . (new Varint($numericValue))->getBinary();

        return $binary;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }
}
