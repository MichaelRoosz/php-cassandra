<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Decimal extends ValueReadableWithLength {
    protected readonly string $value;

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public function __construct(string|int|float $value) {
        if (!is_numeric($value)) {
            throw new Exception('Value must be a numeric value', ExceptionCode::VALUE_DECIMAL_NON_NUMERIC->value, [
                'value' => $value,
            ]);
        }

        if (is_float($value)) {
            $this->value = number_format($value, 0, '.', '');

            return;
        }

        $this->value = (string) $value;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

        $length = strlen($binary);
        if ($length < 4) {
            throw new Exception('Cannot unpack decimal binary data', ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => $length,
                'note' => 'expected >= 4 bytes (scale + varint)',
            ]);
        }
        /**
         * @var false|array<int> $scaleUnpacked
         */
        $scaleUnpacked = unpack('N', substr($binary, 0, 4));
        if ($scaleUnpacked === false) {
            throw new Exception('Cannot unpack decimal scale', ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => $length,
            ]);
        }
        $scale = $scaleUnpacked[1];

        $varintBinary = substr($binary, 4);
        $unscaledVarint = Varint::fromBinary($varintBinary);
        $unscaled = $unscaledVarint->asString();

        if ($scale === 0) {
            $value = $unscaled;
        } else {
            $isNegative = str_starts_with($unscaled, '-');
            $absUnscaled = $isNegative ? substr($unscaled, 1) : $unscaled;

            // Pad with zeros if necessary
            $absUnscaled = str_pad($absUnscaled, $scale + 1, '0', STR_PAD_LEFT);

            // Insert decimal point
            $integerPart = substr($absUnscaled, 0, -$scale);
            $decimalPart = substr($absUnscaled, -$scale);

            // Remove leading zeros from integer part, but keep at least one digit
            $integerPart = ltrim($integerPart, '0') ?: '0';

            $value = $integerPart . '.' . $decimalPart;
            if ($isNegative) {
                $value = '-' . $value;
            }
        }

        return new static($value);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_numeric($value)) {
            throw new Exception('Invalid decimal value; expected numeric value', ExceptionCode::VALUE_DECIMAL_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public static function fromValue(string|int|float $value): static {
        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public function getBinary(): string {

        $scalePos = strpos($this->value, '.');
        $hasScale = $scalePos !== false;

        if ($hasScale) {
            $scale = strlen($this->value) - $scalePos - 1;
            $unscaled = substr($this->value, 0, $scalePos) . substr($this->value, $scalePos + 1);
        } else {
            $scale = 0;
            $unscaled = $this->value;
        }

        $binary = pack('N', $scale) . (new Varint($unscaled))->getBinary();

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::DECIMAL;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
