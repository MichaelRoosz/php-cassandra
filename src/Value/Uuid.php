<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Exception as PhpException;

final class Uuid extends ValueWithFixedLength {
    protected readonly string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 16;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n8', $binary);

        if ($unpacked === false) {
            throw new ValueException(
                'Cannot unpack UUID binary data',
                ExceptionCode::VALUE_UUID_UNPACK_FAILED->value,
                [
                    'binary_length' => strlen($binary),
                    'expected_length' => 16,
                ]
            );
        }

        return new static(sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            $unpacked[1],
            $unpacked[2],
            $unpacked[3],
            $unpacked[4],
            $unpacked[5],
            $unpacked[6],
            $unpacked[7],
            $unpacked[8]
        ));
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value)) {
            throw new ValueException('Invalid UUID value; expected string', ExceptionCode::VALUE_UUID_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
                'expected_format' => 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
            ]);
        }

        return new static($value);
    }

    final public static function fromValue(string $value): static {
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('H*', str_replace('-', '', $this->value));
    }

    #[\Override]
    public function getType(): Type {
        return Type::UUID;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }

    #[\Override]
    final public static function isSerializedAsFixedLength(): bool {
        return true;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public static function random(): static {

        try {
            $bytes = random_bytes(16);
        } catch (PhpException $e) {
            throw new ValueException('Failed to generate random bytes', ExceptionCode::VALUE_UUID_RANDOM_FAILED->value);
        }

        // Set version to 0100
        $bytes[6] = chr(ord($bytes[6]) & 0x0f | 0x40);

        // Set bits 6-7 to 10
        $bytes[8] = chr(ord($bytes[8]) & 0x3f | 0x80);

        return static::fromBinary($bytes);
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
