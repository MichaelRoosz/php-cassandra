<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\Value\EncodeOption\UuidEncodeOption;
use Exception;

final class Uuid extends ValueWithFixedLength implements ValueWithMultipleEncodings {
    /** The value in its raw 16-byte wire form. */
    private readonly string $binary;

    /**
     * Accepts the canonical 36-character string form
     * (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx), the compact 32-character undashed
     * hex form (xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx), or the raw 16-byte binary form
     * (for example the value returned by a UuidEncodeOption::AS_BINARY read). The
     * three forms are distinguished by length and cannot collide (16, 32 and 36
     * bytes respectively). The value is stored raw, so getBinary() is a no-op and
     * the canonical string form is produced on demand.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(string $value) {

        if (strlen($value) === 16) {
            $this->binary = $value;

            return;
        }

        // Hex string form (canonical dashed or compact undashed): validate before
        // packing, otherwise pack('H*', …) would silently coerce non-hex
        // characters to 0 and produce corrupt bytes.
        if (
            preg_match('/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i', $value) !== 1
            && preg_match('/^[0-9a-f]{32}$/i', $value) !== 1
        ) {
            throw new ValueException('Invalid UUID value; expected the canonical string form xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx, the 32-character undashed hex form, or a raw 16-byte binary string', ExceptionCode::VALUE_UUID_INVALID_FORMAT->value, [
                'value' => $value,
                'length' => strlen($value),
            ]);
        }

        $this->binary = pack('H*', str_replace('-', '', $value));
    }

    #[\Override]
    public function asConfigured(ValueEncodeConfig $valueEncodeConfig): mixed {
        return match ($valueEncodeConfig->uuidEncodeOption) {
            UuidEncodeOption::AS_BINARY => $this->binary,
            UuidEncodeOption::AS_STRING => $this->toCanonicalString(),
        };
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
        if (strlen($binary) !== 16) {
            throw new ValueException(
                'Cannot unpack UUID binary data',
                ExceptionCode::VALUE_UUID_UNPACK_FAILED->value,
                [
                    'binary_length' => strlen($binary),
                    'expected_length' => 16,
                ]
            );
        }

        return new static($binary);
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
                'expected_format' => 'canonical string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx", 32-character undashed hex string, or raw 16-byte binary string',
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public static function fromValue(string $value): static {
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return $this->binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::UUID;
    }

    #[\Override]
    public function getValue(): string {
        return $this->toCanonicalString();
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
        } catch (Exception $e) {
            throw new ValueException('Failed to generate random bytes', ExceptionCode::VALUE_UUID_RANDOM_FAILED->value);
        }

        // Set version to 0100
        $bytes[6] = chr(ord($bytes[6]) & 0x0f | 0x40);

        // Set bits 6-7 to 10
        $bytes[8] = chr(ord($bytes[8]) & 0x3f | 0x80);

        return new static($bytes);
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }

    private function toCanonicalString(): string {
        $hex = bin2hex($this->binary);

        return substr($hex, 0, 8) . '-'
            . substr($hex, 8, 4) . '-'
            . substr($hex, 12, 4) . '-'
            . substr($hex, 16, 4) . '-'
            . substr($hex, 20, 12);
    }
}
