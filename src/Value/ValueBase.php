<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Stringable;

abstract class ValueBase implements Stringable {
    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function __toString(): string {
        /** @psalm-suppress MixedAssignment */
        $value = $this->getValue();

        if (is_string($value)) {
            return $value;
        }

        /**
         * @throws \Cassandra\Exception\ValueException
         * */
        $json = json_encode(
            $value,
            JSON_PRETTY_PRINT
            | JSON_PRESERVE_ZERO_FRACTION
            | JSON_PARTIAL_OUTPUT_ON_ERROR
            | JSON_UNESCAPED_LINE_TERMINATORS
            | JSON_UNESCAPED_SLASHES
            | JSON_UNESCAPED_UNICODE
        );

        return $json === false ? '' : $json;
    }

    /**
     * Whether a zero-length ("empty") value is allowed for this type.
     *
     * The value follows Cassandra's type *serializer* rather than that its
     * "allowsEmpty" method, because the serializer is where Cassandra actually
     * enforces it and the two do not always agree.
     */
    public static function allowsEmpty(): bool {
        return false;
    }

    abstract public static function fixedLength(): int;

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    abstract public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): ?static;

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    abstract public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static;

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    abstract public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static;

    abstract public function getBinary(): string;

    abstract public function getType(): Type;

    abstract public function getValue(): mixed;

    abstract public static function hasFixedLength(): bool;

    /**
     * Whether this object's existing bytes can be used for the destination type
     * without converting the value.
     *
     * Simple values need only the same type discriminator. Values whose type is
     * defined recursively expose that definition through
     * {@see self::binaryTypeInfo()}; exact immutable TypeInfo trees take PHP's
     * native structural-comparison path. The successful path can therefore reuse
     * {@see self::getBinary()} instead of decoding and encoding the value again.
     *
     * @internal
     */
    final public function isBinaryCompatibleWith(TypeInfo $typeInfo): bool {
        $valueType = $this->getType();
        if (
            $valueType !== $typeInfo->type
            && !(
                ($valueType === Type::TEXT && $typeInfo->type === Type::VARCHAR)
                || ($valueType === Type::VARCHAR && $typeInfo->type === Type::TEXT)
            )
        ) {
            return false;
        }

        $valueTypeInfo = $this->binaryTypeInfo();

        return $valueTypeInfo === null
            ? !static::requiresDefinition()
            : $valueTypeInfo->isBinaryCompatibleWith($typeInfo);
    }

    /**
     * Whether an empty value of this type denotes null rather than a value of
     * its own.
     * 
     * Only consulted where {@see self::allowsEmpty()} is true; a type that does
     * not admit an empty value has nothing to say about what one would mean.
     */
    public static function isEmptyValueMeaningless(): bool {
        return false;
    }

    abstract public static function isReadableWithoutLength(): bool;

    abstract public static function isSerializedAsFixedLength(): bool;

    abstract public static function requiresDefinition(): bool;

    /**
     * The immutable type definition that determines this value's binary layout,
     * or null for a simple type whose discriminator is the whole definition.
     */
    protected function binaryTypeInfo(): ?TypeInfo {
        return null;
    }

    /**
     * Whether an empty binary is this type's spelling of null, so that
     * {@see self::fromBinary()} reports one.
     *
     * Asked of the two predicates rather than of the string alone, so what a
     * decoder does with an empty value can never drift from what its type
     * declares about one.
     */
    final protected static function emptyBinaryIsNull(string $binary): bool {

        return $binary === ''
            && static::allowsEmpty()
            && static::isEmptyValueMeaningless();
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    protected static function require64Bit(): void {
        if (PHP_INT_SIZE < 8) {
            $className = static::class;

            throw new ValueException('The ' . $className . ' data type requires 64-bit integers, 64-bit php is required', ExceptionCode::VALUE_TYPE_REQUIRES_64BIT_INTEGER->value, [
                'class' => $className,
                'php_int_size_bytes' => PHP_INT_SIZE,
                'php_int_size_bits' => PHP_INT_SIZE * 8,
            ]);
        }
    }
}
