<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Type;

abstract class TypeInfo {
    public function __construct(
        public readonly Type $type,
    ) {
    }

    /**
     * Whether the two immutable definitions describe the same binary layout.
     *
     * Exact definitions take the native object-comparison fast path. The
     * recursive fallback exists for definitions that describe the same layout
     * without being identical: TEXT/VARCHAR, two protocol names for the same
     * CQL type and encoding, and the properties below that a definition may
     * carry without them reaching the encoded bytes.
     *
     * Freezing is one of those: it decides how Cassandra stores a column, not
     * how a value of it is serialized, and the protocol never reports it - a
     * frozen<set<varchar>> column arrives as a plain set<varchar> in prepared
     * and rows metadata (see {@see \Cassandra\Response\StreamReader}). Letting
     * `isFrozen` decide would reject every explicitly frozen value object bound
     * to the column it belongs to.
     *
     * A UDT's keyspace and name are compared only where both sides name them,
     * for the same reason: {@see \Cassandra\Value\UDT::fromValue()} builds a
     * definition out of field types alone, and its unnamed type is the one the
     * server's named metadata describes as long as those fields line up.
     *
     * @internal
     */
    final public function isBinaryCompatibleWith(self $other): bool {
        if ($this == $other) {
            return true;
        }

        if (!self::typesAreBinaryCompatible($this->type, $other->type)) {
            return false;
        }

        return match (true) {
            $this instanceof SimpleTypeInfo && $other instanceof SimpleTypeInfo => true,
            $this instanceof CustomInfo && $other instanceof CustomInfo => $this->javaClassName === $other->javaClassName,
            $this instanceof ListCollectionInfo && $other instanceof ListCollectionInfo =>
                $this->valueType->isBinaryCompatibleWith($other->valueType),
            $this instanceof SetCollectionInfo && $other instanceof SetCollectionInfo =>
                $this->valueType->isBinaryCompatibleWith($other->valueType),
            $this instanceof MapCollectionInfo && $other instanceof MapCollectionInfo =>
                $this->keyType->isBinaryCompatibleWith($other->keyType)
                && $this->valueType->isBinaryCompatibleWith($other->valueType),
            $this instanceof TupleInfo && $other instanceof TupleInfo =>
                self::typeInfoListsAreBinaryCompatible($this->valueTypes, $other->valueTypes),
            $this instanceof UDTInfo && $other instanceof UDTInfo =>
                self::optionalNamesAreBinaryCompatible($this->keyspace, $other->keyspace)
                && self::optionalNamesAreBinaryCompatible($this->name, $other->name)
                && array_keys($this->valueTypes) === array_keys($other->valueTypes)
                && self::typeInfoListsAreBinaryCompatible(
                    array_values($this->valueTypes),
                    array_values($other->valueTypes),
                ),
            $this instanceof VectorInfo && $other instanceof VectorInfo =>
                $this->dimensions === $other->dimensions
                && $this->valueType->isBinaryCompatibleWith($other->valueType),
            default => false,
        };
    }

    /**
     * Read the optional frozen marker at the runtime-validation boundary used
     * by complex type definitions.
     *
     * @param array<mixed> $typeDefinition
     * @throws \Cassandra\Exception\TypeInfoException
     */
    final protected static function isFrozenFromTypeDefinition(array $typeDefinition): bool {
        if (!array_key_exists('isFrozen', $typeDefinition)) {
            return false;
        }

        if (!is_bool($typeDefinition['isFrozen'])) {
            throw new TypeInfoException(
                "Invalid type definition: 'isFrozen' must be a boolean",
                ExceptionCode::TYPEINFO_INVALID_IS_FROZEN->value,
                [
                    'actual_type' => get_debug_type($typeDefinition['isFrozen']),
                    'expected_type' => 'bool',
                ]
            );
        }

        return $typeDefinition['isFrozen'];
    }

    /**
     * Whether an optional name of a definition - one the client is not required
     * to state - stands in the way of the two describing the same type; an
     * unstated name on either side matches whatever the other one says.
     */
    protected static function optionalNamesAreBinaryCompatible(?string $left, ?string $right): bool {
        return $left === null || $right === null || $left === $right;
    }

    /**
     * @param list<TypeInfo> $left
     * @param list<TypeInfo> $right
     */
    protected static function typeInfoListsAreBinaryCompatible(array $left, array $right): bool {
        if (array_keys($left) !== array_keys($right)) {
            return false;
        }

        foreach ($left as $index => $typeInfo) {
            if (!$typeInfo->isBinaryCompatibleWith($right[$index])) {
                return false;
            }
        }

        return true;
    }

    protected static function typesAreBinaryCompatible(Type $left, Type $right): bool {
        return $left === $right
            || ($left === Type::TEXT && $right === Type::VARCHAR)
            || ($left === Type::VARCHAR && $right === Type::TEXT);
    }
}
