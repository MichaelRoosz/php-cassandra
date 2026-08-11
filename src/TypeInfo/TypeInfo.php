<?php

declare(strict_types=1);

namespace Cassandra\TypeInfo;

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
     * recursive fallback exists for TEXT/VARCHAR, two protocol names for the
     * same CQL type and encoding.
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
                $this->isFrozen === $other->isFrozen
                && $this->valueType->isBinaryCompatibleWith($other->valueType),
            $this instanceof SetCollectionInfo && $other instanceof SetCollectionInfo =>
                $this->isFrozen === $other->isFrozen
                && $this->valueType->isBinaryCompatibleWith($other->valueType),
            $this instanceof MapCollectionInfo && $other instanceof MapCollectionInfo =>
                $this->isFrozen === $other->isFrozen
                && $this->keyType->isBinaryCompatibleWith($other->keyType)
                && $this->valueType->isBinaryCompatibleWith($other->valueType),
            $this instanceof TupleInfo && $other instanceof TupleInfo =>
                self::typeInfoListsAreBinaryCompatible($this->valueTypes, $other->valueTypes),
            $this instanceof UDTInfo && $other instanceof UDTInfo =>
                $this->isFrozen === $other->isFrozen
                && $this->keyspace === $other->keyspace
                && $this->name === $other->name
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
