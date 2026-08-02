<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\ValueFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\TypeInfo;

final class MapCollection extends ValueReadableWithoutLength {
    private MapCollectionInfo $typeInfo;

    /**
     * @var array<mixed> $value
     */
    private readonly array $value;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(
        array $value,
        MapCollectionInfo $typeInfo,
    ) {
        $this->typeInfo = $typeInfo;
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        return self::fromStream(new StreamReader($binary), typeInfo: $typeInfo, valueEncodeConfig: $valueEncodeConfig);
    }

    /**
     * @param mixed $value
     * 
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_array($value)) {
            throw new ValueException('Invalid map value; expected array', ExceptionCode::VALUE_MAP_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_MAP_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof MapCollectionInfo) {
            throw new ValueException('Invalid type info, MapCollectionInfo expected', ExceptionCode::VALUE_MAP_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    final public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        if ($typeInfo === null) {
            throw new ValueException('typeInfo is required', ExceptionCode::VALUE_MAP_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof MapCollectionInfo) {
            throw new ValueException('Invalid type info, MapCollectionInfo expected', ExceptionCode::VALUE_MAP_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $valueEncodeConfig ??= ValueEncodeConfig::default();

        $map = [];
        $count = $stream->readInt();

        /** @psalm-suppress MixedAssignment */
        for ($i = 0; $i < $count; ++$i) {
            $key = $stream->readValue($typeInfo->keyType, $valueEncodeConfig);

            // PHP array keys must be int|string. Scalar key types that decode to
            // other PHP types are converted to a string representation (a float
            // key must not be left as-is — PHP would silently truncate it to
            // int); everything else cannot be represented as an array key.
            if (is_float($key)) {
                $key = (string) $key;
            } elseif (is_bool($key)) {
                $key = $key ? 1 : 0;
            }

            if (!is_string($key) && !is_int($key)) {
                throw new ValueException(
                    message: 'Invalid map key type; expected string|int',
                    code: ExceptionCode::VALUE_MAP_INVALID_MAP_KEY_TYPE->value,
                    context: [
                        'method' => __METHOD__,
                        'key_php_type' => gettype($key),
                        'offset' => $stream->pos(),
                    ]
                );
            }
            $map[$key] = $stream->readValue($typeInfo->valueType, $valueEncodeConfig);
        }

        return new static($map, typeInfo: $typeInfo);
    }

    /**
     * @param array<mixed> $value
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $keyDefinition
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $valueDefinition
     *
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    final public static function fromValue(
        array $value,
        Type|array $keyDefinition,
        Type|array $valueDefinition,
        bool $isFrozen = false,
    ): static {

        return new static($value, MapCollectionInfo::fromTypeDefinition([
            'type' => Type::MAP,
            'keyType' => $keyDefinition,
            'valueType' => $valueDefinition,
            'isFrozen' => $isFrozen,
        ]));
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    public function getBinary(): string {

        $binary = pack('N', count($this->value));

        /** @var ValueBase|mixed $val */
        foreach ($this->value as $key => $val) {
            $keyPacked = ValueFactory::getBinaryByTypeInfo($this->typeInfo->keyType, self::keyAsNativeValue($this->typeInfo->keyType, $key));

            $valuePacked = $val instanceof ValueBase
                ? $val->getBinary()
                : ValueFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val);

            $binary .= pack('N', strlen($keyPacked)) . $keyPacked;
            $binary .= pack('N', strlen($valuePacked)) . $valuePacked;
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::MAP;
    }

    /**
     * @return array<mixed> $value
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return true;
    }

    /**
     * Undo the array-key coercion a map key went through on its way into the
     * PHP array, so that it can be encoded as the type it belongs to.
     *
     * A PHP array key is an int or a string and nothing else, so a boolean key
     * is already an int by the time anything here sees it: PHP itself folds
     * true and false to 1 and 0 on assignment, and {@see self::fromStream()}
     * spells the same coercion out for the keys it decodes. Boolean is the one
     * key type that cannot survive that on its own —
     * {@see Boolean::fromMixedValue()} takes a bool and refuses everything else
     * — so without this a `map<boolean, …>` could not be encoded at all,
     * whether it was built by hand or read off the wire a moment earlier.
     *
     * Only the two ints a boolean can have become one are converted. Anything
     * else at a boolean key is not a coerced bool but a key that was never
     * valid, and is left to be refused where every other bad key is.
     *
     * The other key types need nothing here: the numeric ones take the int PHP
     * left them as, and the two that {@see self::fromStream()} turns into
     * strings — float and double, which PHP would otherwise truncate to int —
     * are parsed back from that string by their own value classes.
     */
    private static function keyAsNativeValue(TypeInfo $keyType, int|string $key): int|string|bool {

        if ($keyType->type === Type::BOOLEAN && ($key === 0 || $key === 1)) {
            return $key === 1;
        }

        return $key;
    }
}
