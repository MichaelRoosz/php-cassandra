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
        $maximumCount = intdiv(max(0, $stream->remainingLength()), 8);
        if ($count < 0 || $count > $maximumCount) {
            throw new ValueException(
                'Map entry count does not fit in the available value data',
                ExceptionCode::VALUE_MAP_INVALID_VALUE_TYPE->value,
                ['count' => $count, 'maximum_count' => $maximumCount]
            );
        }

        /** @psalm-suppress MixedAssignment */
        for ($i = 0; $i < $count; ++$i) {
            $key = $stream->readValue($typeInfo->keyType, $valueEncodeConfig);

            // PHP array keys must be int|string. Scalar key types that decode to
            // other PHP types are converted to a string representation (a float
            // key must not be left as-is — PHP would silently truncate it to
            // int); everything else cannot be represented as an array key.
            if (is_float($key)) {
                $key = self::floatAsMapKey($key);
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
     * Represent a floating-point map key without losing enough precision for
     * two distinct wire values to collapse onto the same PHP array key.
     *
     * Seventeen significant decimal digits are sufficient to round-trip every
     * IEEE-754 double, and therefore every single-precision float as well.
     * sprintf() honours LC_NUMERIC, while the string is also fed back to PHP's
     * locale-independent float parser by {@see self::keyAsNativeValue()}, so
     * normalise the locale's decimal separator before exposing the key.
     */
    private static function floatAsMapKey(float $key): string {
        if (is_nan($key)) {
            return 'NAN';
        }

        if ($key === INF) {
            return 'INF';
        }

        if ($key === -INF) {
            return '-INF';
        }

        $formatted = sprintf('%.17g', $key);
        $decimalPoint = localeconv()['decimal_point'];

        if ($decimalPoint !== '' && $decimalPoint !== '.') {
            $formatted = str_replace($decimalPoint, '.', $formatted);
        }

        return $formatted;
    }

    /**
     * Undo the array-key coercion a map key went through on its way into the
     * PHP array, so that it can be encoded as the type it belongs to.
     *
     * A PHP array key is an int or a string and nothing else, and PHP folds
     * both booleans and canonical decimal integer strings into ints on
     * assignment. Two families of key type therefore cannot survive being put
     * into the array at all, and both are spelled back out here.
     *
     * A boolean key is already an int by the time anything here sees it: PHP
     * itself folds true and false to 1 and 0, and {@see self::fromStream()}
     * spells the same coercion out for the keys it decodes.
     * {@see Boolean::fromMixedValue()} takes a bool and refuses everything else,
     * so without this a `map<boolean, …>` could not be encoded at all. Only the
     * two ints a boolean can have become one are converted; anything else at a
     * boolean key is not a coerced bool but a key that was never valid, and is
     * left to be refused where every other bad key is.
     *
     * A string-valued key — ascii, text, varchar, blob and custom — goes the
     * same way whenever it spells a canonical integer: `['123' => …]` is an int
     * key the moment the literal is written, and so is the "123" that
     * {@see self::fromStream()} decoded a moment earlier. Their value classes
     * take a string and nothing else, so without this a `map<text, …>` with
     * such a key could neither be built by hand nor written back after being
     * read. The fold is lossless in exactly the cases it happens in — PHP only
     * folds a key that is already the canonical decimal spelling of the int —
     * so this restores the original key rather than guessing at one.
     * {@see \Cassandra\Request\Request::encodeQueryValuesAsBinary()} does the
     * same for bind marker names, which are array keys for the same reason.
     *
     * uuid and timeuuid are among them, though only under
     * {@see \Cassandra\Value\EncodeOption\UuidEncodeOption::AS_BINARY}: the raw
     * 16 bytes a key decodes to there are a string, and sixteen digits are a
     * key PHP folds like any other. {@see Uuid::__construct()} reads that raw
     * form back. The canonical string form the default option produces is never
     * an integer spelling, so nothing happens to it.
     *
     * Float and double — which {@see self::fromStream()} turn into strings
     * precisely so PHP cannot truncate them to int — are parsed back by their
     * value classes. Only the non-finite values are restored here, since their
     * canonical keys are deliberately not numeric strings. Leaving every
     * other string alone also preserves the value classes' validation for a
     * caller-supplied invalid key. The remaining numeric types take the int PHP
     * left them as.
     */
    private static function keyAsNativeValue(TypeInfo $keyType, int|string $key): int|string|bool|float {

        if ($keyType->type === Type::FLOAT || $keyType->type === Type::DOUBLE) {
            return match ($key) {
                'NAN' => NAN,
                'INF' => INF,
                '-INF' => -INF,
                default => $key,
            };
        }

        if ($keyType->type === Type::BOOLEAN && ($key === 0 || $key === 1)) {
            return $key === 1;
        }

        if (is_int($key)) {
            return match ($keyType->type) {
                Type::ASCII,
                Type::BLOB,
                Type::CUSTOM,
                Type::TEXT,
                Type::TIMEUUID,
                Type::UUID,
                Type::VARCHAR => (string) $key,

                default => $key,
            };
        }

        return $key;
    }
}
