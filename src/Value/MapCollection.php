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
use Cassandra\Value\EncodeOption\DateEncodeOption;
use Cassandra\Value\EncodeOption\DurationEncodeOption;
use Cassandra\Value\EncodeOption\MapEncodeOption;
use Cassandra\Value\EncodeOption\TimeEncodeOption;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;

final class MapCollection extends ValueReadableWithoutLength implements ValueWithMultipleEncodings {
    /**
     * @var list<MapEntry> $entries
     */
    private array $entries;
    private MapCollectionInfo $typeInfo;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(
        array $value,
        MapCollectionInfo $typeInfo,
    ) {
        $this->typeInfo = $typeInfo;
        $this->entries = [];

        /** @var mixed $entryValue */
        foreach ($value as $key => $entryValue) {
            $this->entries[] = new MapEntry(
                self::keyAsNativeValue($typeInfo->keyType, $key),
                $entryValue,
            );
        }
    }

    /**
     * @return array<mixed>|$this
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function asConfigured(ValueEncodeConfig $valueEncodeConfig): array|self {
        if ($valueEncodeConfig->mapEncodeOption === MapEncodeOption::AS_MAP_COLLECTION) {
            return $this;
        }

        if ($valueEncodeConfig->mapEncodeOption === MapEncodeOption::AUTO) {
            if (
                !self::keyTypeSupportsArray($this->typeInfo->keyType, $valueEncodeConfig)
                || !$this->entriesCanBeProjectedToArray()
            ) {
                return $this;
            }
        }

        return $this->toArray($valueEncodeConfig);
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

        return self::fromStream(
            new StreamReader($binary),
            length: strlen($binary),
            typeInfo: $typeInfo,
            valueEncodeConfig: $valueEncodeConfig
        );
    }

    /**
     * Construct a map whose keys cannot necessarily be represented by PHP
     * array keys, such as tuples, frozen UDTs or configured temporal objects.
     *
     * @param list<MapEntry> $entries
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $keyDefinition
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $valueDefinition
     *
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    final public static function fromEntries(
        array $entries,
        Type|array $keyDefinition,
        Type|array $valueDefinition,
        bool $isFrozen = false,
    ): static {
        if (!self::isListInput($entries)) {
            throw new ValueException(
                'Invalid map entries; expected a list of Cassandra\\Value\\MapEntry objects',
                ExceptionCode::VALUE_MAP_INVALID_VALUE_TYPE->value,
                ['entry_keys' => array_keys($entries)]
            );
        }

        foreach ($entries as $index => $entry) {
            if (!$entry instanceof MapEntry) {
                throw new ValueException(
                    'Invalid map entry; expected Cassandra\\Value\\MapEntry',
                    ExceptionCode::VALUE_MAP_INVALID_VALUE_TYPE->value,
                    [
                        'index' => $index,
                        'entry_type' => get_debug_type($entry),
                    ]
                );
            }

            if ($entry->key === null) {
                throw new ValueException(
                    'A map key cannot be null',
                    ExceptionCode::VALUE_MAP_INVALID_MAP_KEY_TYPE->value,
                    [
                        'index' => $index,
                        'key_php_type' => get_debug_type($entry->key),
                    ]
                );
            }
        }

        $typeInfo = MapCollectionInfo::fromTypeDefinition([
            'type' => Type::MAP,
            'keyType' => $keyDefinition,
            'valueType' => $valueDefinition,
            'isFrozen' => $isFrozen,
        ]);

        /** @var list<MapEntry> $entries */
        return self::fromDecodedEntries($entries, $typeInfo);
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

        $entries = [];
        $count = $stream->readInt();
        $maximumCount = self::maximumCollectionEntryCount($stream->remainingLength(), $length, 8);
        if ($count < 0 || $count > $maximumCount) {
            throw new ValueException(
                'Map entry count does not fit in the available value data',
                ExceptionCode::VALUE_MAP_INVALID_VALUE_TYPE->value,
                [
                    'count' => $count,
                    'maximum_count' => $maximumCount,
                    'declared_length' => $length,
                    'remaining_length' => $stream->remainingLength(),
                ]
            );
        }

        /** @psalm-suppress MixedAssignment */
        for ($i = 0; $i < $count; ++$i) {
            $key = $stream->readValue($typeInfo->keyType, $valueEncodeConfig);
            if ($key === null) {
                throw new ValueException(
                    'A map key cannot be null',
                    ExceptionCode::VALUE_MAP_INVALID_MAP_KEY_TYPE->value,
                    [
                        'index' => $i,
                        'key_type' => $typeInfo->keyType->type->name,
                        'offset' => $stream->pos(),
                    ]
                );
            }

            $value = $stream->readValue($typeInfo->valueType, $valueEncodeConfig);
            if ($value === null) {
                throw new ValueException(
                    'A map value decoded from a response cannot be null',
                    ExceptionCode::VALUE_MAP_NULL_VALUE->value,
                    [
                        'index' => $i,
                        'key' => $key,
                        'value_type' => $typeInfo->valueType->type->name,
                        'offset' => $stream->pos(),
                    ]
                );
            }

            $entries[] = new MapEntry($key, $value);
        }

        return self::fromDecodedEntries($entries, $typeInfo);
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

        /**
         * @var list<array{
         *   key: mixed,
         *   keyBinary: string,
         *   valueBinary: string
         * }> $entries
         */
        $entries = [];

        foreach ($this->entries as $entry) {
            /** @psalm-suppress MixedAssignment */
            $key = $entry->key;
            /** @psalm-suppress MixedAssignment */
            $val = $entry->value;

            if ($val === null) {
                throw new ValueException(
                    'A map value cannot be null; CQL has no null inside a collection, so remove the entry instead',
                    ExceptionCode::VALUE_MAP_NULL_VALUE->value,
                    [
                        'key' => $key,
                        'value_type' => $this->typeInfo->valueType->type->name,
                    ]
                );
            }

            if ($key instanceof ValueBase) {
                if (!$key->isBinaryCompatibleWith($this->typeInfo->keyType)) {
                    throw new ValueException(
                        'Map key value object does not match the declared key type',
                        ExceptionCode::VALUE_MAP_INVALID_MAP_KEY_TYPE->value,
                        [
                            'expected_type' => $this->typeInfo->keyType->type->name,
                            'actual_type' => $key->getType()->name,
                        ]
                    );
                }

                $keyPacked = $key->getBinary();
            } else {
                $keyPacked = ValueFactory::getBinaryByTypeInfo($this->typeInfo->keyType, $key);
            }

            $valuePacked = ValueFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val);

            $entries[] = [
                'key' => $key,
                'keyBinary' => $keyPacked,
                'valueBinary' => $valuePacked,
            ];
        }

        usort($entries, $this->compareEntries(...));

        $binary = pack('N', count($entries));
        $previous = null;
        foreach ($entries as $entry) {
            if (
                $previous !== null
                && ValueComparator::compare($this->typeInfo->keyType, $previous['keyBinary'], $entry['keyBinary']) === 0
            ) {
                throw new ValueException(
                    'A map cannot contain the same CQL key more than once',
                    ExceptionCode::VALUE_MAP_DUPLICATE_KEY->value,
                    [
                        'first_key' => $previous['key'],
                        'duplicate_key' => $entry['key'],
                        'key_type' => $this->typeInfo->keyType->type->name,
                    ]
                );
            }

            $binary .= pack('N', strlen($entry['keyBinary'])) . $entry['keyBinary'];
            $binary .= pack('N', strlen($entry['valueBinary'])) . $entry['valueBinary'];
            $previous = $entry;
        }

        return $binary;
    }

    /**
     * @return list<MapEntry>
     */
    public function getEntries(): array {
        return $this->entries;
    }

    #[\Override]
    public function getType(): Type {
        return Type::MAP;
    }

    /**
     * @return array<mixed> $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function getValue(): array {
        return $this->toArray(ValueEncodeConfig::default());
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return true;
    }

    #[\Override]
    protected function binaryTypeInfo(): TypeInfo {
        return $this->typeInfo;
    }

    /**
     * @param array{key: mixed, keyBinary: string, valueBinary: string} $left
     * @param array{key: mixed, keyBinary: string, valueBinary: string} $right
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private function compareEntries(array $left, array $right): int {
        return ValueComparator::compare(
            $this->typeInfo->keyType,
            $left['keyBinary'],
            $right['keyBinary'],
        );
    }

    private function entriesCanBeProjectedToArray(): bool {
        $keys = [];
        foreach ($this->entries as $entry) {
            $key = self::entryKeyAsArrayKey($entry->key);
            if ($key === null || array_key_exists($key, $keys)) {
                return false;
            }

            $keys[$key] = true;
        }

        return true;
    }

    private static function entryKeyAsArrayKey(mixed $key): int|string|null {
        if (is_float($key)) {
            return self::floatAsMapKey($key);
        }

        if (is_bool($key)) {
            return $key ? 1 : 0;
        }

        return is_int($key) || is_string($key) ? $key : null;
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
     * Everything %.17g emits besides that separator is a digit, a sign or the
     * exponent marker, so whatever else turns up is the separator itself —
     * however many bytes the locale happens to spell it with.
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

        return preg_replace('/[^0-9eE+-]+/', '.', $formatted) ?? $formatted;
    }

    /**
     * @param list<MapEntry> $entries
     */
    private static function fromDecodedEntries(array $entries, MapCollectionInfo $typeInfo): static {
        $map = new static([], $typeInfo);
        $map->entries = $entries;

        return $map;
    }

    /**
     * Validate the runtime input without widening the public fromEntries()
     * contract merely to make defensive validation visible to analyzers.
     *
     * @param array<array-key, mixed> $entries
     */
    private static function isListInput(array $entries): bool {
        return array_is_list($entries);
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
     * A boolean key is already an int by the time an associative-array input
     * reaches this method: PHP itself folds true and false to 1 and 0.
     * {@see Boolean::fromMixedValue()} takes a bool and refuses everything else,
     * so without this a `map<boolean, …>` could not be encoded at all. Only the
     * two ints a boolean can have become one are converted; anything else at a
     * boolean key is not a coerced bool but a key that was never valid, and is
     * left to be refused where every other bad key is.
     *
     * A string-valued key — ascii, text, varchar, blob and custom — goes the
     * same way whenever it spells a canonical integer: `['123' => …]` is an int
     * key the moment the literal is written. Their value classes take a string
     * and nothing else, so without this a `map<text, …>` with such a key could
     * not be built from the associative-array API. The fold is lossless in
     * exactly the cases it happens in — PHP only folds a key that is already
     * the canonical decimal spelling of the int — so this restores the
     * original key rather than guessing at one.
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
     * Float and double keys supplied through a PHP associative array have to be
     * strings so PHP cannot truncate them to int. They are parsed back by their
     * value classes. Only the non-finite values are restored here, since their
     * canonical keys are deliberately not numeric strings. Leaving every other
     * string alone also preserves the value classes' validation for a
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

    private static function keyTypeSupportsArray(TypeInfo $keyType, ValueEncodeConfig $valueEncodeConfig): bool {
        return match ($keyType->type) {
            Type::DATE => $valueEncodeConfig->dateEncodeOption !== DateEncodeOption::AS_DATETIME_IMMUTABLE,
            Type::DURATION => $valueEncodeConfig->durationEncodeOption === DurationEncodeOption::AS_STRING
                || $valueEncodeConfig->durationEncodeOption === DurationEncodeOption::AS_DATEINTERVAL_STRING,
            Type::TIME => $valueEncodeConfig->timeEncodeOption !== TimeEncodeOption::AS_DATETIME_IMMUTABLE,
            Type::TIMESTAMP => $valueEncodeConfig->timestampEncodeOption !== TimestampEncodeOption::AS_DATETIME_IMMUTABLE,
            Type::LIST, Type::MAP, Type::SET, Type::TUPLE, Type::UDT, Type::VECTOR => false,
            default => true,
        };
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private function toArray(ValueEncodeConfig $valueEncodeConfig): array {
        if (!self::keyTypeSupportsArray($this->typeInfo->keyType, $valueEncodeConfig)) {
            throw new ValueException(
                'The configured map key representation cannot be used as a PHP array key; use MapEncodeOption::AUTO or AS_MAP_COLLECTION',
                ExceptionCode::VALUE_MAP_CANNOT_CONVERT_TO_ARRAY->value,
                [
                    'key_type' => $this->typeInfo->keyType->type->name,
                    'map_encode_option' => $valueEncodeConfig->mapEncodeOption->value,
                ]
            );
        }

        $value = [];
        foreach ($this->entries as $index => $entry) {
            $key = self::entryKeyAsArrayKey($entry->key);
            if ($key === null) {
                throw new ValueException(
                    'A map key cannot be represented as a PHP array key; use MapEncodeOption::AUTO or AS_MAP_COLLECTION',
                    ExceptionCode::VALUE_MAP_CANNOT_CONVERT_TO_ARRAY->value,
                    [
                        'index' => $index,
                        'key_type' => $this->typeInfo->keyType->type->name,
                        'key_php_type' => get_debug_type($entry->key),
                    ]
                );
            }

            if (array_key_exists($key, $value)) {
                throw new ValueException(
                    'Distinct CQL map keys collapse to the same PHP array key; use MapEncodeOption::AUTO or AS_MAP_COLLECTION',
                    ExceptionCode::VALUE_MAP_CANNOT_CONVERT_TO_ARRAY->value,
                    [
                        'index' => $index,
                        'key_type' => $this->typeInfo->keyType->type->name,
                        'array_key' => $key,
                    ]
                );
            }

            /** @psalm-suppress MixedAssignment */
            $value[$key] = $entry->value;
        }

        return $value;
    }
}
