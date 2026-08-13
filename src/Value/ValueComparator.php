<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;

/**
 * Compare serialized values in the order their declared CQL type uses.
 *
 * Cassandra requires map keys and set elements to be serialized in comparator
 * order. The order is not the byte order for signed numbers, and PHP's own
 * ordering is not enough for values supplied as ValueBase objects, decimals or
 * nested frozen values, so collection encoders compare the wire values they
 * have already validated and encoded.
 *
 * @internal
 */
final class ValueComparator {
    private const SIGNED_INT_SHIFT_BIT_SIZE = (PHP_INT_SIZE * 8) - 32;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public static function compare(TypeInfo $typeInfo, string $left, string $right): int {
        if ($left === $right) {
            return 0;
        }

        return match ($typeInfo->type) {
            Type::BIGINT,
            Type::COUNTER,
            Type::INT,
            Type::SMALLINT,
            Type::TIME,
            Type::TIMESTAMP,
            Type::TINYINT => self::compareSignedBinary($left, $right),

            Type::BOOLEAN,
            Type::DATE,
            Type::DURATION => strcmp($left, $right),

            Type::DECIMAL => self::compareDecimal(
                Decimal::fromBinary($left)?->getValue() ?? '0',
                Decimal::fromBinary($right)?->getValue() ?? '0',
            ),

            Type::DOUBLE => self::compareFloat($left, $right, false),
            Type::FLOAT => self::compareFloat($left, $right, true),
            Type::INET => strcmp($left, $right),
            Type::TIMEUUID => self::compareTimeUuid($left, $right, true),
            Type::UUID => self::compareUuid($left, $right),
            Type::VARINT => self::compareDecimal(
                Varint::fromBinary($left)?->asString() ?? '0',
                Varint::fromBinary($right)?->asString() ?? '0',
            ),

            Type::LIST => self::compareCollection($typeInfo, $left, $right),
            Type::MAP => self::compareMap($typeInfo, $left, $right),
            Type::SET => self::compareCollection($typeInfo, $left, $right),
            Type::TUPLE => self::compareTuple($typeInfo, $left, $right),
            Type::UDT => self::compareUdt($typeInfo, $left, $right),

            // Custom comparators are defined by server-side Java classes and
            // vectors have their own version-dependent component framing. The
            // library exposes custom values as blobs and Cassandra does not
            // permit vectors as collection keys/elements, so unsigned byte
            // order is the only meaningful local order for these two types.
            Type::ASCII,
            Type::BLOB,
            Type::CUSTOM,
            Type::TEXT,
            Type::VARCHAR,
            Type::VECTOR => strcmp($left, $right),
        };
    }

    /**
     * Refuse a value whose binary is not the width its type is serialized at.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function assertFixedWidth(string $left, string $right, int $width): void {

        if (strlen($left) === $width && strlen($right) === $width) {
            return;
        }

        throw new ValueException(
            'Cannot compare values of a fixed-width type whose binary is not that width',
            ExceptionCode::VALUE_COMPARATOR_INVALID_LENGTH->value,
            [
                'expected_length' => $width,
                'left_length' => strlen($left),
                'right_length' => strlen($right),
            ]
        );
    }

    /**
     * Refuse a read that would run past the end of the binary it walks.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function assertHasBytes(string $binary, int $offset, int $length, string $field): void {

        if ($length >= 0 && $offset + $length <= strlen($binary)) {
            return;
        }

        throw new ValueException(
            'Cannot compare values: the serialized value ends before its ' . $field . ' does',
            ExceptionCode::VALUE_COMPARATOR_TRUNCATED_BINARY->value,
            [
                'field' => $field,
                'offset' => $offset,
                'required_length' => $length,
                'binary_length' => strlen($binary),
            ]
        );
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function collectionCount(string $binary): int {
        self::assertHasBytes($binary, 0, 4, 'element count');

        /** @var array{1: int} $count */
        $count = unpack('N', substr($binary, 0, 4));

        return $count[1];
    }

    /**
     * @return list<string>
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function collectionValues(string $binary, int $valuesPerEntry = 1): array {
        $count = self::collectionCount($binary) * $valuesPerEntry;
        $offset = 4;
        $values = [];
        for ($index = 0; $index < $count; ++$index) {
            $values[] = self::readValue($binary, $offset);
        }

        return $values;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareCollection(TypeInfo $typeInfo, string $left, string $right): int {
        if ($typeInfo instanceof ListCollectionInfo || $typeInfo instanceof SetCollectionInfo) {
            return self::compareSequences(
                self::collectionValues($left),
                self::collectionValues($right),
                $typeInfo->valueType,
            );
        }

        return strcmp($left, $right);
    }

    private static function compareDecimal(string $left, string $right): int {
        $leftNegative = str_starts_with($left, '-');
        $rightNegative = str_starts_with($right, '-');
        if ($leftNegative !== $rightNegative) {
            return $leftNegative ? -1 : 1;
        }

        if ($leftNegative) {
            $left = substr($left, 1);
            $right = substr($right, 1);
        }

        [$leftInteger, $leftFraction] = self::decimalParts($left);
        [$rightInteger, $rightFraction] = self::decimalParts($right);

        $comparison = strlen($leftInteger) <=> strlen($rightInteger);
        if ($comparison === 0) {
            $comparison = strcmp($leftInteger, $rightInteger);
        }
        if ($comparison === 0) {
            $fractionLength = max(strlen($leftFraction), strlen($rightFraction));
            $comparison = strcmp(
                str_pad($leftFraction, $fractionLength, '0'),
                str_pad($rightFraction, $fractionLength, '0'),
            );
        }

        return $leftNegative ? -$comparison : $comparison;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareFloat(string $left, string $right, bool $singlePrecision): int {
        self::assertFixedWidth($left, $right, $singlePrecision ? 4 : 8);

        /** @var array{1: float} $leftUnpacked */
        $leftUnpacked = unpack($singlePrecision ? 'G' : 'E', $left);
        /** @var array{1: float} $rightUnpacked */
        $rightUnpacked = unpack($singlePrecision ? 'G' : 'E', $right);
        $leftValue = $leftUnpacked[1];
        $rightValue = $rightUnpacked[1];

        if (is_nan($leftValue)) {
            return is_nan($rightValue) ? 0 : 1;
        }
        if (is_nan($rightValue)) {
            return -1;
        }
        if ($leftValue < $rightValue) {
            return -1;
        }
        if ($leftValue > $rightValue) {
            return 1;
        }

        // Java Float.compare()/Double.compare(), which Cassandra's numeric
        // types use, distinguish negative zero from positive zero.
        return self::compareSignedBinary($left, $right);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareMap(TypeInfo $typeInfo, string $left, string $right): int {
        if (!$typeInfo instanceof MapCollectionInfo) {
            return strcmp($left, $right);
        }

        return self::compareSequences(
            self::collectionValues($left, 2),
            self::collectionValues($right, 2),
            [],
            [$typeInfo->keyType, $typeInfo->valueType],
        );
    }

    /**
     * @param list<?string> $left
     * @param list<?string> $right
     * @param TypeInfo|list<?TypeInfo> $types the one type every position shares
     * — a list's or set's element type — or one type per position, as a tuple
     * and a UDT have. An empty list where $alternatingTypes settles it instead.
     * @param ?array{TypeInfo, TypeInfo} $alternatingTypes the key and value
     * types of a map, whose entries arrive as one flat alternating sequence
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareSequences(
        array $left,
        array $right,
        TypeInfo|array $types,
        ?array $alternatingTypes = null,
    ): int {
        $common = min(count($left), count($right));
        for ($index = 0; $index < $common; ++$index) {
            $leftValue = $left[$index];
            $rightValue = $right[$index];
            if ($leftValue === null || $rightValue === null) {
                if ($leftValue === $rightValue) {
                    continue;
                }

                return $leftValue === null ? -1 : 1;
            }

            if ($alternatingTypes !== null) {
                $type = $alternatingTypes[$index % 2];
            } elseif ($types instanceof TypeInfo) {
                $type = $types;
            } else {
                $type = $types[$index] ?? null;
            }

            $comparison = $type === null
                ? strcmp($leftValue, $rightValue)
                : self::compare($type, $leftValue, $rightValue);
            if ($comparison !== 0) {
                return $comparison;
            }
        }

        return count($left) <=> count($right);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareSignedBinary(string $left, string $right): int {
        self::assertHasBytes($left, 0, 1, 'sign byte');
        self::assertHasBytes($right, 0, 1, 'sign byte');

        $leftNegative = (ord($left[0]) & 0x80) !== 0;
        $rightNegative = (ord($right[0]) & 0x80) !== 0;

        if ($leftNegative !== $rightNegative) {
            return $leftNegative ? -1 : 1;
        }

        return strcmp($left, $right);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareTimeUuid(string $left, string $right, bool $signedTail): int {
        self::assertFixedWidth($left, $right, 16);

        // UUID v1 stores its timestamp least-significant group first. Compare
        // the high 12, middle 16 and low 32 bits in chronological order, then
        // the clock sequence and node bytes as the tie-breaker.
        foreach ([[6, 2], [4, 2], [0, 4]] as [$offset, $length]) {
            $leftPart = substr($left, $offset, $length);
            $rightPart = substr($right, $offset, $length);
            if ($offset === 6) {
                $leftPart[0] = chr(ord($leftPart[0]) & 0x0f);
                $rightPart[0] = chr(ord($rightPart[0]) & 0x0f);
            }

            $comparison = strcmp($leftPart, $rightPart);
            if ($comparison !== 0) {
                return $comparison;
            }
        }

        if (!$signedTail) {
            return strcmp(substr($left, 8), substr($right, 8));
        }

        // TimeUUIDType retains its historical signed-byte comparison for the
        // clock-sequence/node half (CASSANDRA-8730), unlike UUIDType's unsigned
        // comparison of the same bytes.
        for ($offset = 8; $offset < 16; ++$offset) {
            $leftByte = ord($left[$offset]);
            $rightByte = ord($right[$offset]);
            $leftByte = $leftByte >= 0x80 ? $leftByte - 0x100 : $leftByte;
            $rightByte = $rightByte >= 0x80 ? $rightByte - 0x100 : $rightByte;
            $comparison = $leftByte <=> $rightByte;
            if ($comparison !== 0) {
                return $comparison;
            }
        }

        return 0;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareTuple(TypeInfo $typeInfo, string $left, string $right): int {
        if (!$typeInfo instanceof TupleInfo) {
            return strcmp($left, $right);
        }

        return self::compareSequences(
            self::tupleValues($left, count($typeInfo->valueTypes)),
            self::tupleValues($right, count($typeInfo->valueTypes)),
            $typeInfo->valueTypes,
        );
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareUdt(TypeInfo $typeInfo, string $left, string $right): int {
        if (!$typeInfo instanceof UDTInfo) {
            return strcmp($left, $right);
        }

        $types = array_values($typeInfo->valueTypes);

        return self::compareSequences(
            self::tupleValues($left, count($types)),
            self::tupleValues($right, count($types)),
            $types,
        );
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function compareUuid(string $left, string $right): int {
        self::assertFixedWidth($left, $right, 16);

        $leftVersion = ord($left[6]) >> 4;
        $rightVersion = ord($right[6]) >> 4;
        $versionComparison = $leftVersion <=> $rightVersion;
        if ($versionComparison !== 0) {
            return $versionComparison;
        }

        return $leftVersion === 1
            ? self::compareTimeUuid($left, $right, false)
            : strcmp($left, $right);
    }

    /**
     * @return array{string, string}
     */
    private static function decimalParts(string $value): array {
        $point = strpos($value, '.');
        if ($point === false) {
            return [ltrim($value, '0') ?: '0', ''];
        }

        return [
            ltrim(substr($value, 0, $point), '0') ?: '0',
            rtrim(substr($value, $point + 1), '0'),
        ];
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function readValue(string $binary, int &$offset): string {

        self::assertHasBytes($binary, $offset, 4, 'element length');

        /** @var array{1: int} $length */
        $length = unpack('N', substr($binary, $offset, 4));
        $offset += 4;

        // length can be negative, but unpack() returns an unsigned int,
        // so we need to shift it back to a signed int.
        $elementLength = $length[1]
            << self::SIGNED_INT_SHIFT_BIT_SIZE
            >> self::SIGNED_INT_SHIFT_BIT_SIZE;

        self::assertHasBytes($binary, $offset, $elementLength, 'element');

        $value = substr($binary, $offset, $elementLength);
        $offset += $elementLength;

        return $value;
    }

    /**
     * @return list<?string>
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function tupleValues(string $binary, int $count): array {
        $offset = 0;
        $values = [];
        for ($index = 0; $index < $count; ++$index) {
            self::assertHasBytes($binary, $offset, 4, 'element length');

            /** @var array{1: int} $length */
            $length = unpack('N', substr($binary, $offset, 4));
            $offset += 4;

            // length can be negative, but unpack() returns an unsigned int,
            // so we need to shift it back to a signed int.
            $elementLength = $length[1]
                << self::SIGNED_INT_SHIFT_BIT_SIZE
                >> self::SIGNED_INT_SHIFT_BIT_SIZE;

            if ($elementLength < 0) {
                $values[] = null;

                continue;
            }

            self::assertHasBytes($binary, $offset, $elementLength, 'element');

            $values[] = substr($binary, $offset, $elementLength);
            $offset += $elementLength;
        }

        return $values;
    }
}
