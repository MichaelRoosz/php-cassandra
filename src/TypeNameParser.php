<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeNameParserException;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\VectorInfo;

/**
 * Parser for the Java class name strings Cassandra uses to describe column types
 * (org.apache.cassandra.db.marshal.*).
 *
 * The grammar is ASCII-only: Cassandra's own TypeParser.isIdentifierChar() accepts
 * exactly [0-9a-zA-Z-+._&], plus "(", ")", ",", ":" as structure and space/tab/newline
 * as separators. Non-ASCII UDT type and field names are hex-encoded precisely so that
 * they can round-trip through that grammar. This parser is therefore byte-oriented and
 * needs no mbstring; the only place arbitrary bytes can appear is after hex-decoding a
 * UDT name, which decodeUdtName() validates as UTF-8.
 */
final class TypeNameParser {
    /**
     * How deeply a type string may nest before it is refused.
     *
     * A parameterised type is parsed by parsing its parameters, so this descends
     * once per level of nesting — driven entirely by a string that came from the
     * node. Nothing in the grammar bounds that, and PHP has no catchable stack
     * overflow: a deep enough type string would take the process down rather
     * than raise. The same reasoning, and the same limit, as
     * {@see \Cassandra\Response\StreamReader::MAX_TYPE_NESTING_DEPTH}, which
     * bounds the wire form of the same thing.
     */
    private const MAX_NESTING_DEPTH = 64;

    /**
     * How many levels of nesting {@see self::parse()} is currently inside.
     *
     * Kept here rather than passed down because the descent goes back through
     * the public parse() at every level: the parameter parsers hand their
     * parameters to it, so a counter it maintains itself bounds all of them
     * without every one of them having to carry a depth. It is unwound in a
     * finally, so a type string that is refused part way down leaves this parser
     * usable.
     */
    private int $nestingDepth = 0;

    /**
     * @var ?array<string, \Cassandra\Type>
     */
    private static ?array $simpleTypeMap = null;

    /**
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    public function parse(string $typeString, bool $isFrozen = false): TypeInfo {

        if ($this->nestingDepth > self::MAX_NESTING_DEPTH) {
            throw new TypeNameParserException(
                'Invalid type string: nesting is deeper than this parser will read',
                ExceptionCode::TYPENAMEPARSER_NESTING_TOO_DEEP->value,
                [
                    'type_string' => $typeString,
                    'max_depth' => self::MAX_NESTING_DEPTH,
                    'reason' => 'nesting_too_deep',
                ]
            );
        }

        $this->nestingDepth++;

        try {
            return $this->parseType($typeString, $isFrozen);
        } finally {
            $this->nestingDepth--;
        }
    }

    /**
     * Decode a hex-encoded UDT name or field name as found in "UserType(...)"
     * type strings.
     *
     * Cassandra hex-encodes the type name and every field name (but not the
     * keyspace) in TypeParser::stringifyUserTypeParameters(), and requires hex
     * when reading them back in TypeParser::getUserTypeParameters(): both are
     * passed through ByteBufferUtil::hexToBytes(), which rejects odd-length
     * strings and non-hex characters. Hex is therefore mandatory, not optional,
     * and a name that is not valid hex is a malformed type string.
     *
     * @throws \Cassandra\Exception\TypeNameParserException
     */
    private function decodeUdtName(string $name): string {
        if ($name !== '' && ((strlen($name) % 2) !== 0 || !StringUtil::isHexDigits($name))) {
            throw new TypeNameParserException(
                'Invalid UDT type params: name is not hex-encoded',
                ExceptionCode::TYPENAMEPARSER_UDT_NAME_INVALID_HEX->value,
                [
                    'name' => $name,
                    'reason' => 'udt_name_not_hex_encoded',
                ]
            );
        }

        $decoded = $name === '' ? '' : hex2bin($name);

        if ($decoded === false || preg_match('//u', $decoded) !== 1) {
            throw new TypeNameParserException(
                'Invalid UDT type params: name does not decode to valid UTF-8',
                ExceptionCode::TYPENAMEPARSER_UDT_NAME_INVALID_HEX->value,
                [
                    'name' => $name,
                    'reason' => 'udt_name_not_valid_utf8',
                ]
            );
        }

        return $decoded;
    }

    /**
     * Split a bracketed parameter list into its top-level parameters.
     *
     * Each parameter is returned as a name/value pair; the name is the part
     * before a top-level ":" (used by UDT field entries) or null when the
     * parameter is unnamed. Names are kept as strings here on purpose — using
     * them as array keys would let PHP silently coerce all-digit names (which
     * hex-encoded UDT field names frequently are) into integers.
     *
     * @return list<array{name: ?string, value: string}>
     *
     * @throws \Cassandra\Exception\TypeNameParserException
     */
    private function extractParams(string $paramString): array {

        $params = [];

        $length = strlen($paramString);
        if ($length === 0) {
            return $params;
        }

        $name = null;
        $startCurrentParam = 0;
        $bracketsOpened = 0;

        for ($i = 0; $i < $length; $i++) {
            $char = $paramString[$i];

            if ($char === '(') {
                $bracketsOpened++;

                continue;
            }

            if ($char === ')') {

                if ($bracketsOpened === 0) {
                    throw new TypeNameParserException(
                        'Invalid type string: unmatched closing bracket',
                        ExceptionCode::TYPENAMEPARSER_INVALID_PARAM_BRACKETS_UNMATCHED_CLOSING->value,
                        [
                            'param_string' => $paramString,
                            'position' => $i,
                            'character' => $char,
                            'reason' => 'unmatched_closing_bracket',
                        ]
                    );
                }

                $bracketsOpened--;

                continue;
            }

            if ($bracketsOpened > 0) {
                continue;
            }

            if ($char === ':') {
                if ($name !== null) {
                    throw new TypeNameParserException(
                        'Invalid UDT type params: multiple colons found in parameter',
                        ExceptionCode::TYPENAMEPARSER_UDT_PARAMS_MULTIPLE_COLONS->value,
                        [
                            'param_string' => $paramString,
                            'position' => $i,
                            'existing_name' => $name,
                            'reason' => 'multiple_colons_in_parameter',
                        ]
                    );
                }

                $name = trim(substr($paramString, $startCurrentParam, $i - $startCurrentParam));
                $startCurrentParam = $i + 1;

                continue;
            }

            if ($char === ',') {
                $params[] = [
                    'name' => $name,
                    'value' => trim(substr($paramString, $startCurrentParam, $i - $startCurrentParam)),
                ];
                $name = null;

                $startCurrentParam = $i + 1;

                continue;
            }
        }

        if ($bracketsOpened > 0) {
            throw new TypeNameParserException(
                'Invalid type string: unmatched opening bracket',
                ExceptionCode::TYPENAMEPARSER_INVALID_PARAM_BRACKETS_UNMATCHED_OPENING->value,
                [
                    'param_string' => $paramString,
                    'unmatched_brackets' => $bracketsOpened,
                    'reason' => 'unmatched_opening_bracket',
                ]
            );
        }

        $params[] = [
            'name' => $name,
            'value' => trim(substr($paramString, $startCurrentParam)),
        ];

        foreach ($params as $index => $param) {
            if ($param['value'] !== '') {
                continue;
            }

            throw new TypeNameParserException(
                'Invalid type string: parameter ' . $index . ' names no type',
                ExceptionCode::TYPENAMEPARSER_EMPTY_PARAM->value,
                [
                    'param_string' => $paramString,
                    'index' => $index,
                    'param_name' => $param['name'],
                    'reason' => 'empty_parameter',
                ]
            );
        }

        return $params;
    }

    /**
     * @return array<string, \Cassandra\Type>
     */
    private function getSimpleTypeMap(): array {
        // Cached because enum `->value` cannot be used in a constant expression
        // on PHP 8.1; once 8.1 support is dropped this can become a real `const`.
        return self::$simpleTypeMap ??= [
            TypeName::ASCII->value => Type::ASCII,
            TypeName::BOOLEAN->value => Type::BOOLEAN,
            TypeName::BYTE->value => Type::TINYINT,
            TypeName::BYTES->value => Type::BLOB,
            TypeName::COUNTER_COLUMN->value => Type::COUNTER,
            TypeName::DECIMAL->value => Type::DECIMAL,
            TypeName::DOUBLE->value => Type::DOUBLE,
            TypeName::DURATION->value => Type::DURATION,
            TypeName::FLOAT->value => Type::FLOAT,
            TypeName::INET_ADDRESS->value => Type::INET,
            TypeName::INT32->value => Type::INT,
            TypeName::INTEGER->value => Type::VARINT,
            TypeName::LONG->value => Type::BIGINT,
            TypeName::SHORT->value => Type::SMALLINT,
            TypeName::SIMPLE_DATE->value => Type::DATE,
            TypeName::TIME_UUID->value => Type::TIMEUUID,
            TypeName::TIME->value => Type::TIME,
            TypeName::TIMESTAMP->value => Type::TIMESTAMP,
            TypeName::UTF8->value => Type::VARCHAR,
            TypeName::UUID->value => Type::UUID,
        ];
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     *
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseFrozenType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new TypeNameParserException(
                'Invalid frozen type params: exactly one parameter required',
                ExceptionCode::TYPENAMEPARSER_FROZEN_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'frozen_requires_exactly_one_parameter',
                ]
            );
        }

        return $this->parse($params[0]['value'], isFrozen: true);
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     * 
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseListType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new TypeNameParserException(
                'Invalid list type params: exactly one parameter required',
                ExceptionCode::TYPENAMEPARSER_LIST_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'list_requires_exactly_one_parameter',
                ]
            );
        }

        $typeInfo = $this->parse($params[0]['value']);

        return new ListCollectionInfo($typeInfo, $isFrozen);
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     * 
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseMapType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 2) {
            throw new TypeNameParserException(
                'Invalid map type params: exactly two parameters required',
                ExceptionCode::TYPENAMEPARSER_MAP_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 2,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'map_requires_exactly_two_parameters',
                ]
            );
        }

        $keyType = $this->parse($params[0]['value']);
        $valueType = $this->parse($params[1]['value']);

        return new MapCollectionInfo($keyType, $valueType, $isFrozen);
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     * 
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseReversedType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new TypeNameParserException(
                'Invalid reversed type params: exactly one parameter required',
                ExceptionCode::TYPENAMEPARSER_REVERSED_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'reversed_requires_exactly_one_parameter',
                ]
            );
        }

        return $this->parse($params[0]['value'], $isFrozen);
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     * 
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseSetType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new TypeNameParserException(
                'Invalid set type params: exactly one parameter required',
                ExceptionCode::TYPENAMEPARSER_SET_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'set_requires_exactly_one_parameter',
                ]
            );
        }

        $typeInfo = $this->parse($params[0]['value']);

        return new SetCollectionInfo($typeInfo, $isFrozen);
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     * 
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseTupleType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) < 1) {
            throw new TypeNameParserException(
                'Invalid tuple type params: at least one parameter required',
                ExceptionCode::TYPENAMEPARSER_TUPLE_INVALID_PARAM_COUNT->value,
                [
                    'minimum_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'tuple_requires_at_least_one_parameter',
                ]
            );
        }

        $valueTypes = [];
        $paramsCount = count($params);
        for ($i = 0; $i < $paramsCount; $i++) {
            $valueTypes[] = $this->parse($params[$i]['value']);
        }

        return new TupleInfo($valueTypes);
    }

    /**
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseType(string $typeString, bool $isFrozen): TypeInfo {

        $firstBracketIndex = strpos($typeString, '(');
        if ($firstBracketIndex === false) {

            if (str_contains($typeString, ')')) {
                throw new TypeNameParserException(
                    'Invalid type string: contains closing bracket without opening bracket',
                    ExceptionCode::TYPENAMEPARSER_INVALID_BRACKETS_CLOSING_WITHOUT_OPENING->value,
                    [
                        'type_string' => $typeString,
                        'reason' => 'closing_bracket_without_opening',
                    ]
                );
            }

            $typeName = trim($typeString);
            $params = [];

        } else {
            $typeName = trim(substr($typeString, 0, $firstBracketIndex));
            $paramString = trim(substr($typeString, $firstBracketIndex + 1));

            if (!str_ends_with($paramString, ')')) {
                throw new TypeNameParserException(
                    'Invalid type string: missing closing bracket',
                    ExceptionCode::TYPENAMEPARSER_INVALID_BRACKETS_MISSING_CLOSING->value,
                    [
                        'type_string' => $typeString,
                        'type_name' => $typeName,
                        'param_string' => $paramString,
                        'reason' => 'missing_closing_bracket',
                    ]
                );
            }

            $paramStringWithoutLastBracket = substr($paramString, 0, -1);

            $params = $this->extractParams($paramStringWithoutLastBracket);
        }

        $simpleTypeMap = $this->getSimpleTypeMap();
        if (isset($simpleTypeMap[$typeName])) {

            if ($params) {
                throw new TypeNameParserException(
                    'Invalid type string: simple types cannot have parameters',
                    ExceptionCode::TYPENAMEPARSER_SIMPLE_TYPE_WITH_PARAMETERS->value,
                    [
                        'type_string' => $typeString,
                        'type_name' => $typeName,
                        'parameters' => $params,
                        'simple_type' => $simpleTypeMap[$typeName]->name,
                    ]
                );
            }

            if ($isFrozen) {
                throw new TypeNameParserException(
                    'Invalid type for frozen: simple types cannot be frozen',
                    ExceptionCode::TYPENAMEPARSER_SIMPLE_TYPE_CANNOT_BE_FROZEN->value,
                    [
                        'type_string' => $typeString,
                        'type_name' => $typeName,
                        'simple_type' => $simpleTypeMap[$typeName]->name,
                        'reason' => 'simple_types_cannot_be_frozen',
                    ]
                );
            }

            return new SimpleTypeInfo($simpleTypeMap[$typeName]);
        }

        $complexTypeInfo = match ($typeName) {
            TypeName::FROZEN->value => $this->parseFrozenType($params, $isFrozen),
            TypeName::REVERSED->value => $this->parseReversedType($params, $isFrozen),

            TypeName::MAP->value => $this->parseMapType($params, $isFrozen),
            TypeName::LIST->value => $this->parseListType($params, $isFrozen),
            TypeName::SET->value => $this->parseSetType($params, $isFrozen),
            TypeName::TUPLE->value => $this->parseTupleType($params, $isFrozen),
            TypeName::UDT->value => $this->parseUDTType($params, $isFrozen),
            TypeName::VECTOR->value => $this->parseVectorType($params, $isFrozen),

            default => null,
        };

        if ($complexTypeInfo !== null) {
            return $complexTypeInfo;
        }

        return new CustomInfo(
            javaClassName: trim($typeString),
        );
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     * 
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseUDTType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) < 3) {
            throw new TypeNameParserException(
                'Invalid UDT type params: at least three parameters required',
                ExceptionCode::TYPENAMEPARSER_UDT_INVALID_PARAM_COUNT->value,
                [
                    'minimum_count' => 3,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'udt_requires_keyspace_name_and_fields',
                ]
            );
        }

        $keyspace = $params[0]['value'];
        $name = $this->decodeUdtName($params[1]['value']);

        $valueTypes = [];
        $udtParams = array_slice($params, 2);
        foreach ($udtParams as $param) {

            if ($param['name'] === null) {
                throw new TypeNameParserException(
                    'Invalid UDT type params: field entries must be "name:type" pairs',
                    ExceptionCode::TYPENAMEPARSER_UDT_FIELD_KEY_NOT_STRING->value,
                    [
                        'invalid_field' => $param['value'],
                        'parameters' => $params,
                        'reason' => 'udt_field_entries_must_be_named',
                    ]
                );
            }

            $fieldName = $this->decodeUdtName($param['name']);
            if (array_key_exists($fieldName, $valueTypes)) {
                throw new TypeNameParserException(
                    'Invalid UDT type params: field names must be unique',
                    ExceptionCode::TYPENAMEPARSER_UDT_DUPLICATE_FIELD->value,
                    [
                        'field' => $fieldName,
                        'keyspace' => $keyspace,
                        'udt_name' => $name,
                        'reason' => 'udt_field_names_must_be_unique',
                    ]
                );
            }

            $valueTypes[$fieldName] = $this->parse($param['value']);
        }

        return new UDTInfo($valueTypes, $isFrozen, $keyspace, $name);
    }

    /**
     * @param list<array{name: ?string, value: string}> $params
     * 
     * @throws \Cassandra\Exception\TypeNameParserException
     * @throws \Cassandra\Exception\TypeInfoException
     */
    private function parseVectorType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 2) {
            throw new TypeNameParserException(
                'Invalid vector type params: exactly two parameters required',
                ExceptionCode::TYPENAMEPARSER_VECTOR_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 2,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'vector_requires_type_and_dimensions',
                ]
            );
        }

        $typeInfo = $this->parse($params[0]['value']);

        if (!is_numeric($params[1]['value'])) {
            throw new TypeNameParserException(
                'Invalid vector type dimensions: must be numeric',
                ExceptionCode::TYPENAMEPARSER_VECTOR_DIMENSIONS_NON_NUMERIC->value,
                [
                    'provided_value' => $params[1]['value'],
                    'value_type' => gettype($params[1]['value']),
                    'reason' => 'dimensions_must_be_numeric',
                ]
            );
        }

        if (preg_match('/^[+-]?\d+$/D', $params[1]['value']) !== 1) {
            throw new TypeNameParserException(
                'Invalid vector type dimensions: must be an integer',
                ExceptionCode::TYPENAMEPARSER_VECTOR_DIMENSIONS_NOT_INTEGER->value,
                [
                    'provided_value' => $params[1]['value'],
                    'reason' => 'dimensions_must_be_an_integer',
                ]
            );
        }

        $dimensions = (int) $params[1]['value'];

        if ($dimensions < 1 || $dimensions > VectorInfo::MAX_DIMENSIONS) {
            throw new TypeNameParserException(
                'Invalid vector type dimensions: must be between 1 and ' . VectorInfo::MAX_DIMENSIONS,
                ExceptionCode::TYPENAMEPARSER_VECTOR_DIMENSIONS_OUT_OF_RANGE->value,
                [
                    'provided_value' => $dimensions,
                    'minimum_value' => 1,
                    'maximum_value' => VectorInfo::MAX_DIMENSIONS,
                    'reason' => 'dimensions_out_of_supported_range',
                ]
            );
        }

        return new VectorInfo($typeInfo, $dimensions);
    }
}
