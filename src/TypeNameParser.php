<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\TypeInfo\CollectionListInfo;
use Cassandra\TypeInfo\CollectionMapInfo;
use Cassandra\TypeInfo\CollectionSetInfo;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Type\Exception;

final class TypeNameParser {
    /**
     * @throws \Cassandra\Type\Exception
     */
    public function parse(string $typeString, bool $isFrozen = false): TypeInfo {

        $firstBracketIndex = mb_strpos($typeString, '(');
        if ($firstBracketIndex === false) {

            if (str_contains($typeString, ')')) {
                throw new Exception(
                    'Invalid type string: contains closing bracket without opening bracket',
                    ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_CLOSING_WITHOUT_OPENING->value,
                    [
                        'type_string' => $typeString,
                        'reason' => 'closing_bracket_without_opening',
                    ]
                );
            }

            $typeName = trim($typeString);
            $params = [];

        } else {
            $typeName = trim(mb_substr($typeString, 0, $firstBracketIndex));
            $paramString = trim(mb_substr($typeString, $firstBracketIndex + 1));

            if (!str_ends_with($paramString, ')')) {
                throw new Exception(
                    'Invalid type string: missing closing bracket',
                    ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_MISSING_CLOSING->value,
                    [
                        'type_string' => $typeString,
                        'type_name' => $typeName,
                        'param_string' => $paramString,
                        'reason' => 'missing_closing_bracket',
                    ]
                );
            }

            $paramStringWithoutLastBracket = mb_substr($paramString, 0, -1);

            $params = $this->extractParams($paramStringWithoutLastBracket);
        }

        $simpleTypeMap = $this->getSimpleTypeMap();
        if (isset($simpleTypeMap[$typeName])) {

            if ($params) {
                throw new Exception(
                    'Invalid type string: simple types cannot have parameters',
                    ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_WITH_PARAMETERS->value,
                    [
                        'type_string' => $typeString,
                        'type_name' => $typeName,
                        'parameters' => $params,
                        'simple_type' => $simpleTypeMap[$typeName]->name,
                    ]
                );
            }

            if ($isFrozen) {
                throw new Exception(
                    'Invalid type for frozen: simple types cannot be frozen',
                    ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_CANNOT_BE_FROZEN->value,
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

        $complexTypeMap = $this->getComplexTypeMap();
        if (isset($complexTypeMap[$typeName])) {
            return $complexTypeMap[$typeName]($params, $isFrozen);
        }

        return new CustomInfo(
            javaClassName: $typeString,
        );
    }

    /**
     * @return array<string|int,string>
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function extractParams(string $paramString): array {

        $params = [];

        $length = mb_strlen($paramString);
        if ($length === 0) {
            return $params;
        }

        $name = null;
        $startCurrentParam = 0;
        $bracketsOpened = 0;

        for ($i = 0; $i < $length; $i++) {
            $char = mb_substr($paramString, $i, 1);

            if ($char === '(') {
                $bracketsOpened++;

                continue;
            }

            if ($char === ')') {

                if ($bracketsOpened === 0) {
                    throw new Exception(
                        'Invalid type string: unmatched closing bracket',
                        ExceptionCode::TYPENAME_PARSER_INVALID_PARAM_BRACKETS_UNMATCHED_CLOSING->value,
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

            if ($bracketsOpened > 0 && $char !== ')') {
                continue;
            }

            if ($char === ':') {
                if ($name !== null) {
                    throw new Exception(
                        'Invalid UDT type params: multiple colons found in parameter',
                        ExceptionCode::TYPENAME_PARSER_UDT_PARAMS_MULTIPLE_COLONS->value,
                        [
                            'param_string' => $paramString,
                            'position' => $i,
                            'existing_name' => $name,
                            'reason' => 'multiple_colons_in_parameter',
                        ]
                    );
                }

                $name = trim(mb_substr($paramString, $startCurrentParam, $i - $startCurrentParam));
                $startCurrentParam = $i + 1;
            }

            if ($char === ',') {

                if ($name !== null) {
                    $params[$name] = trim(mb_substr($paramString, $startCurrentParam, $i - $startCurrentParam));
                    $name = null;

                } else {
                    $params[] = trim(mb_substr($paramString, $startCurrentParam, $i - $startCurrentParam));
                }

                $startCurrentParam = $i + 1;
            }
        }

        if ($bracketsOpened > 0) {
            throw new Exception(
                'Invalid type string: unmatched opening bracket',
                ExceptionCode::TYPENAME_PARSER_INVALID_PARAM_BRACKETS_UNMATCHED_OPENING->value,
                [
                    'param_string' => $paramString,
                    'unmatched_brackets' => $bracketsOpened,
                    'reason' => 'unmatched_opening_bracket',
                ]
            );
        }

        if ($startCurrentParam < $length) {
            if ($name !== null) {
                $params[$name] = trim(mb_substr($paramString, $startCurrentParam));
                $name = null;
            } else {
                $params[] = trim(mb_substr($paramString, $startCurrentParam));
            }
        }

        return $params;
    }

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<string, callable(array<string>, boolean): \Cassandra\TypeInfo\TypeInfo>
     */
    protected function getComplexTypeMap(): array {
        return [
            TypeName::FROZEN->value => [$this, 'parseFrozenType'],
            TypeName::REVERSED->value => [$this, 'parseReversedType'],

            TypeName::MAP->value => [$this, 'parseMapType'],
            TypeName::LIST->value => [$this, 'parseListType'],
            TypeName::SET->value => [$this, 'parseSetType'],
            TypeName::TUPLE->value => [$this, 'parseTupleType'],
            TypeName::UDT->value => [$this, 'parseUDTType'],
            TypeName::VECTOR->value => [$this, 'parseVectorType'],
        ];
    }

    /**
     * @todo this should be moved to a const class value once support for php 8.1 is dropped
     * 
     * @return array<string, \Cassandra\Type>
     */
    protected function getSimpleTypeMap(): array {
        return [
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
            TypeName::TIME->value => Type::TIME,
            TypeName::TIME_UUID->value => Type::TIMEUUID,
            TypeName::TIMESTAMP->value => Type::TIMESTAMP,
            TypeName::UTF8->value => Type::VARCHAR,
            TypeName::UUID->value => Type::UUID,
        ];
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseFrozenType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new Exception(
                'Invalid frozen type params: exactly one parameter required',
                ExceptionCode::TYPENAME_PARSER_FROZEN_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'frozen_requires_exactly_one_parameter',
                ]
            );
        }

        return $this->parse($params[0], isFrozen: true);
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseListType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new Exception(
                'Invalid list type params: exactly one parameter required',
                ExceptionCode::TYPENAME_PARSER_LIST_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'list_requires_exactly_one_parameter',
                ]
            );
        }

        $typeInfo = $this->parse($params[0]);

        return new CollectionListInfo($typeInfo, $isFrozen);
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseMapType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 2) {
            throw new Exception(
                'Invalid map type params: exactly two parameters required',
                ExceptionCode::TYPENAME_PARSER_MAP_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 2,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'map_requires_exactly_two_parameters',
                ]
            );
        }

        $keyType = $this->parse($params[0]);
        $valueType = $this->parse($params[1]);

        return new CollectionMapInfo($keyType, $valueType, $isFrozen);
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseReversedType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new Exception(
                'Invalid reversed type params: exactly one parameter required',
                ExceptionCode::TYPENAME_PARSER_REVERSED_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'reversed_requires_exactly_one_parameter',
                ]
            );
        }

        return $this->parse($params[0], $isFrozen);
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseSetType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 1) {
            throw new Exception(
                'Invalid set type params: exactly one parameter required',
                ExceptionCode::TYPENAME_PARSER_SET_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 1,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'set_requires_exactly_one_parameter',
                ]
            );
        }

        $typeInfo = $this->parse($params[0]);

        return new CollectionSetInfo($typeInfo, $isFrozen);
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseTupleType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) < 1) {
            throw new Exception(
                'Invalid tuple type params: at least one parameter required',
                ExceptionCode::TYPENAME_PARSER_TUPLE_INVALID_PARAM_COUNT->value,
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
            $valueTypes[] = $this->parse($params[$i]);
        }

        return new TupleInfo($valueTypes);
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseUDTType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) < 3) {
            throw new Exception(
                'Invalid UDT type params: at least three parameters required',
                ExceptionCode::TYPENAME_PARSER_UDT_INVALID_PARAM_COUNT->value,
                [
                    'minimum_count' => 3,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'udt_requires_keyspace_name_and_fields',
                ]
            );
        }

        $keyspace = $params[0];
        $name = $params[1];

        $valueTypes = [];
        $udtParams = array_slice($params, 2);
        foreach ($udtParams as $key => $value) {

            if (!is_string($key)) {
                throw new Exception(
                    'Invalid UDT type params: field keys must be strings',
                    ExceptionCode::TYPENAME_PARSER_UDT_FIELD_KEY_NOT_STRING->value,
                    [
                        'invalid_key' => $key,
                        'key_type' => gettype($key),
                        'parameters' => $params,
                        'reason' => 'udt_field_keys_must_be_strings',
                    ]
                );
            }

            $valueTypes[$key] = $this->parse($value);
        }

        return new UDTInfo($valueTypes, $isFrozen, $keyspace, $name);
    }

    /**
     * @param array<string> $params
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function parseVectorType(array $params, bool $isFrozen): TypeInfo {

        if (count($params) !== 2) {
            throw new Exception(
                'Invalid vector type params: exactly two parameters required',
                ExceptionCode::TYPENAME_PARSER_VECTOR_INVALID_PARAM_COUNT->value,
                [
                    'expected_count' => 2,
                    'actual_count' => count($params),
                    'parameters' => $params,
                    'reason' => 'vector_requires_type_and_dimensions',
                ]
            );
        }

        $typeInfo = $this->parse($params[0]);

        if (!is_numeric($params[1])) {
            throw new Exception(
                'Invalid vector type dimensions: must be numeric',
                ExceptionCode::TYPENAME_PARSER_VECTOR_DIMENSIONS_NON_NUMERIC->value,
                [
                    'provided_value' => $params[1],
                    'value_type' => gettype($params[1]),
                    'reason' => 'dimensions_must_be_numeric',
                ]
            );
        }

        $dimensions = (int) $params[1];

        if ($dimensions < 0) {
            throw new Exception(
                'Invalid vector type dimensions: must be non-negative',
                ExceptionCode::TYPENAME_PARSER_VECTOR_DIMENSIONS_OUT_OF_RANGE->value,
                [
                    'provided_value' => $dimensions,
                    'minimum_value' => 0,
                    'reason' => 'dimensions_must_be_non_negative',
                ]
            );
        }

        return new VectorInfo($typeInfo, $dimensions);
    }
}
