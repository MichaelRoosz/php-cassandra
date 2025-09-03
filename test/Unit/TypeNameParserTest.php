<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeName;
use Cassandra\TypeNameParser;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Type\Exception;
use PHPUnit\Framework\TestCase;
use ReflectionClass;

class TypeNameParserTest extends TestCase {
    private const SIMPLE_TYPES = [
        [TypeName::ASCII, Type::ASCII],
        [TypeName::BOOLEAN, Type::BOOLEAN],
        [TypeName::BYTE, Type::TINYINT],
        [TypeName::BYTES, Type::BLOB],
        [TypeName::COUNTER_COLUMN, Type::COUNTER],
        [TypeName::DECIMAL, Type::DECIMAL],
        [TypeName::DOUBLE, Type::DOUBLE],
        [TypeName::DURATION, Type::DURATION],
        [TypeName::FLOAT, Type::FLOAT],
        [TypeName::INET_ADDRESS, Type::INET],
        [TypeName::INT32, Type::INT],
        [TypeName::INTEGER, Type::VARINT],
        [TypeName::LONG, Type::BIGINT],
        [TypeName::SHORT, Type::SMALLINT],
        [TypeName::SIMPLE_DATE, Type::DATE],
        [TypeName::TIME, Type::TIME],
        [TypeName::TIME_UUID, Type::TIMEUUID],
        [TypeName::TIMESTAMP, Type::TIMESTAMP],
        [TypeName::UTF8, Type::VARCHAR],
        [TypeName::UUID, Type::UUID],
    ];

    private TypeNameParser $parser;

    protected function setUp(): void {
        $this->parser = new TypeNameParser();
    }

    // =============================================
    // COMBINATION TESTS
    // =============================================

    public function testCombinationsOfSimpleTypesInCollections(): void {

        foreach (self::SIMPLE_TYPES as [$typeName, $expectedType]) {
            // Test list
            $result = $this->parser->parse(TypeName::LIST->value . '(' . $typeName->value . ')');
            $this->assertInstanceOf(ListCollectionInfo::class, $result);

            // Test set
            $result = $this->parser->parse(TypeName::SET->value . '(' . $typeName->value . ')');
            $this->assertInstanceOf(SetCollectionInfo::class, $result);

            // Test map
            $result = $this->parser->parse(TypeName::MAP->value . '(' . $typeName->value . ',' . TypeName::UTF8->value . ')');
            $this->assertInstanceOf(MapCollectionInfo::class, $result);

            // Test tuple
            $result = $this->parser->parse(TypeName::TUPLE->value . '(' . $typeName->value . ')');
            $this->assertInstanceOf(TupleInfo::class, $result);

            // Test UDT
            $result = $this->parser->parse(TypeName::UDT->value . '(ks,typeName,field1:' . $typeName->value . ',field2:' . TypeName::UTF8->value . ')');
            $this->assertInstanceOf(UDTInfo::class, $result);
            $this->assertEquals('ks', $result->keyspace);
            $this->assertEquals('typeName', $result->name);
            $this->assertEquals($expectedType, $result->valueTypes['field1']->type);
            $this->assertEquals(Type::VARCHAR, $result->valueTypes['field2']->type);

            // Test vector
            $result = $this->parser->parse(TypeName::VECTOR->value . '(' . $typeName->value . ',10)');
            $this->assertInstanceOf(VectorInfo::class, $result);
            $this->assertEquals(10, $result->dimensions);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result->valueType);
            $this->assertEquals($expectedType, $result->valueType->type);
        }
    }

    // =============================================
    // CUSTOM TYPE TESTS
    // =============================================

    public function testCustomTypeOnEmptyString(): void {
        $result = $this->parser->parse('');
        $this->assertInstanceOf(CustomInfo::class, $result);
        $this->assertEquals('', $result->javaClassName);
    }

    public function testCustomTypes(): void {
        $testCases = [
            'com.example.CustomType',
            'org.mycompany.MyCustomClass',
            'CustomType',
            'simple',
            'com.example.CustomType(param1,param2)',
            'com.example.CustomType()',
            'com.example.CustomType(param1)',
            '',
        ];

        foreach ($testCases as $typeString) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(CustomInfo::class, $result);
            $this->assertEquals($typeString, $result->javaClassName);
        }
    }

    public function testCustomTypesWithSpecialCharacters(): void {
        $testCases = [
            'com.example.Type$Inner',
            'org.test.Type_With_Underscores',
            'Type-With-Dashes',
            'Type.With.Dots',
            'Type123WithNumbers',
            'com.example.Type(complex,nested(params),more)',
        ];

        foreach ($testCases as $typeString) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(CustomInfo::class, $result);
            $this->assertEquals($typeString, $result->javaClassName);
        }
    }

    // =============================================
    // EDGE CASE TESTS
    // =============================================

    public function testEdgeCaseComplexType(): void {
        // Create a very complex nested type to test parser robustness
        $complexType = TypeName::MAP->value . '(' .
            TypeName::TUPLE->value . '(' . TypeName::UTF8->value . ',' . TypeName::UUID->value . '),' .
            TypeName::FROZEN->value . '(' .
                TypeName::LIST->value . '(' .
                    TypeName::SET->value . '(' .
                        TypeName::UDT->value . '(ks,typeName,field1:' . TypeName::TIMESTAMP->value . ',field2:' . TypeName::INT32->value . ')' .
                    ')' .
                ')' .
            ')' .
        ')';

        $result = $this->parser->parse($complexType);

        $this->assertInstanceOf(MapCollectionInfo::class, $result);

        $this->assertInstanceOf(TupleInfo::class, $result->keyType);
        $this->assertEquals(Type::VARCHAR, $result->keyType->valueTypes[0]->type);
        $this->assertEquals(Type::UUID, $result->keyType->valueTypes[1]->type);

        $this->assertInstanceOf(ListCollectionInfo::class, $result->valueType);
        $this->assertTrue($result->valueType->isFrozen);

        $listValueType = $result->valueType->valueType;
        $this->assertInstanceOf(SetCollectionInfo::class, $listValueType);

        $setValueType = $listValueType->valueType;
        $this->assertInstanceOf(UDTInfo::class, $setValueType);
        $this->assertEquals('ks', $setValueType->keyspace);
        $this->assertEquals('typeName', $setValueType->name);
        $this->assertEquals(Type::TIMESTAMP, $setValueType->valueTypes['field1']->type);
        $this->assertEquals(Type::INT, $setValueType->valueTypes['field2']->type);
    }

    public function testEdgeCaseExtremelyLongTypeString(): void {
        // Test with a very long nested type string to ensure parser can handle complexity
        $nested = TypeName::UTF8->value;
        for ($i = 0; $i < 20; $i++) {
            $nested = TypeName::LIST->value . '(' . $nested . ')';
        }

        $result = $this->parser->parse($nested);

        // Verify it's a deeply nested list structure
        $current = $result;
        for ($i = 0; $i < 20; $i++) {
            $this->assertInstanceOf(ListCollectionInfo::class, $current);
            $current = $current->valueType;
        }

        $this->assertInstanceOf(SimpleTypeInfo::class, $current);
        $this->assertEquals(Type::VARCHAR, $current->type);
    }

    public function testEdgeCaseNestedTypes(): void {
        $testCases = [
            [TypeName::LIST->value . '(' . TypeName::SET->value . '(' . TypeName::UTF8->value . '))', ListCollectionInfo::class],
            [TypeName::MAP->value . '(' . TypeName::UTF8->value . ',' . TypeName::LIST->value . '(' . TypeName::INT32->value . '))', MapCollectionInfo::class],
            [TypeName::SET->value . '(' . TypeName::MAP->value . '(' . TypeName::UUID->value . ',' . TypeName::UTF8->value . '))', SetCollectionInfo::class],
            [TypeName::TUPLE->value . '(' . TypeName::LIST->value . '(' . TypeName::UTF8->value . '),' . TypeName::MAP->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . '))', TupleInfo::class],
        ];

        foreach ($testCases as [$typeString, $expectedClass]) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf($expectedClass, $result);
        }
    }

    // =============================================
    // ERROR CASES
    // =============================================

    public function testErrorInvalidTypeMissingClosingBracket(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_MISSING_CLOSING->value);

        $this->parser->parse('invalidtype(unclosed');
    }

    public function testErrorMalformedBracketClosingWithoutOpening(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_CLOSING_WITHOUT_OPENING->value);

        $this->parser->parse('type)');
    }

    public function testErrorMalformedBracketMissingClosing(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_MISSING_CLOSING->value);

        $this->parser->parse('type(');
    }

    public function testErrorMalformedBracketUnbalanced1(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_PARAM_BRACKETS_UNMATCHED_OPENING->value);

        $this->parser->parse('type(()');
    }

    public function testErrorMalformedBracketUnbalanced2(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_PARAM_BRACKETS_UNMATCHED_CLOSING->value);

        $this->parser->parse('type())');
    }

    // =============================================
    // FROZEN TYPE TESTS
    // =============================================

    public function testFrozenBooleanTypeThrowsException(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_CANNOT_BE_FROZEN->value);

        $this->parser->parse(TypeName::BOOLEAN->value, true);
    }

    public function testFrozenInt32TypeThrowsException(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_CANNOT_BE_FROZEN->value);

        $this->parser->parse(TypeName::INT32->value, true);
    }

    public function testFrozenListType(): void {
        $result = $this->parser->parse(TypeName::LIST->value . '(' . TypeName::UTF8->value . ')', true);
        $this->assertInstanceOf(ListCollectionInfo::class, $result);
        $this->assertTrue($result->isFrozen);
        $this->assertEquals(Type::VARCHAR, $result->valueType->type);
    }

    public function testFrozenMapType(): void {
        $result = $this->parser->parse(TypeName::MAP->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ')', true);
        $this->assertInstanceOf(MapCollectionInfo::class, $result);
        $this->assertTrue($result->isFrozen);
        $this->assertEquals(Type::VARCHAR, $result->keyType->type);
        $this->assertEquals(Type::INT, $result->valueType->type);
    }

    public function testFrozenNestedTypes(): void {
        $typeString = TypeName::FROZEN->value . '(' . TypeName::LIST->value . '(' . TypeName::FROZEN->value . '(' . TypeName::SET->value . '(' . TypeName::UTF8->value . '))))';

        $result = $this->parser->parse($typeString);
        $this->assertInstanceOf(ListCollectionInfo::class, $result);
        $this->assertTrue($result->isFrozen);
        $this->assertInstanceOf(SetCollectionInfo::class, $result->valueType);
        $this->assertTrue($result->valueType->isFrozen);
    }

    public function testFrozenNoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_FROZEN_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::FROZEN->value . '()');
    }

    public function testFrozenSetType(): void {
        $result = $this->parser->parse(TypeName::SET->value . '(' . TypeName::UTF8->value . ')', true);
        $this->assertInstanceOf(SetCollectionInfo::class, $result);
        $this->assertTrue($result->isFrozen);
        $this->assertEquals(Type::VARCHAR, $result->valueType->type);
    }

    public function testFrozenTooManyParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_FROZEN_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::FROZEN->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ')');
    }

    public function testFrozenUDTType(): void {
        $result = $this->parser->parse(TypeName::UDT->value . '(ks,type,field1:' . TypeName::UTF8->value . ',field2:' . TypeName::INT32->value . ')', true);
        $this->assertInstanceOf(UDTInfo::class, $result);
        $this->assertTrue($result->isFrozen);
    }

    public function testFrozenUTF8TypeThrowsException(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_CANNOT_BE_FROZEN->value);

        $this->parser->parse(TypeName::UTF8->value, true);
    }

    public function testFrozenUUIDTypeThrowsException(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_CANNOT_BE_FROZEN->value);

        $this->parser->parse(TypeName::UUID->value, true);
    }

    // =============================================
    // LIST TYPE TESTS
    // =============================================

    public function testListTypeErrorMissingClosingBracket(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_MISSING_CLOSING->value);

        $this->parser->parse(TypeName::LIST->value . '(' . TypeName::UTF8->value);
    }

    public function testListTypeErrorNoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_LIST_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::LIST->value . '()');
    }

    public function testListTypeErrorTooManyParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_LIST_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::LIST->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ')');
    }

    public function testListTypes(): void {
        $testCases = [
            [TypeName::LIST->value . '(' . TypeName::UTF8->value . ')', Type::VARCHAR, false],
            [TypeName::LIST->value . '(' . TypeName::INT32->value . ')', Type::INT, false],
            [TypeName::LIST->value . '(' . TypeName::BOOLEAN->value . ')', Type::BOOLEAN, false],
            [TypeName::LIST->value . '(' . TypeName::UUID->value . ')', Type::UUID, false],
            [TypeName::LIST->value . '(' . TypeName::TIMESTAMP->value . ')', Type::TIMESTAMP, false],
        ];

        foreach ($testCases as [$typeString, $expectedElementType, $isFrozen]) {
            $result = $this->parser->parse($typeString, $isFrozen);
            $this->assertInstanceOf(ListCollectionInfo::class, $result);
            $this->assertEquals($isFrozen, $result->isFrozen);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result->valueType);
            $this->assertEquals($expectedElementType, $result->valueType->type);
        }
    }

    public function testListTypeWithWhitespace(): void {
        $testCases = [
            '  ' . TypeName::LIST->value . ' ( ' . TypeName::UTF8->value . ' ) ',
            TypeName::LIST->value . '( ' . TypeName::UTF8->value . ' )',
            TypeName::LIST->value . '(' . TypeName::UTF8->value . ' )',
        ];

        foreach ($testCases as $typeString) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(ListCollectionInfo::class, $result);
            $this->assertEquals(Type::VARCHAR, $result->valueType->type);
        }
    }

    // =============================================
    // MAP TYPE TESTS
    // =============================================

    public function testMapTypeErrorMissingClosingBracket(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_MISSING_CLOSING->value);

        $this->parser->parse(TypeName::MAP->value . '(' . TypeName::UTF8->value . '(');
    }

    public function testMapTypeErrorNoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_MAP_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::MAP->value . '()');
    }

    public function testMapTypeErrorOneParam(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_MAP_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::MAP->value . '(' . TypeName::UTF8->value . ')');
    }

    public function testMapTypeErrorThreeParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_MAP_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::MAP->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ',' . TypeName::BOOLEAN->value . ')');
    }

    public function testMapTypes(): void {
        $testCases = [
            [TypeName::MAP->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ')', Type::VARCHAR, Type::INT, false],
            [TypeName::MAP->value . '(' . TypeName::UUID->value . ',' . TypeName::UTF8->value . ')', Type::UUID, Type::VARCHAR, false],
            [TypeName::MAP->value . '(' . TypeName::INT32->value . ',' . TypeName::BOOLEAN->value . ')', Type::INT, Type::BOOLEAN, false],
        ];

        foreach ($testCases as [$typeString, $expectedKeyType, $expectedValueType, $isFrozen]) {
            $result = $this->parser->parse($typeString, $isFrozen);
            $this->assertInstanceOf(MapCollectionInfo::class, $result);
            $this->assertEquals($isFrozen, $result->isFrozen);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result->keyType);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result->valueType);
            $this->assertEquals($expectedKeyType, $result->keyType->type);
            $this->assertEquals($expectedValueType, $result->valueType->type);
        }
    }

    // =============================================
    // PARAMETER EXTRACTION TESTS
    // =============================================

    public function testParameterExtraction(): void {
        $testCases = [
            ['', []],
            ['param1', ['param1']],
            ['param1,param2,param3', ['param1', 'param2', 'param3']],
            ['key1:value1,key2:value2', ['key1' => 'value1', 'key2' => 'value2']],
            [
                'org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)',
                [
                    'org.apache.cassandra.db.marshal.UTF8Type',
                    'org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)',
                ],
            ],
            [
                'keyspace,typename,field1:org.apache.cassandra.db.marshal.UTF8Type,field2:org.apache.cassandra.db.marshal.Int32Type',
                [
                    'keyspace',
                    'typename',
                    'field1' => 'org.apache.cassandra.db.marshal.UTF8Type',
                    'field2' => 'org.apache.cassandra.db.marshal.Int32Type',
                ],
            ],
        ];

        $reflection = new ReflectionClass($this->parser);
        $method = $reflection->getMethod('extractParams');
        $method->setAccessible(true);

        foreach ($testCases as [$paramString, $expectedParams]) {
            $result = $method->invoke($this->parser, $paramString);
            $this->assertEquals($expectedParams, $result);
        }
    }

    public function testParameterExtractionDoubleColonThrowsException(): void {
        $reflection = new ReflectionClass($this->parser);
        $method = $reflection->getMethod('extractParams');
        $method->setAccessible(true);

        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_UDT_PARAMS_MULTIPLE_COLONS->value);

        $method->invoke($this->parser, 'key:value:extra');
    }

    public function testParameterExtractionWithComplexNesting(): void {
        $reflection = new ReflectionClass($this->parser);
        $method = $reflection->getMethod('extractParams');
        $method->setAccessible(true);

        $complexCases = [
            [
                'list(map(text,set(int)))',
                ['list(map(text,set(int)))'],
            ],
            [
                'param1,list(set(text)),param3',
                ['param1', 'list(set(text))', 'param3'],
            ],
            [
                'key:list(map(text,int))',
                ['key' => 'list(map(text,int))'],
            ],
            [
                'a,b(c,d(e)),f:g(h,i)',
                ['a', 'b(c,d(e))', 'f' => 'g(h,i)'],
            ],
        ];

        foreach ($complexCases as [$paramString, $expectedParams]) {
            $result = $method->invoke($this->parser, $paramString);
            $this->assertEquals($expectedParams, $result);
        }
    }

    public function testParameterExtractionWithEdgeCases(): void {
        $reflection = new ReflectionClass($this->parser);
        $method = $reflection->getMethod('extractParams');
        $method->setAccessible(true);

        $edgeCases = [
            [' param1 ', ['param1']],
            ['param1 , param2', ['param1', 'param2']],
            [' key : value ', ['key' => 'value']],
            ['param()', ['param()']],
            ['param((nested))', ['param((nested))']],
        ];

        foreach ($edgeCases as [$paramString, $expectedParams]) {
            $result = $method->invoke($this->parser, $paramString);
            $this->assertEquals($expectedParams, $result);
        }
    }

    // =============================================
    // REVERSED TYPE TESTS
    // =============================================

    public function testReversedType(): void {
        $testCases = [
            [TypeName::REVERSED->value . '(' . TypeName::UTF8->value . ')', Type::VARCHAR],
            [TypeName::REVERSED->value . '(' . TypeName::INT32->value . ')', Type::INT],
            [TypeName::REVERSED->value . '(' . TypeName::UUID->value . ')', Type::UUID],
        ];

        foreach ($testCases as [$typeString, $expectedType]) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result);
            $this->assertEquals($expectedType, $result->type);
        }
    }

    public function testReversedTypeErrorNoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_REVERSED_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::REVERSED->value . '()');
    }

    public function testReversedTypeErrorTooManyParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_REVERSED_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::REVERSED->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ')');
    }

    // =============================================
    // SET TYPE TESTS
    // =============================================

    public function testSetTypeErrorNoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SET_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::SET->value . '()');
    }

    public function testSetTypeErrorTooManyParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SET_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::SET->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ')');
    }

    public function testSetTypes(): void {
        $testCases = [
            [TypeName::SET->value . '(' . TypeName::UTF8->value . ')', Type::VARCHAR, false],
            [TypeName::SET->value . '(' . TypeName::UUID->value . ')', Type::UUID, false],
            [TypeName::SET->value . '(' . TypeName::INT32->value . ')', Type::INT, false],
        ];

        foreach ($testCases as [$typeString, $expectedElementType, $isFrozen]) {
            $result = $this->parser->parse($typeString, $isFrozen);
            $this->assertInstanceOf(SetCollectionInfo::class, $result);
            $this->assertEquals($isFrozen, $result->isFrozen);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result->valueType);
            $this->assertEquals($expectedElementType, $result->valueType->type);
        }
    }

    // =============================================
    // SIMPLE TYPE TESTS
    // =============================================

    public function testSimpleTypeErrorBooleanWithInvalidParam(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_WITH_PARAMETERS->value);

        $this->parser->parse(TypeName::BOOLEAN->value . '(invalidParam)');
    }

    public function testSimpleTypeErrorInt32WithTwoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_WITH_PARAMETERS->value);

        $this->parser->parse(TypeName::INT32->value . '(param1,param2)');
    }

    public function testSimpleTypeErrorUTF8ClosingBracketWithoutOpening(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_CLOSING_WITHOUT_OPENING->value);

        $this->parser->parse(TypeName::UTF8->value . ')');
    }

    public function testSimpleTypeErrorUTF8WithSomeParam(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_SIMPLE_TYPE_WITH_PARAMETERS->value);

        $this->parser->parse(TypeName::UTF8->value . '(someParam)');
    }

    public function testSimpleTypes(): void {
        foreach (self::SIMPLE_TYPES as [$typeName, $expectedType]) {
            $result = $this->parser->parse($typeName->value);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result);
            $this->assertEquals($expectedType, $result->type);
        }
    }

    public function testSimpleTypeUTF8WithEmptyParams(): void {

        $result = $this->parser->parse(TypeName::UTF8->value . '()');

        $this->assertInstanceOf(SimpleTypeInfo::class, $result);
        $this->assertEquals(Type::VARCHAR, $result->type);
    }

    public function testSimpleTypeWithWhitespace(): void {
        $testCases = [
            '  ' . TypeName::UTF8->value . '  ',
            ' ' . TypeName::UTF8->value,
            TypeName::UTF8->value . ' ',
            "\t" . TypeName::UTF8->value . "\t",
            "\n" . TypeName::UTF8->value . "\n",
        ];

        foreach ($testCases as $typeString) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result);
            $this->assertEquals(Type::VARCHAR, $result->type);
        }
    }

    // =============================================
    // TUPLE TYPE TESTS
    // =============================================

    public function testTupleTypeErrorClosingBracketWithoutOpening(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_INVALID_BRACKETS_CLOSING_WITHOUT_OPENING->value);

        $this->parser->parse('type)');
    }

    public function testTupleTypeErrorInvalidParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_TUPLE_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::TUPLE->value . '()');
    }

    public function testTupleTypeErrorWithExcessiveWhitespace(): void {
        $typeString = "  \t\n  " . TypeName::LIST->value . "  \t\n  ( \t\n " . TypeName::UTF8->value . " \t\n ) \t\n  ";
        $result = $this->parser->parse($typeString);

        $this->assertInstanceOf(ListCollectionInfo::class, $result);
        $this->assertInstanceOf(SimpleTypeInfo::class, $result->valueType);
        $this->assertEquals(Type::VARCHAR, $result->valueType->type);
    }

    public function testTupleTypes(): void {
        $testCases = [
            [TypeName::TUPLE->value . '(' . TypeName::UTF8->value . ')', [Type::VARCHAR]],
            [TypeName::TUPLE->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ')', [Type::VARCHAR, Type::INT]],
            [TypeName::TUPLE->value . '(' . TypeName::UTF8->value . ',' . TypeName::INT32->value . ',' . TypeName::BOOLEAN->value . ')', [Type::VARCHAR, Type::INT, Type::BOOLEAN]],
            [TypeName::TUPLE->value . '(' . TypeName::UUID->value . ',' . TypeName::TIMESTAMP->value . ')', [Type::UUID, Type::TIMESTAMP]],
        ];

        foreach ($testCases as [$typeString, $expectedTypes]) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(TupleInfo::class, $result);
            $this->assertCount(count($expectedTypes), $result->valueTypes);

            foreach ($result->valueTypes as $index => $valueType) {
                $this->assertInstanceOf(SimpleTypeInfo::class, $valueType);
                $this->assertEquals($expectedTypes[$index], $valueType->type);
            }
        }
    }

    // =============================================
    // UDT TYPE TESTS
    // =============================================

    public function testUDTTypeErrorNonStringFieldKey(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_UDT_FIELD_KEY_NOT_STRING->value);

        $this->parser->parse(TypeName::UDT->value . '(mykeyspace,mytype,' . TypeName::UTF8->value . ')');
    }

    public function testUDTTypeErrorNoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_UDT_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::UDT->value . '()');
    }

    public function testUDTTypeErrorOneParam(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_UDT_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::UDT->value . '(ks)');
    }

    public function testUDTTypeErrorTwoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_UDT_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::UDT->value . '(ks,type)');
    }

    public function testUDTTypes(): void {
        $testCases = [
            [
                TypeName::UDT->value . '(mykeyspace,mytype,field1:' . TypeName::UTF8->value . ',field2:' . TypeName::INT32->value . ')',
                'mykeyspace',
                'mytype',
                ['field1' => Type::VARCHAR, 'field2' => Type::INT],
            ],
            [
                TypeName::UDT->value . '(testkeyspace,testtype,name:' . TypeName::UTF8->value . ',age:' . TypeName::INT32->value . ',active:' . TypeName::BOOLEAN->value . ')',
                'testkeyspace',
                'testtype',
                ['name' => Type::VARCHAR, 'age' => Type::INT, 'active' => Type::BOOLEAN],
            ],
        ];

        foreach ($testCases as [$typeString, $expectedKeyspace, $expectedName, $expectedFieldTypes]) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(UDTInfo::class, $result);
            $this->assertEquals($expectedKeyspace, $result->keyspace);
            $this->assertEquals($expectedName, $result->name);
            $this->assertCount(count($expectedFieldTypes), $result->valueTypes);

            foreach ($expectedFieldTypes as $fieldName => $expectedType) {
                $this->assertArrayHasKey($fieldName, $result->valueTypes);
                $this->assertInstanceOf(SimpleTypeInfo::class, $result->valueTypes[$fieldName]);
                $this->assertEquals($expectedType, $result->valueTypes[$fieldName]->type);
            }
        }
    }

    public function testUDTTypeWithHexEncodedNames(): void {
        $typeString = TypeName::UDT->value . '(foo,696e617070726f7072696174655f666565646261636a,617574686f725f66696e6765727072696e75:' . TypeName::UTF8->value . ',7375626d697373696f6e5f7473:' . TypeName::TIMESTAMP->value . ',726561736f6e5f74657874:' . TypeName::UTF8->value . ')';

        $result = $this->parser->parse($typeString);
        $this->assertInstanceOf(UDTInfo::class, $result);
        $this->assertEquals('foo', $result->keyspace);
        $this->assertEquals('696e617070726f7072696174655f666565646261636a', $result->name);

        $expectedFields = [
            '617574686f725f66696e6765727072696e75' => Type::VARCHAR,
            '7375626d697373696f6e5f7473' => Type::TIMESTAMP,
            '726561736f6e5f74657874' => Type::VARCHAR,
        ];

        foreach ($expectedFields as $fieldName => $expectedType) {
            $this->assertArrayHasKey($fieldName, $result->valueTypes);
            $this->assertEquals($expectedType, $result->valueTypes[$fieldName]->type);
        }
    }

    // =============================================
    // VECTOR TYPE TESTS
    // =============================================

    public function testVectorTypeErrorNegativeDimensions(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_VECTOR_DIMENSIONS_OUT_OF_RANGE->value);

        $this->parser->parse(TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',-1)');
    }

    public function testVectorTypeErrorNegativeDimensionsMinusFive(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_VECTOR_DIMENSIONS_OUT_OF_RANGE->value);

        $this->parser->parse(TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',-5)');
    }

    public function testVectorTypeErrorNonNumericDimensions(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_VECTOR_DIMENSIONS_NON_NUMERIC->value);

        $this->parser->parse(TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',abc)');
    }

    public function testVectorTypeErrorNonNumericDimensionsNotANumber(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_VECTOR_DIMENSIONS_NON_NUMERIC->value);

        $this->parser->parse(TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',not_a_number)');
    }

    public function testVectorTypeErrorNoParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_VECTOR_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::VECTOR->value . '()');
    }

    public function testVectorTypeErrorOneParam(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_VECTOR_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ')');
    }

    public function testVectorTypeErrorThreeParams(): void {
        $this->expectException(Exception::class);
        $this->expectExceptionCode(ExceptionCode::TYPENAME_PARSER_VECTOR_INVALID_PARAM_COUNT->value);

        $this->parser->parse(TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',' . TypeName::INT32->value . ',' . TypeName::BOOLEAN->value . ')');
    }

    public function testVectorTypes(): void {
        $testCases = [
            [TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',128)', Type::FLOAT, 128],
            [TypeName::VECTOR->value . '(' . TypeName::DOUBLE->value . ',256)', Type::DOUBLE, 256],
            [TypeName::VECTOR->value . '(' . TypeName::INT32->value . ',10)', Type::INT, 10],
            [TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',1)', Type::FLOAT, 1],
            [TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',999999)', Type::FLOAT, 999999],
        ];

        foreach ($testCases as [$typeString, $expectedElementType, $expectedDimensions]) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(VectorInfo::class, $result);
            $this->assertInstanceOf(SimpleTypeInfo::class, $result->valueType);
            $this->assertEquals($expectedElementType, $result->valueType->type);
            $this->assertEquals($expectedDimensions, $result->dimensions);
        }
    }

    public function testVectorTypeWithBoundaryDimensions(): void {
        $testCases = [
            [TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',1)', 1],
            [TypeName::VECTOR->value . '(' . TypeName::FLOAT->value . ',2147483647)', 2147483647], // Max int
        ];

        foreach ($testCases as [$typeString, $expectedDimensions]) {
            $result = $this->parser->parse($typeString);
            $this->assertInstanceOf(VectorInfo::class, $result);
            $this->assertEquals($expectedDimensions, $result->dimensions);
        }
    }
}
