<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Type;
use Cassandra\ValueFactory;

final class TypeInfoDefinitionValidationTest extends AbstractUnitTestCase {
    public function testInvalidFrozenMarkerIsRejectedForEveryComplexTypeThatCarriesIt(): void {
        $definitions = [
            [
                'type' => Type::LIST,
                'valueType' => Type::INT,
                'isFrozen' => 'true',
            ],
            [
                'type' => Type::SET,
                'valueType' => Type::INT,
                'isFrozen' => 1,
            ],
            [
                'type' => Type::MAP,
                'keyType' => Type::VARCHAR,
                'valueType' => Type::INT,
                'isFrozen' => null,
            ],
            [
                'type' => Type::UDT,
                'valueTypes' => ['field' => Type::INT],
                'isFrozen' => [],
            ],
        ];

        foreach ($definitions as $definition) {
            try {
                ValueFactory::getTypeInfoFromTypeDefinition($definition);
                $this->fail('Expected the invalid isFrozen marker to be rejected');
            } catch (TypeInfoException $e) {
                $this->assertSame(ExceptionCode::TYPEINFO_INVALID_IS_FROZEN->value, $e->getCode());
                $this->assertArrayHasKey('actual_type', $e->getContext());
            }
        }
    }

    public function testInvalidUdtKeyspaceIsRejectedInsteadOfBecomingUnnamed(): void {
        try {
            ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::UDT,
                'valueTypes' => ['field' => Type::INT],
                'keyspace' => [],
            ]);
            $this->fail('Expected the invalid UDT keyspace to be rejected');
        } catch (TypeInfoException $e) {
            $this->assertSame(ExceptionCode::TYPEINFO_UDT_INVALID_KEYSPACE->value, $e->getCode());
            $this->assertSame('keyspace', $e->getContext()['property'] ?? null);
        }
    }

    public function testInvalidUdtNameIsRejectedInsteadOfBecomingUnnamed(): void {
        try {
            ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::UDT,
                'valueTypes' => ['field' => Type::INT],
                'name' => 42,
            ]);
            $this->fail('Expected the invalid UDT name to be rejected');
        } catch (TypeInfoException $e) {
            $this->assertSame(ExceptionCode::TYPEINFO_UDT_INVALID_NAME->value, $e->getCode());
            $this->assertSame('name', $e->getContext()['property'] ?? null);
        }
    }
}
