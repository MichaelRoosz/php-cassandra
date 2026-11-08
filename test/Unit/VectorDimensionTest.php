<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\Value\Vector;

final class VectorDimensionTest extends AbstractUnitTestCase {
    public function testAMatchingListIsEncoded(): void {
        $this->assertSame(pack('NN', 1, 2), Vector::fromValue([1, 2], Type::INT, 2)->getBinary());
    }

    public function testAnAssociativeArrayIsRefused(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VECTOR_INVALID_VALUE_TYPE->value);
        Vector::fromValue(['a' => 1, 'b' => 2], Type::INT, 2);
    }
    public function testMoreValuesThanDimensionsAreRefused(): void {
        // Regression: the extra values were dropped without a word.
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VECTOR_DIMENSION_MISMATCH->value);
        Vector::fromValue([1, 2, 3], Type::INT, 2);
    }
}
