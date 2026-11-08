<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Query;

final class UntypedIntValueTest extends AbstractUnitTestCase {
    public function testAnIntBeyond32BitsIsRefusedInsteadOfTruncated(): void {
        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }

        // Regression: 5000000000 was sent as 705032704.
        $query = new Query('SELECT * FROM t WHERE k = ?', [5000000000]);
        $query->setVersion(ProtocolVersion::V4);
        $query->setStream(1);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_INT_OUT_OF_INT32_RANGE->value);
        $query->getBody();
    }

    public function testThe32BitRangeIsStillSent(): void {
        $query = new Query('SELECT * FROM t WHERE k = ? AND j = ?', [2147483647, -2147483648]);
        $query->setVersion(ProtocolVersion::V4);
        $query->setStream(1);

        $this->assertStringContainsString(pack('N', 4) . pack('N', 2147483647) . pack('N', 4) . "\x80\x00\x00\x00", $query->getBody());
    }
}
