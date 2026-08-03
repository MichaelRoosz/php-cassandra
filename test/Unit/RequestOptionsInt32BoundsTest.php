<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Batch;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;

/**
 * The request options that go out as a signed 32-bit `[int]` must refuse a
 * value that does not fit in one.
 *
 * pack('N', …) takes the low four bytes of whatever it is given without
 * complaint, so an unchecked value does not fail — it reaches the coordinator
 * as a different number. A page size of 2^31 arrives as -2^31, and one of
 * 2^32 + 100 arrives as 100: a query that silently runs with a page size the
 * caller never asked for, with nothing to show for it.
 *
 * The same reasoning as Request::MAX_SHORT_COUNT and the int32 bound that
 * Request::encodeQueryValuesAsBinary() holds bound integer values to.
 */
final class RequestOptionsInt32BoundsTest extends AbstractUnitTestCase {
    private const INT32_MAX = 2147483647;
    private const INT32_MIN = -2147483647 - 1;

    protected function setUp(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Out-of-int32-range values are not representable on a 32-bit build');
        }
    }

    /**
     * @return array<string, array{int}>
     */
    public static function outOfRangeNowInSecondsProvider(): array {
        return [
            'just above int32 max' => [self::INT32_MAX + 1],
            'just below int32 min' => [self::INT32_MIN - 1],
            'truncates to zero' => [4294967296],
            'truncates to another value' => [4294967396],
        ];
    }

    /**
     * @return array<string, array{int}>
     */
    public static function outOfRangePageSizeProvider(): array {
        return [
            'just above int32 max' => [self::INT32_MAX + 1],
            'truncates to zero' => [4294967296],
            'truncates to a smaller page size' => [4294967396],
            'truncates to minus one' => [PHP_INT_MAX],
        ];
    }

    public function testBatchOptionsAcceptNowInSecondsAtTheInt32Bounds(): void {
        $this->assertSame(self::INT32_MAX, (new BatchOptions(nowInSeconds: self::INT32_MAX))->nowInSeconds);
        $this->assertSame(self::INT32_MIN, (new BatchOptions(nowInSeconds: self::INT32_MIN))->nowInSeconds);
    }

    /**
     * @dataProvider outOfRangeNowInSecondsProvider
     */
    public function testBatchOptionsRejectOutOfRangeNowInSeconds(int $nowInSeconds): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_NOW_IN_SECONDS->value);

        new BatchOptions(nowInSeconds: $nowInSeconds);
    }

    public function testEncodedBatchNowInSecondsIsNeverTruncated(): void {
        $batch = new Batch(options: new BatchOptions(nowInSeconds: self::INT32_MIN));
        $batch->appendQuery('INSERT INTO t (a) VALUES (1)');
        $batch->setStream(0);
        $batch->setVersion(ProtocolVersion::V5);

        $body = $batch->getBody();

        $this->assertSame(self::INT32_MIN, self::readInt32(substr($body, -4)));
    }

    public function testEncodedQueryOptionsAreNeverTruncated(): void {
        $query = new Query(
            'SELECT * FROM t',
            [],
            Consistency::ONE,
            new QueryOptions(pageSize: self::INT32_MAX, nowInSeconds: self::INT32_MIN),
        );
        $query->setStream(0);
        $query->setVersion(ProtocolVersion::V5);

        $body = $query->getBody();

        // [long string query][short consistency][int flags], then the optional
        // fields in flag order: page size first, now_in_seconds last.
        $offset = 4 + strlen('SELECT * FROM t') + 2 + 4;

        $this->assertSame(self::INT32_MAX, self::readInt32(substr($body, $offset, 4)));
        $this->assertSame(self::INT32_MIN, self::readInt32(substr($body, $offset + 4, 4)));
    }

    /**
     * @dataProvider outOfRangeNowInSecondsProvider
     */
    public function testExecuteOptionsRejectOutOfRangeNowInSeconds(int $nowInSeconds): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_NOW_IN_SECONDS->value);

        new ExecuteOptions(nowInSeconds: $nowInSeconds);
    }

    /**
     * @dataProvider outOfRangePageSizeProvider
     */
    public function testExecuteOptionsRejectOutOfRangePageSize(int $pageSize): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_PAGE_SIZE->value);

        new ExecuteOptions(pageSize: $pageSize);
    }

    public function testQueryOptionsAcceptNowInSecondsAtTheInt32Bounds(): void {
        $this->assertSame(self::INT32_MAX, (new QueryOptions(nowInSeconds: self::INT32_MAX))->nowInSeconds);
        $this->assertSame(self::INT32_MIN, (new QueryOptions(nowInSeconds: self::INT32_MIN))->nowInSeconds);
    }

    public function testQueryOptionsAcceptThePageSizeAtTheInt32Maximum(): void {
        $this->assertSame(self::INT32_MAX, (new QueryOptions(pageSize: self::INT32_MAX))->pageSize);
    }

    /**
     * @dataProvider outOfRangeNowInSecondsProvider
     */
    public function testQueryOptionsRejectOutOfRangeNowInSeconds(int $nowInSeconds): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_NOW_IN_SECONDS->value);

        new QueryOptions(nowInSeconds: $nowInSeconds);
    }

    /**
     * @dataProvider outOfRangePageSizeProvider
     */
    public function testQueryOptionsRejectOutOfRangePageSize(int $pageSize): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_PAGE_SIZE->value);

        new QueryOptions(pageSize: $pageSize);
    }

    /**
     * The rebuilders run the constructor again, so a bound the constructor
     * enforces cannot be sidestepped by copying an options object.
     */
    public function testRebuiltOptionsKeepTheValuesTheConstructorAccepted(): void {
        $options = new QueryOptions(pageSize: self::INT32_MAX, nowInSeconds: self::INT32_MIN);

        $rebuilt = $options->withKeyspace('app');

        $this->assertSame(self::INT32_MAX, $rebuilt->pageSize);
        $this->assertSame(self::INT32_MIN, $rebuilt->nowInSeconds);
    }

    /**
     * The four bytes as the coordinator reads them: a signed 32-bit big-endian
     * integer.
     */
    private static function readInt32(string $binary): int {
        /** @var array<int> $unpacked */
        $unpacked = unpack('N', $binary);

        $shift = (PHP_INT_SIZE * 8) - 32;

        return $unpacked[1] << $shift >> $shift;
    }
}
