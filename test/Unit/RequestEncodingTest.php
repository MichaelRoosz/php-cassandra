<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;
use Cassandra\Request\QueryFlag;
use Cassandra\SerialConsistency;

/**
 * Unit tests for the binary encoding of request options.
 */
final class RequestEncodingTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{SerialConsistency}>
     */
    public static function serialConsistencyProvider(): array {
        return [
            'SERIAL' => [SerialConsistency::SERIAL],
            'LOCAL_SERIAL' => [SerialConsistency::LOCAL_SERIAL],
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('serialConsistencyProvider')]
    public function testBatchEncodesSerialConsistencyValue(SerialConsistency $serial): void {
        $batch = new Batch(
            type: BatchType::LOGGED,
            consistency: Consistency::ONE,
            options: new BatchOptions(serialConsistency: $serial)
        );
        $batch->setVersion(ProtocolVersion::V4);
        $batch->appendQuery('UPDATE t SET v = 1 WHERE id = 1 IF v = 0');

        $body = $batch->getBody();
        $tail = substr($body, -2);

        $this->assertSame(
            pack('n', $serial->value),
            $tail,
            'Batch must encode the serial consistency code, not the enum instance'
        );
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('serialConsistencyProvider')]
    public function testQueryEncodesSerialConsistencyValue(SerialConsistency $serial): void {
        $request = new Query(
            query: 'UPDATE t SET v = ? WHERE id = ? IF v = ?',
            values: [],
            consistency: Consistency::ONE,
            options: new QueryOptions(serialConsistency: $serial)
        );
        $request->setVersion(ProtocolVersion::V5);

        $body = $request->getBody();

        // body = [int query length][query][short consistency][int flags][short serial consistency]
        $offset = 4 + strlen('UPDATE t SET v = ? WHERE id = ? IF v = ?');

        $this->assertSame(
            Consistency::ONE->value,
            $this->unpackInt('n', substr($body, $offset, 2))
        );
        $this->assertSame(
            QueryFlag::WITH_SERIAL_CONSISTENCY,
            $this->unpackInt('N', substr($body, $offset + 2, 4)) & QueryFlag::WITH_SERIAL_CONSISTENCY,
            'The serial consistency flag must be set'
        );
        $this->assertSame(
            $serial->value,
            $this->unpackInt('n', substr($body, $offset + 6, 2)),
            'Query must encode the serial consistency code, not the enum instance'
        );
    }

    public function testQueryOmitsSerialConsistencyWhenNotRequested(): void {
        $request = new Query(
            query: 'SELECT * FROM t',
            values: [],
            consistency: Consistency::ONE,
            options: new QueryOptions()
        );
        $request->setVersion(ProtocolVersion::V5);

        $body = $request->getBody();
        $offset = 4 + strlen('SELECT * FROM t');

        $this->assertSame(
            0,
            $this->unpackInt('N', substr($body, $offset + 2, 4)) & QueryFlag::WITH_SERIAL_CONSISTENCY,
            'The serial consistency flag must not be set when the option is unused'
        );
    }

    private function unpackInt(string $format, string $bytes): int {
        $unpacked = unpack($format, $bytes);

        $this->assertIsArray($unpacked);
        $this->assertIsInt($unpacked[1]);

        return $unpacked[1];
    }
}
