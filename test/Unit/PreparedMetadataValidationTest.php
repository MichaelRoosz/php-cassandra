<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\StreamReader;
use PHPUnit\Framework\Attributes\DataProvider;

final class PreparedMetadataValidationTest extends AbstractUnitTestCase {
    /**
     * @return iterable<string, array{string, ExceptionCode}>
     */
    public static function invalidCounts(): iterable {
        yield 'negative bind marker count' => [
            pack('N', 0) . pack('N', -1),
            ExceptionCode::RESPONSE_PREPARED_INVALID_BIND_MARKER_COUNT,
        ];

        yield 'negative partition key count' => [
            pack('N', 0) . pack('N', 0) . pack('N', -1),
            ExceptionCode::RESPONSE_PREPARED_INVALID_PK_COUNT,
        ];

        yield 'negative result column count' => [
            pack('N', 0) . pack('N', 0) . pack('N', 0)
                . pack('N', 0) . pack('N', -1),
            ExceptionCode::RESPONSE_RES_INVALID_COLUMNS_COUNT,
        ];

        yield 'excessive bind marker count' => [
            pack('N', 0) . pack('N', 65536),
            ExceptionCode::RESPONSE_PREPARED_INVALID_BIND_MARKER_COUNT,
        ];

        yield 'partition key count exceeds bind markers' => [
            pack('N', 0) . pack('N', 0) . pack('N', 1),
            ExceptionCode::RESPONSE_PREPARED_INVALID_PK_COUNT,
        ];

        yield 'bind marker count does not fit body' => [
            pack('N', 0) . pack('N', 1) . pack('N', 0),
            ExceptionCode::RESPONSE_PREPARED_INVALID_BIND_MARKER_COUNT,
        ];

        yield 'result column count does not fit body' => [
            pack('N', 0) . pack('N', 0) . pack('N', 0)
                . pack('N', 0) . pack('N', 1),
            ExceptionCode::RESPONSE_RES_INVALID_COLUMNS_COUNT,
        ];
    }

    #[DataProvider('invalidCounts')]
    public function testRejectsNegativePreparedMetadataCounts(string $metadata, ExceptionCode $code): void {
        $body = pack('N', 4) // result kind: PREPARED
            . pack('n', 1) . 'x' // prepared statement id
            . $metadata;
        $header = new Header(
            version: ProtocolVersion::V4,
            flags: 0,
            stream: 0,
            opcode: Opcode::RESPONSE_RESULT,
            length: strlen($body),
        );

        try {
            new PreparedResult($header, new StreamReader($body));
            $this->fail('Expected malformed prepared metadata to be rejected');
        } catch (ResponseException $e) {
            $this->assertSame($code->value, $e->getCode());
        }
    }
}
