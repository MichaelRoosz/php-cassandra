<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException\ServerErrorException;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Error\Context\WriteType;
use Cassandra\Response\Error\ServerError;
use Cassandra\Response\Error\WriteTimeoutError;
use Cassandra\Response\ErrorType;
use Cassandra\Response\Event\Data\TopologyChangeType;
use Cassandra\Response\Event\TopologyChangeEvent;
use Cassandra\Response\Ready;
use Cassandra\Response\StreamReader;

final class ProtocolRegressionTest extends AbstractUnitTestCase {
    public function testMovedNodeTopologyEventIsRejectedAfterProtocolV3(): void {
        $body = self::encodeString('TOPOLOGY_CHANGE')
            . self::encodeString(TopologyChangeType::MOVED_NODE->value)
            . chr(4) . inet_pton('127.0.0.1') . pack('N', 9042);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_EVENT_TOPOLOGY_CHANGE_INVALID_TYPE->value);

        new TopologyChangeEvent(
            self::header(ProtocolVersion::V4, Opcode::RESPONSE_EVENT, strlen($body), stream: -1),
            new StreamReader($body),
        );
    }

    public function testServerErrorUsesDedicatedExceptionClass(): void {
        $body = self::errorPrefix(ErrorType::SERVER_ERROR, 'server failed');
        $error = new ServerError(
            self::header(ProtocolVersion::V4, Opcode::RESPONSE_ERROR, strlen($body)),
            new StreamReader($body),
        );

        $this->assertInstanceOf(ServerErrorException::class, $error->getException());
    }

    public function testStringifyingResponseRetainsFlaggedEnvelopeData(): void {
        $flags = Flag::TRACING | Flag::WARNING | Flag::CUSTOM_PAYLOAD;
        $body = pack('H*', '00112233445566778899aabbccddeeff')
            . pack('n', 1) . self::encodeString('warning')
            . pack('n', 1) . self::encodeString('key') . pack('N', 5) . 'value';
        $header = self::header(ProtocolVersion::V5, Opcode::RESPONSE_READY, strlen($body), flags: $flags);

        $response = new Ready($header, new StreamReader($body));

        $this->assertSame('', $response->getBody());
        $this->assertSame(
            pack('CCnCN', 0x85, $flags, 0, Opcode::RESPONSE_READY->value, strlen($body)) . $body,
            (string) $response,
        );
    }

    public function testV3MovedNodeTopologyEventIsAccepted(): void {
        $body = self::encodeString('TOPOLOGY_CHANGE')
            . self::encodeString(TopologyChangeType::MOVED_NODE->value)
            . chr(4) . inet_pton('127.0.0.1') . pack('N', 9042);

        $event = new TopologyChangeEvent(
            self::header(ProtocolVersion::V3, Opcode::RESPONSE_EVENT, strlen($body), stream: -1),
            new StreamReader($body),
        );

        $this->assertSame(TopologyChangeType::MOVED_NODE, $event->getTopologyChangeData()->changeType);
        $this->assertSame(['ip' => '127.0.0.1', 'port' => 9042], $event->getTopologyChangeData()->address);
    }

    public function testV5CasWriteTimeoutReadsContentions(): void {
        $body = self::errorPrefix(ErrorType::WRITE_TIMEOUT, 'timed out')
            . pack('n', Consistency::QUORUM->value)
            . pack('N', 1)
            . pack('N', 2)
            . self::encodeString(WriteType::CAS->value)
            . pack('n', 7);

        $error = new WriteTimeoutError(
            self::header(ProtocolVersion::V5, Opcode::RESPONSE_ERROR, strlen($body)),
            new StreamReader($body),
        );

        $this->assertSame(7, $error->getContext()->contentions);
    }

    public function testV5NonCasWriteTimeoutDoesNotReadContentions(): void {
        $body = self::errorPrefix(ErrorType::WRITE_TIMEOUT, 'timed out')
            . pack('n', Consistency::QUORUM->value)
            . pack('N', 1)
            . pack('N', 2)
            . self::encodeString(WriteType::SIMPLE->value);

        $error = new WriteTimeoutError(
            self::header(ProtocolVersion::V5, Opcode::RESPONSE_ERROR, strlen($body)),
            new StreamReader($body),
        );

        $this->assertSame(WriteType::SIMPLE, $error->getContext()->writeType);
        $this->assertNull($error->getContext()->contentions);
    }

    private static function encodeString(string $value): string {
        return pack('n', strlen($value)) . $value;
    }

    private static function errorPrefix(ErrorType $type, string $message): string {
        return pack('N', $type->value) . self::encodeString($message);
    }

    private static function header(
        ProtocolVersion $version,
        Opcode $opcode,
        int $length,
        int $flags = 0,
        int $stream = 0,
    ): Header {
        return new Header(
            version: $version,
            flags: $flags,
            stream: $stream,
            opcode: $opcode,
            length: $length,
        );
    }
}
