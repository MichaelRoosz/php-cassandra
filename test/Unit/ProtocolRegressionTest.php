<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Exception\ServerException\ServerErrorException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Error\Context\WriteType;
use Cassandra\Response\Error\ServerError;
use Cassandra\Response\Error\WriteTimeoutError;
use Cassandra\Response\ErrorType;
use Cassandra\Response\StreamReader;

final class ProtocolRegressionTest extends AbstractUnitTestCase {
    public function testServerErrorUsesDedicatedExceptionClass(): void {
        $body = self::errorPrefix(ErrorType::SERVER_ERROR, 'server failed');
        $error = new ServerError(
            self::header(ProtocolVersion::V4, Opcode::RESPONSE_ERROR, strlen($body)),
            new StreamReader($body),
        );

        $this->assertInstanceOf(ServerErrorException::class, $error->getException());
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
