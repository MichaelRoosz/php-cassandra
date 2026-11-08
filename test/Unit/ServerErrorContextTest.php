<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\ResponseReader;
use Cassandra\Consistency;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\ServerException\AlreadyExistsException;
use Cassandra\Exception\ServerException\CasWriteUnknownException;
use Cassandra\Exception\ServerException\FunctionFailureException;
use Cassandra\Exception\ServerException\ReadFailureException;
use Cassandra\Exception\ServerException\ReadTimeoutException;
use Cassandra\Exception\ServerException\UnavailableException;
use Cassandra\Exception\ServerException\UnpreparedException;
use Cassandra\Exception\ServerException\WriteFailureException;
use Cassandra\Exception\ServerException\WriteTimeoutException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Error;
use Cassandra\Response\Error\Context\ErrorContext;
use Cassandra\Response\Error\Context\AlreadyExistsContext;
use Cassandra\Response\Error\Context\CasWriteUnknownContext;
use Cassandra\Response\Error\Context\FunctionFailureContext;
use Cassandra\Response\Error\Context\ReadFailureContext;
use Cassandra\Response\Error\Context\ReadTimeoutContext;
use Cassandra\Response\Error\Context\UnavailableExceptionContext;
use Cassandra\Response\Error\Context\UnpreparedContext;
use Cassandra\Response\Error\Context\WriteFailureContext;
use Cassandra\Response\Error\Context\WriteTimeoutContext;
use Cassandra\Response\Error\Context\WriteType;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionMethod;

/**
 * The error details applications act on, decoded from frames laid out as the
 * native protocol specifications describe them. Most of these errors cannot be
 * provoked on a single healthy test node.
 */
final class ServerErrorContextTest extends AbstractUnitTestCase {
    public function testAlreadyExists(): void {
        $context = $this->decode(0x2400, AlreadyExistsException::class, AlreadyExistsContext::class, self::string('ks') . self::string('t'), ProtocolVersion::V4);

        $this->assertSame('ks', $context->keyspace);
        $this->assertSame('t', $context->table);
    }

    public function testCasWriteUnknown(): void {
        $context = $this->decode(0x1700, CasWriteUnknownException::class, CasWriteUnknownContext::class, pack('nNN', Consistency::SERIAL->value, 1, 2), ProtocolVersion::V5);

        $this->assertSame(Consistency::SERIAL, $context->consistency);
        $this->assertSame(1, $context->nodesAcknowledged);
        $this->assertSame(2, $context->nodesRequired);
    }

    public function testFunctionFailure(): void {
        $body = self::string('ks') . self::string('fn') . pack('n', 2) . self::string('int') . self::string('text');
        $context = $this->decode(0x1400, FunctionFailureException::class, FunctionFailureContext::class, $body, ProtocolVersion::V4);

        $this->assertSame('ks', $context->keyspace);
        $this->assertSame('fn', $context->function);
        $this->assertSame(['int', 'text'], $context->argTypes);
    }

    public function testReadFailureOnV4CarriesACount(): void {
        $context = $this->decode(0x1300, ReadFailureException::class, ReadFailureContext::class, pack('nNNN', Consistency::ALL->value, 1, 3, 2) . "\x01", ProtocolVersion::V4);

        $this->assertSame(Consistency::ALL, $context->consistency);
        $this->assertSame(1, $context->nodesAnswered);
        $this->assertSame(3, $context->nodesRequired);
        $this->assertSame(2, $context->numFailures);
        $this->assertNull($context->reasonMap);
        $this->assertTrue($context->dataPresent);
    }

    public function testReadFailureOnV5CarriesReasons(): void {
        $reasons = pack('N', 2) . "\x04\x0A\0\0\x01" . pack('n', 1) . "\x10" . str_repeat("\0", 15) . "\x01" . pack('n', 2);
        $context = $this->decode(0x1300, ReadFailureException::class, ReadFailureContext::class, pack('nNN', Consistency::ALL->value, 1, 3) . $reasons . "\0", ProtocolVersion::V5);

        $this->assertSame(['10.0.0.1' => 1, '::1' => 2], $context->reasonMap);
        $this->assertFalse($context->dataPresent);
    }

    #[DataProvider('versions')]
    public function testReadTimeout(ProtocolVersion $version): void {
        $context = $this->decode(0x1200, ReadTimeoutException::class, ReadTimeoutContext::class, pack('nNN', Consistency::QUORUM->value, 1, 2) . "\x05", $version);

        $this->assertSame(Consistency::QUORUM, $context->consistency);
        $this->assertSame(1, $context->nodesAnswered);
        $this->assertSame(2, $context->nodesRequired);
        $this->assertTrue($context->dataPresent, 'any non-zero byte means the data replica answered');

        $absent = $this->decode(0x1200, ReadTimeoutException::class, ReadTimeoutContext::class, pack('nNN', Consistency::ONE->value, 0, 1) . "\0", $version);
        $this->assertFalse($absent->dataPresent);
    }

    #[DataProvider('versions')]
    public function testUnavailable(ProtocolVersion $version): void {
        $context = $this->decode(0x1000, UnavailableException::class, UnavailableExceptionContext::class, pack('nNN', Consistency::LOCAL_QUORUM->value, 3, 1), $version);

        $this->assertSame(Consistency::LOCAL_QUORUM, $context->consistency);
        $this->assertSame(3, $context->nodesRequired);
        $this->assertSame(1, $context->nodesAlive);
    }

    public function testUnprepared(): void {
        $context = $this->decode(0x2500, UnpreparedException::class, UnpreparedContext::class, pack('n', 4) . "\x01\x02\x03\x04", ProtocolVersion::V4);

        $this->assertSame("\x01\x02\x03\x04", $context->unknownStatementId);
    }

    public function testWriteFailureOnV4(): void {
        $body = pack('nNNN', Consistency::TWO->value, 1, 2, 1) . self::string('BATCH_LOG');
        $context = $this->decode(0x1500, WriteFailureException::class, WriteFailureContext::class, $body, ProtocolVersion::V4);

        $this->assertSame(1, $context->numFailures);
        $this->assertSame(WriteType::BATCH_LOG, $context->writeType);
    }

    public function testWriteFailureOnV5(): void {
        $body = pack('nNN', Consistency::TWO->value, 1, 2) . pack('N', 1) . "\x04\x0A\0\0\x02" . pack('n', 3) . self::string('COUNTER');
        $context = $this->decode(0x1500, WriteFailureException::class, WriteFailureContext::class, $body, ProtocolVersion::V5);

        $this->assertSame(['10.0.0.2' => 3], $context->reasonMap);
        $this->assertSame(WriteType::COUNTER, $context->writeType);
    }

    #[DataProvider('versions')]
    public function testWriteTimeout(ProtocolVersion $version): void {
        foreach (WriteType::cases() as $type) {
            $body = pack('nNN', Consistency::EACH_QUORUM->value, 2, 3) . self::string($type->value);
            if ($type === WriteType::CAS && $version === ProtocolVersion::V5) {
                $body .= pack('n', 0);
            }
            $context = $this->decode(0x1100, WriteTimeoutException::class, WriteTimeoutContext::class, $body, $version);

            $this->assertSame(Consistency::EACH_QUORUM, $context->consistency);
            $this->assertSame(2, $context->nodesAcknowledged);
            $this->assertSame(3, $context->nodesRequired);
            $this->assertSame($type, $context->writeType);
        }
    }

    public function testWriteTimeoutForCasOnV5CarriesContentions(): void {
        $body = pack('nNN', Consistency::SERIAL->value, 0, 2) . self::string('CAS') . pack('n', 7);
        $context = $this->decode(0x1100, WriteTimeoutException::class, WriteTimeoutContext::class, $body, ProtocolVersion::V5);

        $this->assertSame(WriteType::CAS, $context->writeType);
        $this->assertSame(7, $context->contentions);
    }
    /**
     * @return array<string, array{ProtocolVersion}>
     */
    public static function versions(): array {
        return ['v3' => [ProtocolVersion::V3], 'v4' => [ProtocolVersion::V4], 'v5' => [ProtocolVersion::V5]];
    }

    /**
     * @template T of ErrorContext
     *
     * @param class-string<ServerException> $exceptionClass
     * @param class-string<T> $contextClass
     *
     * @return T
     */
    private function decode(int $code, string $exceptionClass, string $contextClass, string $details, ProtocolVersion $version): ErrorContext {
        $body = pack('N', $code) . self::string('message') . $details;
        $header = new Header($version, 0, 1, Opcode::RESPONSE_ERROR, strlen($body));
        $response = (new ReflectionMethod(ResponseReader::class, 'createResponse'))->invoke(new ResponseReader(), $header, $body);
        $this->assertInstanceOf(Error::class, $response);

        $exception = $response->getException();
        $this->assertInstanceOf($exceptionClass, $exception);
        $this->assertSame($code, $exception->getCode());
        $this->assertStringContainsString('message', $exception->getMessage());

        $context = $exception->getErrorContext();
        $this->assertInstanceOf($contextClass, $context);
        $this->assertNotFalse(json_encode($context->toArray()));

        return $context;
    }

    private static function string(string $value): string {
        return pack('n', strlen($value)) . $value;
    }
}
