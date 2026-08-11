<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Value\EncodeOption\UuidEncodeOption;
use Cassandra\Value\Timeuuid;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\Value\Vector;

/**
 * A timeuuid is a version 1 UUID and nothing else, and every way of reading one
 * has to agree about that.
 *
 * {@see \Cassandra\Response\StreamReader::decodeValue()} decodes uuid and
 * timeuuid cells without building a {@see Timeuuid}, which is the path every
 * cell of a result row comes through — a bare column, and the elements of a
 * list, set, map, tuple or UDT, all of which recurse through readValue(). A
 * vector element goes through the value object instead. The version check used
 * to live only in the value object, so the same bytes decoded in one place and
 * raised in the other.
 */
final class TimeuuidVersionTest extends AbstractUnitTestCase {
    /** Version nibble 6, i.e. not a timeuuid. */
    private const NOT_VERSION_ONE = '00112233445566778899aabbccddeeff';

    /** The same value with the version nibble set to 1. */
    private const VERSION_ONE = '00112233445511778899aabbccddeeff';

    private const VERSION_ONE_CANONICAL = '00112233-4455-1177-8899-aabbccddeeff';

    /**
     * @return array<string, array{TypeInfo, string, mixed}>
     */
    public static function readPathProvider(): array {
        $timeuuid = new SimpleTypeInfo(Type::TIMEUUID);

        return [
            'bare column' => [
                $timeuuid,
                self::cell(self::VERSION_ONE),
                self::VERSION_ONE_CANONICAL,
            ],
            'list element' => [
                new ListCollectionInfo($timeuuid, false),
                self::cell(pack('N', 1) . self::cell(self::VERSION_ONE)),
                [self::VERSION_ONE_CANONICAL],
            ],
            'set element' => [
                new SetCollectionInfo($timeuuid, false),
                self::cell(pack('N', 1) . self::cell(self::VERSION_ONE)),
                [self::VERSION_ONE_CANONICAL],
            ],
            'map value' => [
                new MapCollectionInfo(new SimpleTypeInfo(Type::INT), $timeuuid, false),
                self::cell(pack('N', 1) . self::cell(pack('N', 7)) . self::cell(self::VERSION_ONE)),
                [7 => self::VERSION_ONE_CANONICAL],
            ],
            'tuple field' => [
                new TupleInfo([$timeuuid]),
                self::cell(self::cell(self::VERSION_ONE)),
                [self::VERSION_ONE_CANONICAL],
            ],
            'udt field' => [
                new UDTInfo(['f' => $timeuuid], false, 'ks', 'u'),
                self::cell(self::cell(self::VERSION_ONE)),
                ['f' => self::VERSION_ONE_CANONICAL],
            ],
        ];
    }

    public function testANonVersionOneTimeuuidIsRefusedAsABinaryEncodedValue(): void {
        // The AS_BINARY option returns the raw bytes rather than the canonical
        // string, so it skips the formatting step — but not the check.
        $stream = new StreamReader(self::cell(self::NOT_VERSION_ONE));

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_TIMEUUID_INVALID_VERSION->value);

        $stream->readValue(
            new SimpleTypeInfo(Type::TIMEUUID),
            new ValueEncodeConfig(uuidEncodeOption: UuidEncodeOption::AS_BINARY)
        );
    }

    /**
     * @dataProvider readPathProvider
     */
    public function testANonVersionOneTimeuuidIsRefusedOnEveryReadPath(TypeInfo $typeInfo, string $body, mixed $expected): void {

        $stream = new StreamReader(str_replace(
            pack('H*', self::VERSION_ONE),
            pack('H*', self::NOT_VERSION_ONE),
            $body
        ));

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_TIMEUUID_INVALID_VERSION->value);

        $stream->readValue($typeInfo, ValueEncodeConfig::default());
    }

    public function testANonVersionOneTimeuuidIsRefusedOnTheVectorPath(): void {
        // The one read path that does build a Timeuuid, and so the one that
        // already refused this before the check reached decodeValue().
        $typeInfo = new VectorInfo(new SimpleTypeInfo(Type::TIMEUUID), 1);

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_TIMEUUID_INVALID_VERSION->value);

        Vector::fromStream(new StreamReader(pack('H*', self::NOT_VERSION_ONE)), null, $typeInfo);
    }

    public function testAPlainUuidColumnStillTakesAnyVersion(): void {
        // Only timeuuid carries the constraint; uuid is any 16 bytes, which is
        // what Value\Uuid accepts too.
        $stream = new StreamReader(self::cell(self::NOT_VERSION_ONE));

        $this->assertSame(
            '00112233-4455-6677-8899-aabbccddeeff',
            $stream->readValue(new SimpleTypeInfo(Type::UUID), ValueEncodeConfig::default())
        );
    }

    /**
     * @dataProvider readPathProvider
     */
    public function testAVersionOneTimeuuidIsAcceptedOnEveryReadPath(TypeInfo $typeInfo, string $body, mixed $expected): void {

        $stream = new StreamReader($body);

        $this->assertSame($expected, $stream->readValue($typeInfo, ValueEncodeConfig::default()));
    }

    public function testTheValueObjectAndTheReaderRaiseTheSameFailure(): void {
        $stream = new StreamReader(self::cell(self::NOT_VERSION_ONE));

        $fromReader = null;

        try {
            $stream->readValue(new SimpleTypeInfo(Type::TIMEUUID), ValueEncodeConfig::default());
        } catch (ValueException $e) {
            $fromReader = $e;
        }

        $fromValueObject = null;

        try {
            Timeuuid::fromBinary(pack('H*', self::NOT_VERSION_ONE));
        } catch (ValueException $e) {
            $fromValueObject = $e;
        }

        $this->assertNotNull($fromReader);
        $this->assertNotNull($fromValueObject);
        $this->assertSame($fromValueObject->getMessage(), $fromReader->getMessage());
        $this->assertSame($fromValueObject->getCode(), $fromReader->getCode());
        $this->assertSame(
            $fromValueObject->getContext()['value'] ?? null,
            $fromReader->getContext()['value'] ?? null,
            'both report the offending value in its canonical form'
        );
    }

    /**
     * A `[bytes]` cell: the four-byte length prefix followed by the body. Hex
     * input is decoded first, so the uuid constants can be written readably.
     * pack('H*') rather than hex2bin() because it is total — the constants here
     * are hex by construction, and nothing should have to be said about the
     * case that cannot arise.
     */
    private static function cell(string $body): string {
        if (preg_match('/^[0-9a-f]+$/', $body) === 1 && (strlen($body) % 2) === 0) {
            $body = pack('H*', $body);
        }

        return pack('N', strlen($body)) . $body;
    }
}
