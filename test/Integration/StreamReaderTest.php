<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\ExceptionCode;
use Cassandra\Response\Exception as ResponseException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\Value\ValueEncodeConfig;
use PHPUnit\Framework\TestCase;

final class StreamReaderTest extends TestCase {
    public function testBasicReadAndOffset(): void {
        $reader = new StreamReader('abcdef');

        $this->assertSame(0, $reader->pos());
        $this->assertSame('ab', $reader->read(2));
        $this->assertSame(2, $reader->pos());

        $reader->offset(1);
        $this->assertSame(1, $reader->pos());
        $this->assertSame('bc', $reader->read(2));

        $reader->reset();
        $this->assertSame(0, $reader->pos());
        $this->assertSame('ab', $reader->read(2));
    }

    public function testReadBeyondAvailableThrows(): void {
        $reader = new StreamReader('abc');
        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_READ_BEYOND_AVAILABLE->value);
        $reader->read(10);
    }

    public function testReadByteShortIntLong(): void {
        // byte
        $reader = new StreamReader(pack('C', 0xAB));
        $this->assertSame(0xAB, $reader->readByte());

        // short (0x1234)
        $reader = new StreamReader(pack('n', 0x1234));
        $this->assertSame(0x1234, $reader->readShort());

        // int (0x7FFFFFFF then 0xFFFFFFFF -> -1)
        $reader = new StreamReader(pack('N', 0x7FFFFFFF) . pack('N', 0xFFFFFFFF));
        $this->assertSame(2147483647, $reader->readInt());
        $this->assertSame(-1, $reader->readInt());

        // long: StreamReader uses unpack('J'), so test simple positive values within range
        $valPos = 1;
        $valPos2 = 2;
        $bin = pack('J', $valPos) . pack('J', $valPos2);
        $reader = new StreamReader($bin);
        $this->assertSame($valPos, $reader->readLong());
        $this->assertSame($valPos2, $reader->readLong());
    }

    public function testReadConsistency(): void {
        // short representing Consistency::ONE (0x0001)
        $reader = new StreamReader(pack('n', Consistency::ONE->value));
        $this->assertSame(Consistency::ONE, $reader->readConsistency());

        // invalid consistency short triggers error
        $reader = new StreamReader(pack('n', 0x00FF));
        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_INVALID_CONSISTENCY->value);
        $reader->readConsistency();
    }

    public function testReadInetAddrErrors(): void {
        // invalid length
        $reader = new StreamReader(pack('C', 5) . 'abcde');
        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_INVALID_INET_LENGTH->value);
        $reader->readInetAddr();
    }

    public function testReadInetAndInetAddr(): void {
        // IPv4 address
        $addr = inet_pton('10.0.0.7');
        $bin = pack('C', 4) . $addr;
        $reader = new StreamReader($bin);
        $this->assertSame('10.0.0.7', $reader->readInetAddr());

        // IPv6 address
        $addr6 = inet_pton('2001:db8::1');
        $bin = pack('C', 16) . $addr6;
        $reader = new StreamReader($bin);
        $this->assertSame('2001:db8::1', $reader->readInetAddr());

        // readInet combines address length + address + int port
        $addr = inet_pton('127.0.0.1');
        $bin = pack('C', 4) . $addr . pack('N', 9042);
        $reader = new StreamReader($bin);
        $this->assertSame(['ip' => '127.0.0.1', 'port' => 9042], $reader->readInet());
    }

    public function testReadStringCollectionsAndMaps(): void {
        // readStringList: count=3, values a,b,""
        $list = ['a', 'b', ''];
        $bin = pack('n', count($list));
        foreach ($list as $v) {
            $bin .= pack('n', strlen($v)) . $v;
        }
        $reader = new StreamReader($bin);
        $this->assertSame($list, $reader->readStringList());

        // readStringMap: 2 entries
        $entries = ['k1' => 'v1', 'k2' => ''];
        $bin = pack('n', 2);
        foreach ($entries as $k => $v) {
            $bin .= pack('n', strlen($k)) . $k;
            $bin .= pack('n', strlen($v)) . $v;
        }
        $reader = new StreamReader($bin);
        $this->assertSame($entries, $reader->readStringMap());

        // readStringMultimap: 1 key -> 2 values
        $bin = pack('n', 1);
        $bin .= pack('n', 3) . 'key';
        $bin .= pack('n', 2); // list count
        $bin .= pack('n', 1) . 'a';
        $bin .= pack('n', 1) . 'b';
        $reader = new StreamReader($bin);
        $this->assertSame(['key' => ['a', 'b']], $reader->readStringMultimap());

        // readBytesMap: 3 entries with null, empty and non-empty
        $bin = pack('n', 3);
        $bin .= pack('n', 4) . 'key1' . pack('N', -1 & 0xFFFFFFFF); // null
        $bin .= pack('n', 4) . 'key2' . pack('N', 0); // empty string
        $bin .= pack('n', 4) . 'key3' . pack('N', strlen('value')) . 'value'; // non-empty
        $reader = new StreamReader($bin);
        $this->assertSame(
            ['key1' => null, 'key2' => '', 'key3' => 'value'],
            $reader->readBytesMap()
        );

        // readReasonMap: count=1, ip=127.0.0.1, value=123
        $ip = inet_pton('127.0.0.1');
        $bin = pack('N', 1); // count
        $bin .= pack('C', 4) . $ip; // inet addr
        $bin .= pack('n', 123); // short
        $reader = new StreamReader($bin);
        $this->assertSame(['127.0.0.1' => 123], $reader->readReasonMap());
    }

    public function testReadStringVariants(): void {
        // readString: length as short
        $s1 = 'hello';
        $bin = pack('n', strlen($s1)) . $s1 . pack('n', 0);
        $reader = new StreamReader($bin);
        $this->assertSame('hello', $reader->readString());
        $this->assertSame('', $reader->readString());

        // readLongString: length as int
        $s2 = 'world!';
        $bin = pack('N', strlen($s2)) . $s2 . pack('N', 0);
        $reader = new StreamReader($bin);
        $this->assertSame('world!', $reader->readLongString());
        $this->assertSame('', $reader->readLongString());

        // readShortBytes
        $b1 = "\x01\x02\x03";
        $bin = pack('n', strlen($b1)) . $b1 . pack('n', 0);
        $reader = new StreamReader($bin);
        $this->assertSame($b1, $reader->readShortBytes());
        $this->assertSame('', $reader->readShortBytes());

        // readBytes: int length with null(-1), empty(0), non-empty
        $data = 'xyz';
        $bin = pack('N', -1 & 0xFFFFFFFF) . pack('N', 0) . pack('N', strlen($data)) . $data;
        $reader = new StreamReader($bin);
        $this->assertNull($reader->readBytes());
        $this->assertSame('', $reader->readBytes());
        $this->assertSame($data, $reader->readBytes());
    }

    public function testReadTypeInfoSimpleAndCollections(): void {
        // simple type: INT
        $bin = pack('n', Type::INT->value);
        $reader = new StreamReader($bin);
        $typeInfo = $reader->readTypeInfo();
        $this->assertSame(Type::INT, $typeInfo->type);

        // list<varchar>
        $bin = pack('n', Type::LIST->value) . pack('n', Type::VARCHAR->value);
        $reader = new StreamReader($bin);
        $typeInfo = $reader->readTypeInfo();
        $this->assertSame(Type::LIST, $typeInfo->type);

        // map<inet,int>
        $bin = pack('n', Type::MAP->value) . pack('n', Type::INET->value) . pack('n', Type::INT->value);
        $reader = new StreamReader($bin);
        $typeInfo = $reader->readTypeInfo();
        $this->assertSame(Type::MAP, $typeInfo->type);
    }

    public function testReadUuid(): void {
        $uuid = '00112233-4455-6677-8899-aabbccddeeff';
        $hex = str_replace('-', '', $uuid);
        $bin = pack('H*', $hex);
        $reader = new StreamReader($bin);
        $this->assertSame($uuid, $reader->readUuid());
    }

    public function testReadValueHappyPaths(): void {
        // Value reading relies on length(int) + binary value according to TypeInfo
        // Example: int value 123
        $val = 123;
        $valueBinary = pack('N', $val);
        $bin = pack('N', strlen($valueBinary)) . $valueBinary; // length + content
        $reader = new StreamReader($bin);
        $cfg = new ValueEncodeConfig();
        $typeInfo = \Cassandra\ValueFactory::getTypeInfoFromType(Type::INT);
        $this->assertSame($val, $reader->readValue($typeInfo, $cfg));

        // null (-1)
        $reader = new StreamReader(pack('N', -1 & 0xFFFFFFFF));
        $this->assertNull($reader->readValue($typeInfo, $cfg));

        // empty (0) for varchar
        $typeInfo = \Cassandra\ValueFactory::getTypeInfoFromType(Type::VARCHAR);
        $reader = new StreamReader(pack('N', 0));
        $this->assertSame('', $reader->readValue($typeInfo, $cfg));
    }

    public function testReadValueInvalidNegativeLength(): void {
        $typeInfo = \Cassandra\ValueFactory::getTypeInfoFromType(Type::INT);
        // -3 is invalid
        $reader = new StreamReader(pack('N', (-3) & 0xFFFFFFFF));
        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_UNPACK_VALUE_LENGTH_FAIL->value);
        $reader->readValue($typeInfo, new ValueEncodeConfig());
    }

    public function testReadVIntVariants(): void {
        $codec = new \Cassandra\VIntCodec();

        $cases = [0, 1, -1, 127, 128, 255, 256, -256, PHP_INT_MAX >> 1];
        foreach ($cases as $n) {
            $reader = new StreamReader($codec->encodeSignedVint64($n));
            $this->assertSame($n, $reader->readSignedVint64());
        }

        $uCases = [0, 1, 127, 128, 255, 256, 65535, 4294967295];
        foreach ($uCases as $n) {
            $reader = new StreamReader($codec->encodeUnsignedVint64($n));
            $this->assertSame($n, $reader->readUnsignedVInt64());
        }

        // 32-bit variants within range
        $reader = new StreamReader($codec->encodeSignedVint32(2147483647));
        $this->assertSame(2147483647, $reader->readSignedVint32());
        $reader = new StreamReader($codec->encodeSignedVint32(-2147483648));
        $this->assertSame(-2147483648, $reader->readSignedVint32());

        $reader = new StreamReader($codec->encodeUnsignedVint32(4294967295));
        $this->assertSame(4294967295, $reader->readUnsignedVInt32());
    }
}
