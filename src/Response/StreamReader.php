<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Consistency;
use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeFactory;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeNameParser;
use TypeError;
use ValueError;
use Cassandra\VIntCodec;

class StreamReader {
    final protected const SIGNED_INT_SHIFT_BIT_SIZE = (PHP_INT_SIZE * 8) - 32;

    protected string $data;
    protected int $dataLength;
    protected int $extraDataOffset = 0;
    protected int $offset = 0;
    protected TypeNameParser $typeNameParser;
    protected VIntCodec $vIntCodec;

    public function __construct(string $data) {
        $this->data = $data;
        $this->dataLength = strlen($data);
        $this->typeNameParser = new TypeNameParser();
        $this->vIntCodec = new VIntCodec();
    }

    /**
     * Sets the extra data offset used to hide extra data at the beginning of the response.
     */
    public function extraDataOffset(int $extraDataOffset): void {
        $this->extraDataOffset = $extraDataOffset;
    }

    public function getData(bool $includeExtraData = false): string {
        return $includeExtraData ? $this->data : substr($this->data, $this->extraDataOffset);
    }

    public function offset(int $offset): void {
        $this->offset = $this->extraDataOffset + $offset;
    }

    public function pos(): int {
        return $this->offset - $this->extraDataOffset;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function read(int $length): string {
        if ($length < 1) {
            return '';
        }

        if ($this->offset + $length > $this->dataLength) {
            throw new Exception(
                message: 'Attempted to read beyond available data',
                code: ExceptionCode::RESPONSE_SR_READ_BEYOND_AVAILABLE->value,
                context: [
                    'method' => __METHOD__,
                    'requested_length' => $length,
                    'available' => $this->dataLength - $this->offset,
                    'offset' => $this->pos(),
                    'data_length' => $this->dataLength,
                ]
            );
        }

        $output = substr($this->data, $this->offset, $length);
        $this->offset += $length;

        return $output;
    }

    /**
     * Reads a 1 byte unsigned integer
     * @throws \Cassandra\Response\Exception
     */
    final public function readByte(): int {
        return ord($this->read(1));
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readBytes(): ?string {

        $length = $this->readInt();

        if ($length < 0) {
            return null;
        }

        if ($length === 0) {
            return '';
        }

        return $this->read($length);
    }

    /**
     * @return array<string,?string>
     *
     * @throws \Cassandra\Response\Exception
     */
    final public function readBytesMap(): array {
        $map = [];
        $count = $this->readShort();
        for ($i = 0; $i < $count; $i++) {
            $key = $this->readString();
            $value = $this->readBytes();
            $map[$key] = $value;
        }

        return $map;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readConsistency(): Consistency {

        $consistencyAsInt = $this->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception(
                'Invalid consistency: ' . $consistencyAsInt,
                ExceptionCode::RESPONSE_SR_INVALID_CONSISTENCY->value,
                [
                    'consistency' => $consistencyAsInt,
                ],
                $e
            );
        }

        return $consistency;
    }

    /**
     * @return array{
     *   ip: string,
     *   port: int,
     * }
     * 
     * @throws \Cassandra\Response\Exception
     */
    final public function readInet(): array {
        $address = $this->readInetAddr();
        $port = $this->readInt();

        return [
            'ip' => $address,
            'port' => $port,
        ];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readInetAddr(): string {

        $length = $this->readByte();

        if ($length !== 4 && $length !== 16) {
            throw new Exception(
                message: 'Invalid inet length byte',
                code: ExceptionCode::RESPONSE_SR_INVALID_INET_LENGTH->value,
                context: [
                    'method' => __METHOD__,
                    'address_length' => $length,
                    'offset' => $this->pos(),
                ]
            );
        }

        $inet = inet_ntop($this->read($length));
        if ($inet === false) {
            throw new Exception(
                message: 'Cannot parse inet address',
                code: ExceptionCode::RESPONSE_SR_INET_PARSE_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'address_length' => $length,
                    'offset' => $this->pos(),
                ]
            );
        }

        return $inet;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readInt(): int {

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $this->read(4));
        if ($unpacked === false) {
            throw new Exception(
                message: 'Cannot unpack 32-bit integer',
                code: ExceptionCode::RESPONSE_SR_UNPACK_INT_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'offset' => $this->pos(),
                ]
            );
        }

        return $unpacked[1]
            << self::SIGNED_INT_SHIFT_BIT_SIZE
            >> self::SIGNED_INT_SHIFT_BIT_SIZE;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readLong(): int {

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('J', $this->read(8));
        if ($unpacked === false) {
            throw new Exception(
                message: 'Cannot unpack 64-bit integer',
                code: ExceptionCode::RESPONSE_SR_UNPACK_LONG_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'offset' => $this->pos(),
                ]
            );
        }

        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readLongString(): string {

        $length = $this->readInt();

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * @return array<string,int>
     *
     * @throws \Cassandra\Response\Exception
     */
    final public function readReasonMap(): array {
        $map = [];
        $count = $this->readInt();
        for ($i = 0; $i < $count; $i++) {
            $key = $this->readInetAddr();
            $value = $this->readShort();
            $map[$key] = $value;
        }

        return $map;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readShort(): int {

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', $this->read(2));
        if ($unpacked === false) {
            throw new Exception(
                message: 'Cannot unpack 16-bit integer',
                code: ExceptionCode::RESPONSE_SR_UNPACK_SHORT_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'offset' => $this->pos(),
                ]
            );
        }

        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readShortBytes(): string {

        $length = $this->readShort();

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * Reads a signed VInt with a maximum size of 32 bits
     * 
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    final public function readSignedVint32(): int {
        return $this->vIntCodec->readSignedVint32($this);
    }

    /**
     * Reads a signed VInt with a maximum size of 64 bits.
     * This is named "vint" in the native protocol specification.
     * 
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    final public function readSignedVint64(): int {
        return $this->vIntCodec->readSignedVint64($this);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readString(): string {

        $length = $this->readShort();

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * @return string[]
     *
     * @throws \Cassandra\Response\Exception
     */
    final public function readStringList(): array {
        $list = [];
        $count = $this->readShort();
        for ($i = 0; $i < $count; $i++) {
            $list[] = $this->readString();
        }

        return $list;
    }

    /**
     * @return array<string,string>
     *
     * @throws \Cassandra\Response\Exception
     */
    final public function readStringMap(): array {
        $map = [];
        $count = $this->readShort();
        for ($i = 0; $i < $count; $i++) {
            $key = $this->readString();
            $value = $this->readString();
            $map[$key] = $value;
        }

        return $map;
    }

    /**
     * @return array<string,string[]>
     *
     * @throws \Cassandra\Response\Exception
     */
    final public function readStringMultimap(): array {
        $map = [];
        $count = $this->readShort();
        for ($i = 0; $i < $count; $i++) {
            $key = $this->readString();
            $list = $this->readStringList();

            $map[$key] = $list;
        }

        return $map;
    }

    /**
     * Reads a type info object.
     * The native protocol specification calls this an "option".
     * 
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    final public function readTypeInfo(): TypeInfo {
        $typeShort = $this->readShort();

        try {
            $type = Type::from($typeShort);
        } catch (ValueError|TypeError $e) {
            throw new Exception(
                message: 'Invalid type discriminator',
                code: ExceptionCode::RESPONSE_SR_INVALID_TYPE_DISCRIMINATOR->value,
                context: [
                    'method' => __METHOD__,
                    'type_short' => $typeShort,
                    'offset' => $this->pos(),
                ],
                previous: $e,
            );
        }

        switch ($type) {
            case Type::CUSTOM:
                $javaClassName = $this->readString();

                return $this->typeNameParser->parse($javaClassName);

            case Type::LIST_COLLECTION:
                return new ListCollectionInfo(
                    valueType: $this->readTypeInfo(),
                    isFrozen: false,
                );

            case Type::SET_COLLECTION:
                return new SetCollectionInfo(
                    valueType: $this->readTypeInfo(),
                    isFrozen: false,
                );

            case Type::MAP_COLLECTION:
                return new MapCollectionInfo(
                    keyType: $this->readTypeInfo(),
                    valueType: $this->readTypeInfo(),
                    isFrozen: false,
                );

            case Type::UDT:

                $keyspace = $this->readString();
                $name = $this->readString();

                $types = [];
                $length = $this->readShort();
                for ($i = 0; $i < $length; ++$i) {
                    $key = $this->readString();
                    $types[$key] = $this->readTypeInfo();
                }

                return new UDTInfo(
                    valueTypes: $types,
                    isFrozen: false,
                    keyspace: $keyspace,
                    name: $name,
                );

            case Type::TUPLE:

                $types = [];
                $length = $this->readShort();
                for ($i = 0; $i < $length; ++$i) {
                    $types[] = $this->readTypeInfo();
                }

                return new TupleInfo(
                    valueTypes: $types,
                );

            case Type::VECTOR:

                // not supported as of protocol v5
                throw new Exception(
                    message: 'Native vector type not supported as of protocol v5',
                    code: ExceptionCode::RESPONSE_SR_VECTOR_TYPE_NOT_SUPPORTED->value,
                    context: [
                        'method' => __METHOD__,
                    ]
                );

            default:
                return new SimpleTypeInfo(
                    type: $type,
                );
        }
    }

    /**
     * Reads an unsigned VInt with a maximum size of 32 bits
     * 
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    final public function readUnsignedVInt32(): int {
        return $this->vIntCodec->readUnsignedVint32($this);
    }

    /**
     * Reads an unsigned VInt with a maximum size of 64 bits.
     * This is named "unsigned vint" in the native protocol specification.
     * 
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Exception
     */
    final public function readUnsignedVInt64(): int {
        return $this->vIntCodec->readUnsignedVint64($this);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    final public function readUuid(): string {

        $binary = $this->read(16);

        /**
         * @var false|array<int> $data
         */
        $data = unpack('n8', $binary);
        if ($data === false) {
            throw new Exception(
                message: 'Cannot unpack UUID',
                code: ExceptionCode::RESPONSE_SR_UNPACK_UUID_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'offset' => $this->pos(),
                    'binary_length' => strlen($binary),
                    'expected_length' => 16,
                ]
            );
        }

        return sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            $data[1],
            $data[2],
            $data[3],
            $data[4],
            $data[5],
            $data[6],
            $data[7],
            $data[8]
        );
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    final public function readValue(TypeInfo $typeInfo): mixed {

        $length = $this->readInt();

        if ($length < 0) {
            if ($length === -1) {
                return null;
            }

            if ($length === -2) { // "not set"
                // note: we could also return a NotSet object here,
                // but we return null to avoid serializing issues
                return null;
            }

            throw new Exception(
                message: 'Invalid value length',
                code: ExceptionCode::RESPONSE_SR_UNPACK_VALUE_LENGTH_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'length' => $length,
                ]
            );
        }

        $typeObject = TypeFactory::getTypeObjectFromStream($typeInfo, $length, $this);

        return $typeObject->getValue();
    }

    public function reset(): void {
        $this->offset = $this->extraDataOffset;
    }
}
