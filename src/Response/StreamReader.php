<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Type;
use Cassandra\ValueFactory;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeNameParser;
use Cassandra\Value\EncodeOption\UuidEncodeOption;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\Value\ValueWithMultipleEncodings;
use TypeError;
use ValueError;
use Cassandra\VIntCodec;

final class StreamReader {
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
     * @throws \Cassandra\Exception\ResponseException
     */
    public function read(int $length): string {
        if ($length < 1) {
            return '';
        }

        if ($this->offset + $length > $this->dataLength) {
            throw new ResponseException(
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readByte(): int {
        return ord($this->read(1));
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readConsistency(): Consistency {

        $consistencyAsInt = $this->readShort();

        try {
            $consistency = Consistency::from($consistencyAsInt);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException(
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
     * Reads an IEEE-754 big-endian double (8 bytes).
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readDouble(): float {

        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('E', $this->read(8));
        if ($unpacked === false) {
            throw new ResponseException(
                message: 'Cannot unpack double',
                code: ExceptionCode::RESPONSE_SR_UNPACK_DOUBLE_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'offset' => $this->pos(),
                ]
            );
        }

        return $unpacked[1];
    }

    /**
     * Reads an IEEE-754 big-endian float (4 bytes).
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readFloat(): float {

        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('G', $this->read(4));
        if ($unpacked === false) {
            throw new ResponseException(
                message: 'Cannot unpack float',
                code: ExceptionCode::RESPONSE_SR_UNPACK_FLOAT_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'offset' => $this->pos(),
                ]
            );
        }

        return $unpacked[1];
    }

    /**
     * @return array{
     *   ip: string,
     *   port: int,
     * }
     * 
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readInetAddr(): string {

        $length = $this->readByte();

        if ($length !== 4 && $length !== 16) {
            throw new ResponseException(
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
            throw new ResponseException(
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readInt(): int {

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $this->read(4));
        if ($unpacked === false) {
            throw new ResponseException(
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readLong(): int {

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('J', $this->read(8));
        if ($unpacked === false) {
            throw new ResponseException(
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readLongString(): string {

        $length = $this->readInt();

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * @return array<string,int>
     *
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readShort(): int {

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', $this->read(2));
        if ($unpacked === false) {
            throw new ResponseException(
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
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readShortBytes(): string {

        $length = $this->readShort();

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * Reads a signed VInt with a maximum size of 32 bits
     * 
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\VIntCodecException
     */
    final public function readSignedVint32(): int {
        return $this->vIntCodec->readSignedVint32($this);
    }

    /**
     * Reads a signed VInt with a maximum size of 64 bits.
     * This is named "vint" in the native protocol specification.
     * 
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readSignedVint64(): int {
        return $this->vIntCodec->readSignedVint64($this);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readString(): string {

        $length = $this->readShort();

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * @return string[]
     *
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\TypeNameParserException
     */
    final public function readTypeInfo(): TypeInfo {
        $typeShort = $this->readShort();

        try {
            $type = Type::from($typeShort);
        } catch (ValueError|TypeError $e) {
            throw new ResponseException(
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

            case Type::LIST:
                return new ListCollectionInfo(
                    valueType: $this->readTypeInfo(),
                    isFrozen: false,
                );

            case Type::SET:
                return new SetCollectionInfo(
                    valueType: $this->readTypeInfo(),
                    isFrozen: false,
                );

            case Type::MAP:
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
                throw new ResponseException(
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
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\VIntCodecException
     */
    final public function readUnsignedVInt32(): int {
        return $this->vIntCodec->readUnsignedVint32($this);
    }

    /**
     * Reads an unsigned VInt with a maximum size of 64 bits.
     * This is named "unsigned vint" in the native protocol specification.
     * 
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readUnsignedVInt64(): int {
        return $this->vIntCodec->readUnsignedVint64($this);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    final public function readUuid(): string {

        $hex = bin2hex($this->read(16));

        return substr($hex, 0, 8) . '-'
            . substr($hex, 8, 4) . '-'
            . substr($hex, 12, 4) . '-'
            . substr($hex, 16, 4) . '-'
            . substr($hex, 20, 12);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    final public function readValue(TypeInfo $typeInfo, ValueEncodeConfig $valueEncodeConfig): mixed {

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

            throw new ResponseException(
                message: 'Invalid value length',
                code: ExceptionCode::RESPONSE_SR_UNPACK_VALUE_LENGTH_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'length' => $length,
                ]
            );
        }

        if ($length === 0) {
            return $this->emptyValue($typeInfo);
        }

        $startOffset = $this->offset;

        /** @psalm-suppress MixedAssignment */
        $value = $this->decodeValue($typeInfo, $length, $valueEncodeConfig);

        $this->resyncAfterValue($typeInfo, $startOffset, $length);

        /** @psalm-suppress MixedReturnStatement */
        return $value;
    }

    public function reset(): void {
        $this->offset = $this->extraDataOffset;
    }

    /**
     * Decodes a single value of $length bytes, positioned at the start of the
     * value body.
     *
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    private function decodeValue(TypeInfo $typeInfo, int $length, ValueEncodeConfig $valueEncodeConfig): mixed {

        // Fast path: return exactly what the matching Cassandra\Value\*
        // getValue()/asConfigured() would, but without allocating a Value object
        // — the dominant per-cell cost. Most scalar branches decode directly
        // because their value is independent of $valueEncodeConfig; uuid/timeuuid
        // are the exception and honour $valueEncodeConfig->uuidEncodeOption
        // inline. list/set recurse through readValue (which forwards the config),
        // so config-dependent elements are still handled correctly. Maps, UDTs,
        // tuples, vectors and the config-dependent temporal/varint scalars fall
        // through to the object path below.
        switch ($typeInfo->type) {
            case Type::INT:
                return $this->readInt();

            case Type::VARCHAR:
            case Type::TEXT:
            case Type::ASCII:
            case Type::BLOB:
                return $this->read($length);

            case Type::UUID:
            case Type::TIMEUUID:
                // A uuid/timeuuid is always exactly 16 bytes; validate before
                // decoding so a corrupt cell length is rejected here rather than
                // silently yielding a wrong-length binary value on the AS_BINARY
                // path (the object path in Value\Uuid::fromBinary enforces the
                // same). AS_BINARY returns the raw 16 bytes, skipping the
                // hex/format step (see Value\Uuid::asConfigured); AS_STRING (the
                // default) returns the canonical string form.
                if ($length !== 16) {
                    throw new ResponseException(
                        message: 'Invalid uuid value length; expected 16 bytes',
                        code: ExceptionCode::RESPONSE_SR_UNPACK_UUID_FAIL->value,
                        context: [
                            'method' => __METHOD__,
                            'type' => $typeInfo->type->name,
                            'length' => $length,
                            'offset' => $this->pos(),
                        ]
                    );
                }

                return $valueEncodeConfig->uuidEncodeOption === UuidEncodeOption::AS_BINARY
                    ? $this->read(16)
                    : $this->readUuid();

            case Type::DOUBLE:
                return $this->readDouble();

            case Type::FLOAT:
                return $this->readFloat();

            case Type::BOOLEAN:
                return $this->read(1) !== "\0";

            case Type::BIGINT:
            case Type::COUNTER:
                // Bigint/Counter need special handling on 32-bit PHP (see
                // Value\Bigint); only take the fast path where readLong() is safe.
                if (PHP_INT_SIZE >= 8) {
                    return $this->readLong();
                }

                break;

            case Type::LIST:
                // List/Set decode to a plain array of their elements (see
                // Value\ListCollection/SetCollection::getValue), so build it
                // directly. Elements recurse through readValue, so they take
                // these fast paths too.
                if ($typeInfo instanceof ListCollectionInfo) {
                    return $this->readCollectionValues($typeInfo->valueType, $valueEncodeConfig);
                }

                break;

            case Type::SET:
                if ($typeInfo instanceof SetCollectionInfo) {
                    return $this->readCollectionValues($typeInfo->valueType, $valueEncodeConfig);
                }

                break;

            default:
                break;
        }

        $valueObject = ValueFactory::getValueObjectFromStream($typeInfo, $length, $this, $valueEncodeConfig);

        if ($valueObject instanceof ValueWithMultipleEncodings) {
            return $valueObject->asConfigured($valueEncodeConfig);
        } else {
            return $valueObject->getValue();
        }
    }

    /**
     * The value a zero-length ("empty") cell decodes to.
     *
     * Cassandra distinguishes null (length -1) from empty (length 0), and empty
     * is a legal value for every type. Only the types whose serialization is a
     * raw byte string or a counted collection can represent it; for everything
     * else — in particular the fixed-length scalars, whose decoders would
     * otherwise read past the end of the cell and desync the rest of the row —
     * an empty cell is reported as null.
     */
    private function emptyValue(TypeInfo $typeInfo): mixed {
        return match ($typeInfo->type) {
            Type::ASCII, Type::BLOB, Type::CUSTOM, Type::TEXT, Type::VARCHAR => '',
            Type::LIST, Type::MAP, Type::SET => [],
            default => null,
        };
    }

    /**
     * Reads a count-prefixed sequence of values of a single element type — the
     * body of a list or set collection.
     *
     * @return array<mixed>
     *
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    private function readCollectionValues(TypeInfo $elementType, ValueEncodeConfig $valueEncodeConfig): array {
        $count = $this->readInt();

        $values = [];
        for ($i = 0; $i < $count; ++$i) {
            /** @psalm-suppress MixedAssignment */
            $values[] = $this->readValue($elementType, $valueEncodeConfig);
        }

        return $values;
    }

    /**
     * Makes the cell length authoritative: a decoder that consumed fewer bytes
     * than the cell declares leaves the reader positioned at the next cell
     * instead of drifting, and one that consumed more is a protocol error
     * rather than silent corruption of every following value.
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    private function resyncAfterValue(TypeInfo $typeInfo, int $startOffset, int $length): void {

        $consumed = $this->offset - $startOffset;

        if ($consumed === $length) {
            return;
        }

        if ($consumed > $length) {
            throw new ResponseException(
                message: 'Value decoder read beyond the declared value length',
                code: ExceptionCode::RESPONSE_SR_VALUE_LENGTH_MISMATCH->value,
                context: [
                    'method' => __METHOD__,
                    'type' => $typeInfo->type->name,
                    'declared_length' => $length,
                    'consumed_length' => $consumed,
                    'offset' => $this->pos(),
                ]
            );
        }

        // Skip the trailing bytes the decoder did not consume. read() applies
        // the same bounds checks (and, for the progressive reader, the same
        // on-demand fetching) as any other read.
        $this->read($length - $consumed);
    }
}
