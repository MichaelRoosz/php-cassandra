<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Type;
use Cassandra\TypeFactory;
use Cassandra\TypeInfo\CollectionListInfo;
use Cassandra\TypeInfo\CollectionMapInfo;
use Cassandra\TypeInfo\CollectionSetInfo;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\TypeInfo\UDTInfo;
use TypeError;
use ValueError;

class StreamReader {
    protected string $data;
    protected int $dataLength;
    protected int $extraDataOffset = 0;

    protected int $offset = 0;

    public function __construct(string $data) {
        $this->data = $data;
        $this->dataLength = strlen($data);
    }

    /**
     * Sets the extra data offset used to hide extra data at the beginning of the response.
     */
    public function extraDataOffset(int $extraDataOffset): void {
        $this->extraDataOffset = $extraDataOffset;
    }

    public function getData(): string {
        return $this->data;
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
    public function readBoolean(): bool {
        return (bool) ord($this->read(1));
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readBytes(): ?string {
        $binaryLength = $this->read(4);
        if ($binaryLength === "\xff\xff\xff\xff") {
            return null;
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binaryLength);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }
        $length = $unpacked[1];

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * @return array<string,?string>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function readBytesMap(): array {
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
    public function readChar(): int {
        return ord($this->read(1));
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readDouble(): float {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('E', $this->read(8));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }

        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readFloat(): float {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('G', $this->read(4));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }

        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readInet(): string {
        $addressLength = ord($this->read(1));

        if ($addressLength !== 4 && $addressLength !== 16) {
            throw new Exception('Invalid read inet length');
        }

        $inet = inet_ntop(chr($addressLength) . $this->read($addressLength));
        if ($inet === false) {
            throw new Exception('Cannot read inet');
        }

        return $inet;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readInt(): int {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $this->read(4));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }

        return $unpacked[1];
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readList(CollectionListInfo $typeInfo): array {

        $list = [];
        $count = $this->readInt();
        for ($i = 0; $i < $count; ++$i) {
            /** @psalm-suppress MixedAssignment */
            $list[] = $this->readValue($typeInfo->valueType);
        }

        return $list;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readLongString(): string {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $this->read(4));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }
        $length = $unpacked[1];

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readMap(CollectionMapInfo $typeInfo): array {

        $map = [];
        $count = $this->readInt();

        /** @psalm-suppress MixedAssignment */
        for ($i = 0; $i < $count; ++$i) {
            $key = $this->readValue($typeInfo->keyType);
            if (!is_string($key) && !is_int($key)) {
                throw new Exception('invalid key type');
            }
            $map[$key] = $this->readValue($typeInfo->valueType);
        }

        return $map;
    }

    /**
     * @return array<string,int>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function readReasonMap(): array {
        $map = [];
        $count = $this->readInt();
        for ($i = 0; $i < $count; $i++) {
            $key = $this->readInet();
            $value = $this->readShort();
            $map[$key] = $value;
        }

        return $map;
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readSet(CollectionSetInfo $typeInfo): array {

        $list = [];
        $count = $this->readInt();
        for ($i = 0; $i < $count; ++$i) {
            /** @psalm-suppress MixedAssignment */
            $list[] = $this->readValue($typeInfo->valueType);
        }

        return $list;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readShort(): int {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', $this->read(2));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }

        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readString(): string {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', $this->read(2));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }
        $length = $unpacked[1];

        return $length === 0 ? '' : $this->read($length);
    }

    /**
     * @return array<string>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function readStringList(): array {
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
    public function readStringMap(): array {
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
     * @return array<string,array<int,string>>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function readStringMultimap(): array {
        $map = [];
        $count = $this->readShort();
        for ($i = 0; $i < $count; $i++) {
            $key = $this->readString();

            $listLength = $this->readShort();
            $list = [];
            for ($j = 0; $j < $listLength; $j++) {
                $list[] = $this->readString();
            }

            $map[$key] = $list;
        }

        return $map;
    }

    /**
     * @return array<string>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readTextList(): array {
        $rawList = $this->readList(new CollectionListInfo(new SimpleTypeInfo(Type::TEXT)));

        $list = [];
        foreach ($rawList as $item) {
            if (!is_string($item)) {
                throw new Exception('Invalid text list item: ' . gettype($item));
            }

            $list[] = $item;
        }

        return $list;
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readTuple(TupleInfo $typeInfo): array {
        $tuple = [];
        foreach ($typeInfo->valueTypes as $key => $type) {
            /** @psalm-suppress MixedAssignment */
            $tuple[$key] = $this->readValue($type);
        }

        return $tuple;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readType(): TypeInfo {
        $typeShort = $this->readShort();

        try {
            $type = Type::from($typeShort);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid type short: ' . $typeShort);
        }

        switch ($typeShort) {
            case Type::CUSTOM:
                return new CustomInfo(
                    javaClassName: $this->readString(),
                );

            case Type::COLLECTION_LIST:
                return new CollectionListInfo(
                    valueType: $this->readType(),
                );

            case Type::COLLECTION_SET:
                return new CollectionSetInfo(
                    valueType: $this->readType(),
                );

            case Type::COLLECTION_MAP:
                return new CollectionMapInfo(
                    keyType: $this->readType(),
                    valueType: $this->readType(),
                );

            case Type::UDT:

                $keyspace = $this->readString();
                $name = $this->readString();

                $types = [];
                $length = $this->readShort();
                for ($i = 0; $i < $length; ++$i) {
                    $key = $this->readString();
                    $types[$key] = $this->readType();
                }

                return new UDTInfo(
                    valueTypes: $types,
                    keyspace: $keyspace,
                    name: $name,
                );

            case Type::TUPLE:

                $types = [];
                $length = $this->readShort();
                for ($i = 0; $i < $length; ++$i) {
                    $types[] = $this->readType();
                }

                return new TupleInfo(
                    valueTypes: $types,
                );

            default:
                return new SimpleTypeInfo(
                    type: $type,
                );
        }
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readUDT(UDTInfo $typeInfo): array {
        $tuple = [];
        foreach ($typeInfo->valueTypes as $key => $type) {
            /** @psalm-suppress MixedAssignment */
            $tuple[$key] = $this->readValue($type);
        }

        return $tuple;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readUuid(): string {
        /**
         * @var false|array<int> $data
         */
        $data = unpack('n8', $this->read(16));
        if ($data === false) {
            throw new Exception('Cannot unpack data');
        }

        return sprintf('%04x%04x-%04x-%04x-%04x-%04x%04x%04x', $data[1], $data[2], $data[3], $data[4], $data[5], $data[6], $data[7], $data[8]);
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readValue(TypeInfo $typeInfo): mixed {
        $binaryLength = $this->read(4);
        if ($binaryLength === "\xff\xff\xff\xff") {
            return null;
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binaryLength);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }
        $length = $unpacked[1];

        $binary = $this->read($length);
        $typeObject = TypeFactory::getTypeObjectForBinary($typeInfo, $binary);

        return $typeObject->getValue();
    }

    public function reset(): void {
        $this->offset = $this->extraDataOffset;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function read(int $length): string {
        if ($length < 1) {
            return '';
        }

        if ($this->offset + $length > $this->dataLength) {
            //$length = $this->dataLength - $this->offset;
            throw new Exception('Tried to read more data than available');
        }

        $output = substr($this->data, $this->offset, $length);
        $this->offset += $length;

        return $output;
    }
}
