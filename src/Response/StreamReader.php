<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Type;

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
     * @deprecated readValue() should be used instead
     *
     * @param int|array<mixed> $dataType
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readBytesAndConvertToType($dataType): mixed {
        return $this->readValue($dataType);
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
     * Read list.
     *
     * @param int|array<int|array<mixed>> $definition [$valueType]
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readList(int|array $definition): array {
        if (is_array($definition)) {
            $count = count($definition);

            if ($count < 1) {
                throw new Exception('invalid type definition');
            } elseif ($count === 1) {
                /** @psalm-suppress PossiblyUndefinedArrayOffset */
                [$valueType] = array_values($definition);
            } else {
                $valueType = $definition;
            }
        } else {
            $valueType = $definition;
        }

        $list = [];
        $count = $this->readInt();
        for ($i = 0; $i < $count; ++$i) {
            /** @psalm-suppress MixedAssignment */
            $list[] = $this->readValue($valueType);
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
     * Read map.
     *
     * @param array<int|array<mixed>> $definition [$keyType, $valueType]
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readMap(array $definition): array {
        if (count($definition) < 2) {
            throw new Exception('invalid type definition');
        }

        [$keyType, $valueType] = array_values($definition);
        $map = [];
        $count = $this->readInt();

        /** @psalm-suppress MixedAssignment */
        for ($i = 0; $i < $count; ++$i) {
            $key = $this->readValue($keyType);
            if (!is_string($key) && !is_int($key)) {
                throw new Exception('invalid key type');
            }
            $map[$key] = $this->readValue($valueType);
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
     *
     * @param array<int|array<mixed>> $definition ['key1'=>$valueType1, 'key2'=>$valueType2, ...]
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readTuple(array $definition): array {
        $tuple = [];
        foreach ($definition as $key => $type) {
            /** @psalm-suppress MixedAssignment */
            $tuple[$key] = $this->readValue($type);
        }

        return $tuple;
    }

    /**
     * @return int|array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readType() {
        $type = $this->readShort();

        switch ($type) {
            case Type::CUSTOM:
                return [
                    'type' => $type,
                    'definition'=> [$this->readString()],
                ];
            case Type::COLLECTION_LIST:
            case Type::COLLECTION_SET:
                return [
                    'type' => $type,
                    'definition' => [$this->readType()],
                ];
            case Type::COLLECTION_MAP:
                return [
                    'type' => $type,
                    'definition'=> [$this->readType(), $this->readType()],
                ];
            case Type::UDT:
                $data = [
                    'type' => $type,
                    'definition' => [],
                    'keyspace' => $this->readString(),
                    'name'=> $this->readString(),
                ];
                $length = $this->readShort();
                for ($i = 0; $i < $length; ++$i) {
                    $key = $this->readString();
                    $data['definition'][$key] = $this->readType();
                }

                return $data;
            case Type::TUPLE:
                $data = [
                    'type' => $type,
                    'definition' => [],
                ];
                $length = $this->readShort();
                for ($i = 0; $i < $length; ++$i) {
                    $data['definition'][] = $this->readType();
                }

                return $data;

            default:
                return $type;
        }
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
     * @param int|array<mixed> $dataType
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readValue($dataType): mixed {
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
        $typeObject = Type::getTypeObjectForBinary($dataType, $binary);

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
