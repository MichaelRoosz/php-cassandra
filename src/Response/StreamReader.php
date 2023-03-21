<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Type;

class StreamReader
{
    protected string $data;
    protected int $dataLength;

    protected int $offset = 0;
    protected int $extraDataOffset = 0;

    public function __construct(string $data)
    {
        $this->data = $data;
        $this->dataLength = strlen($data);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function read(int $length): string
    {
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

    /**
     * Sets the extra data offset used to hide extra data at the beginning of the response.
     */
    public function extraDataOffset(int $extraDataOffset): void
    {
        $this->extraDataOffset = $extraDataOffset;
    }

    public function offset(int $offset): void
    {
        $this->offset = $this->extraDataOffset + $offset;
    }

    public function reset(): void
    {
        $this->offset = $this->extraDataOffset;
    }

    public function pos(): int
    {
        return $this->offset - $this->extraDataOffset;
    }

    public function getData(): string
    {
        return $this->data;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readChar(): int
    {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('C', $this->read(1));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }
        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readShort(): int
    {
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
    public function readInt(): int
    {
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
     * @throws \Cassandra\Response\Exception
     */
    public function readString(): string
    {
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
     * @throws \Cassandra\Response\Exception
     */
    public function readLongString(): string
    {
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
     * @throws \Cassandra\Response\Exception
     */
    public function readBytes(): ?string
    {
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
     * @throws \Cassandra\Response\Exception
     */
    public function readUuid(): string
    {
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
     * Read list.
     *
     * @param int|array<int|array<mixed>> $definition [$valueType]
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readList(int|array $definition): array
    {
        if (is_array($definition)) {
            if (count($definition) < 1) {
                throw new Exception('invalid type definition');
            } elseif (count($definition) === 1) {
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
     * Read map.
     *
     * @param array<int|array<mixed>> $definition [$keyType, $valueType]
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readMap(array $definition): array
    {
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
     *
     * @param array<int|array<mixed>> $definition ['key1'=>$valueType1, 'key2'=>$valueType2, ...]
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readTuple(array $definition): array
    {
        $tuple = [];
        foreach ($definition as $key => $type) {
            /** @psalm-suppress MixedAssignment */
            $tuple[$key] = $this->readValue($type);
        }
        return $tuple;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readFloat(): float
    {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('g', strrev($this->read(4)));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }
        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readDouble(): float
    {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('e', strrev($this->read(8)));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }
        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readBoolean(): bool
    {
        return (bool)$this->readChar();
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readInet(): string
    {
        $addressLength = $this->readChar();

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
    public function readVarint(): int
    {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N2', $this->read(8));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack data');
        }

        if (count($unpacked) < 2) {
            throw new Exception('invalid data');
        }

        [$higher, $lower] = array_values($unpacked);
        return $higher << 32 | $lower;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function readDecimal(): string
    {
        $scale = $this->readInt();
        $value = (string) $this->readVarint();
        $len = strlen($value);
        return substr($value, 0, $len - $scale) . '.' . substr($value, $len - $scale);
    }

    /**
     * @return array<string,?string>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function readBytesMap(): array
    {
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
     * @return array<string>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function readStringList(): array
    {
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
    public function readStringMap(): array
    {
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
    public function readStringMultimap(): array
    {
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
     * @return array<string,int>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function readReasonMap(): array
    {
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
     * alias of readValue()
     * @deprecated
     *
     * @param int|array<mixed> $dataType
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readBytesAndConvertToType($dataType): mixed
    {
        return $this->readValue($dataType);
    }

    /**
     * @param int|array<mixed> $dataType
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readValue($dataType): mixed
    {
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

        $data = $this->read($length);

        if (is_int($dataType)) {
            $type = $dataType;
            $definition = null;
        } else {
            $didShift = false;

            if (isset($dataType['type'])) {
                /** @var int|array<mixed> $type */
                $type = $dataType['type'];
            } else {
                /** @var int|array<mixed> $type */
                $type = array_shift($dataType);
                $didShift = true;
            }

            if (isset($dataType['definition'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $dataType['definition'];
            } elseif (isset($dataType['value'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $dataType['value'];
            } elseif (isset($dataType['typeMap'])) {
                /** @var int|array<int|array<mixed>> $definition */
                $definition = $dataType['typeMap'];
            } else {
                if (!$didShift) {
                    array_shift($dataType);
                }
                /** @var int|array<int|array<mixed>> $definition */
                $definition = array_shift($dataType);
            }
        }

        if ($definition === null && in_array($type, Type\Base::$typesWithDefinition)) {
            throw new Exception('type is missing its definition');
        }

        if (!is_int($type)) {
            throw new Exception('invalid data type');
        }

        if (!isset(Type\Base::$typeClassMap[$type])) {
            throw new Exception('invalid data type');
        }

        $class = Type\Base::$typeClassMap[$type];

        return $class::parse($data, $definition);
    }

    /**
     * @return int|array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function readType()
    {
        $type = $this->readShort();
        switch ($type) {
            case Type\Base::CUSTOM:
                return [
                    'type' => $type,
                    'definition'=> [$this->readString()],
                ];
            case Type\Base::COLLECTION_LIST:
            case Type\Base::COLLECTION_SET:
                return [
                    'type' => $type,
                    'definition' => [$this->readType()],
                ];
            case Type\Base::COLLECTION_MAP:
                return [
                    'type' => $type,
                    'definition'=> [$this->readType(), $this->readType()],
                ];
            case Type\Base::UDT:
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
            case Type\Base::TUPLE:
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
}
