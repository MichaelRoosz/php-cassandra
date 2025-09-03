<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\TypeInfo;

final class MapCollection extends TypeReadableWithoutLength {
    protected MapCollectionInfo $typeInfo;

    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     */
    final public function __construct(
        array $value,
        MapCollectionInfo $typeInfo,
    ) {
        $this->typeInfo = $typeInfo;
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

        return self::fromStream(new StreamReader($binary), typeInfo: $typeInfo);
    }

    /**
     * @param mixed $value
     * 
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_array($value)) {
            throw new Exception('Invalid map value; expected array', ExceptionCode::TYPE_MAP_COLLECTION_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_MAP_COLLECTION_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof MapCollectionInfo) {
            throw new Exception('Invalid type info, MapCollectionInfo expected', ExceptionCode::TYPE_MAP_COLLECTION_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    final public static function fromStream(StreamReader $stream, ?int $length = null, ?TypeInfo $typeInfo = null): static {

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_MAP_COLLECTION_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof MapCollectionInfo) {
            throw new Exception('Invalid type info, MapCollectionInfo expected', ExceptionCode::TYPE_MAP_COLLECTION_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        $map = [];
        $count = $stream->readInt();

        /** @psalm-suppress MixedAssignment */
        for ($i = 0; $i < $count; ++$i) {
            $key = $stream->readValue($typeInfo->keyType);
            if (!is_string($key) && !is_int($key)) {
                throw new Exception(
                    message: 'Invalid map key type; expected string|int',
                    code: ExceptionCode::TYPE_MAP_COLLECTION_INVALID_MAP_KEY_TYPE->value,
                    context: [
                        'method' => __METHOD__,
                        'key_php_type' => gettype($key),
                        'offset' => $stream->pos(),
                    ]
                );
            }
            $map[$key] = $stream->readValue($typeInfo->valueType);
        }

        return new static($map, typeInfo: $typeInfo);
    }

    /**
     * @param array<mixed> $value
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $keyDefinition
     * @param \Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>) $valueDefinition
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public static function fromValue(
        array $value,
        Type|array $keyDefinition,
        Type|array $valueDefinition,
        bool $isFrozen = false,
    ): static {

        return new static($value, MapCollectionInfo::fromTypeDefinition([
            'type' => Type::MAP_COLLECTION,
            'keyType' => $keyDefinition,
            'valueType' => $valueDefinition,
            'isFrozen' => $isFrozen,
        ]));
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {

        $binary = pack('N', count($this->value));

        /** @var TypeBase|mixed $val */
        foreach ($this->value as $key => $val) {
            $keyPacked = TypeFactory::getBinaryByTypeInfo($this->typeInfo->keyType, $key);

            $valuePacked = $val instanceof TypeBase
                ? $val->getBinary()
                : TypeFactory::getBinaryByTypeInfo($this->typeInfo->valueType, $val);

            $binary .= pack('N', strlen($keyPacked)) . $keyPacked;
            $binary .= pack('N', strlen($valuePacked)) . $valuePacked;
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::MAP_COLLECTION;
    }

    /**
     * @return array<mixed> $value
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return true;
    }
}
