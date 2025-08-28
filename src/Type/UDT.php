<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\TypeInfo;

final class UDT extends TypeBase {
    protected UDTInfo $typeInfo;
    /**
     * @var array<mixed> $value
     */
    protected readonly array $value;

    /**
     * @param array<mixed> $value
     * @param array<string,\Cassandra\Type|(array{ type: \Cassandra\Type }&array<mixed>)>|null $valueDefinition 
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    final public function __construct(
        array $value,
        array|null $valueDefinition = null,
        bool $isFrozen = false,
        ?UDTInfo $typeInfo = null,
    ) {
        $this->value = $value;

        if ($valueDefinition !== null) {
            $this->typeInfo = UDTInfo::fromTypeDefinition([
                'type' => Type::UDT,
                'valueTypes' => $valueDefinition,
                'isFrozen' => $isFrozen,
            ]);
        } elseif ($typeInfo !== null) {
            $this->typeInfo = $typeInfo;
        } else {
            throw new Exception('Either valueDefinition or typeInfo must be provided', ExceptionCode::TYPE_UDT_VALUEDEF_OR_TYPEINFO_REQUIRED->value);
        }
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_UDT_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof UDTInfo) {
            throw new Exception('Invalid type info, UDTInfo expected', ExceptionCode::TYPE_UDT_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static((new StreamReader($binary))->readUDT($typeInfo), typeInfo: $typeInfo);
    }

    /**
     * @param mixed $value
     * 
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\TypeInfo\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_array($value)) {
            throw new Exception('Invalid UDT value; expected associative array', ExceptionCode::TYPE_UDT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        if ($typeInfo === null) {
            throw new Exception('typeInfo is required', ExceptionCode::TYPE_UDT_TYPEINFO_REQUIRED->value);
        }

        if (!$typeInfo instanceof UDTInfo) {
            throw new Exception('Invalid type info, UDTInfo expected', ExceptionCode::TYPE_UDT_INVALID_TYPEINFO->value, [
                'given_type' => get_class($typeInfo),
            ]);
        }

        return new static($value, typeInfo: $typeInfo);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = '';
        $value = $this->value;

        foreach ($this->typeInfo->valueTypes as $key => $type) {
            if ($value[$key] === null) {
                $binary .= "\xff\xff\xff\xff";
            } else {
                $valueBinary = $value[$key] instanceof TypeBase
                    ? $value[$key]->getBinary()
                    : TypeFactory::getBinaryByTypeInfo($type, $value[$key]);

                $binary .= pack('N', strlen($valueBinary)) . $valueBinary;
            }
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::UDT;
    }

    /**
     * @return array<mixed> $value
     */
    #[\Override]
    public function getValue(): array {
        return $this->value;
    }
}
