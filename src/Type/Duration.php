<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Duration extends Base
{
    /**
     * @var ?array{ months: int, days: int, nanoseconds: int } $_value
     */
    protected ?array $_value = null;

    /**
     * @param ?array{ months: int, days: int, nanoseconds: int } $value
     */
    public function __construct(?array $value = null)
    {
        $this->_value = $value;
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    protected static function create(mixed $value, null|int|array $definition): self
    {
        if ($value !== null && !is_array($value)) {
            throw new Exception('Invalid value type');
        }

        if (!isset($value['months']) || !is_int($value['months'])) {
            throw new Exception('Invalid value type - key "months" not set or invalid data type');
        }

        if (!isset($value['days']) || !is_int($value['days'])) {
            throw new Exception('Invalid value type - key "days" not set or invalid data type');
        }

        if (!isset($value['nanoseconds']) || !is_int($value['nanoseconds'])) {
            throw new Exception('Invalid value type - key "nanoseconds" not set or invalid data type');
        }

        if (
            !($value['months'] <= 0 && $value['days'] <= 0 && $value['nanoseconds'] <= 0)
            &&
            !($value['months'] >= 0 && $value['days'] >= 0 && $value['nanoseconds'] >= 0)
        ) {
            throw new Exception('Invalid value type - all values must be either positive or negative');
        }

        return new self([
            'months' => $value['months'],
            'days' => $value['days'],
            'nanoseconds' => $value['nanoseconds'],
        ]);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function binaryOfValue(): string
    {
        if ($this->_value === null) {
            throw new Exception('value is null');
        }

        return static::binary($this->_value);
    }

    /**
     * @return ?array{ months: int, days: int, nanoseconds: int }
     * @throws \Cassandra\Type\Exception
     */
    public function parseValue(): ?array
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    public function __toString(): string
    {
        return (string) json_encode($this->_value);
    }

    /**
     * @param array{ months: int, days: int, nanoseconds: int } $value
     */
    public static function binary(array $value): string
    {
        $monthsEncoded = ($value['months'] >> 31) ^ ($value['months'] << 1);
        $daysEncoded = ($value['days'] >> 31) ^ ($value['days'] << 1);

        $nsEncoded = ($value['nanoseconds'] >> 63) ^ ($value['nanoseconds'] << 1);
        $nsEncodedUpper = ($nsEncoded & 0xffffffff00000000) >>32;
        $nsEncodedLower = $nsEncoded & 0x00000000ffffffff;

        return pack('N', $monthsEncoded)
                . pack('N', $daysEncoded)
                . pack('NN', $nsEncodedUpper, $nsEncodedLower);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     * @return array{ months: int, days: int, nanoseconds: int }
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): array
    {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N4', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        $nsEncoded = $unpacked[3] << 32 | $unpacked[4];

        return [
            'months' => ($unpacked[1] >> 1) ^ -($unpacked[1] & 1),
            'days' => ($unpacked[2] >> 1) ^ -($unpacked[2] & 1),
            'nanoseconds' => ($nsEncoded >> 1) ^ -($nsEncoded & 1)
        ];
    }
}
