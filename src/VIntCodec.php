<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Response\StreamReader;

/**
 * Note that a VInt is different from a Varint.
 * See native_protocol_v5.spec for more details.
 */
final class VIntCodec {
    private const INT_BIT_SIZE_MINUS_1 = (PHP_INT_SIZE * 8) - 1;
    private const INT_MAX_SIGNED_32BIT = 2_147_483_647;
    private const INT_MAX_UNSIGNED_32BIT = 4_294_967_295;
    private const INT_MIN_SIGNED_32BIT = -2_147_483_648;
    private const INT_MIN_UNSIGNED_32BIT = 0;

    /**
     * @throws \Cassandra\Exception
     */
    final public function decodeSignedVint32(string $binary): int {

        $number = $this->zigZagDecode($this->decodeUnsignedVint64($binary));
        if ($number > self::INT_MAX_SIGNED_32BIT || $number < self::INT_MIN_SIGNED_32BIT) {
            throw new Exception('Integer value is outside of supported range', ExceptionCode::VINTCODEC_SIGNED_VINT32_OUT_OF_RANGE->value, [
                'value' => $number,
                'min' => self::INT_MIN_SIGNED_32BIT,
                'max' => self::INT_MAX_SIGNED_32BIT,
            ]);
        }

        return $number;
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function decodeSignedVint64(string $binary): int {
        return $this->zigZagDecode($this->decodeUnsignedVint64($binary));
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function decodeUnsignedVint32(string $binary): int {

        $number = $this->decodeUnsignedVint64($binary);
        if ($number > self::INT_MAX_UNSIGNED_32BIT || $number < self::INT_MIN_UNSIGNED_32BIT) {
            throw new Exception('Integer value is outside of supported range', ExceptionCode::VINTCODEC_UNSIGNED_VINT32_OUT_OF_RANGE->value, [
                'value' => $number,
                'min' => self::INT_MIN_UNSIGNED_32BIT,
                'max' => self::INT_MAX_UNSIGNED_32BIT,
            ]);
        }

        return $number;
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function decodeUnsignedVint64(string $binary): int {

        if ($binary === '') {
            throw new Exception('Binary vint data is empty', ExceptionCode::VINTCODEC_VINT64_UNPACK_FAILED->value);
        }

        /**
         * @var false|array<int> $data
         */
        $data = unpack('C*', $binary);
        if ($data === false) {
            throw new Exception('Cannot unpack vint binary data', ExceptionCode::VINTCODEC_VINT64_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
            ]);
        }

        $firstByte = $data[1];
        if ($firstByte <= 0x7F) {
            return $firstByte;
        }

        $extraBytesCount = 0;
        while (($firstByte & 0x80) !== 0) {
            $extraBytesCount++;
            $firstByte <<= 1;
        }

        $decodedValue = ($firstByte & 0x7F) >> $extraBytesCount;

        $totalBytesCount = $extraBytesCount + 2;
        for ($i = 2; $i < $totalBytesCount; $i++) {
            $decodedValue <<= 8;
            $decodedValue |= $data[$i];
        }

        return $decodedValue;
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function encodeSignedVint32(int $number): string {

        if ($number > self::INT_MAX_SIGNED_32BIT || $number < self::INT_MIN_SIGNED_32BIT) {
            throw new Exception('Integer value is outside of supported range', ExceptionCode::VINTCODEC_SIGNED_VINT32_OUT_OF_RANGE->value, [
                'value' => $number,
                'min' => self::INT_MIN_SIGNED_32BIT,
                'max' => self::INT_MAX_SIGNED_32BIT,
            ]);
        }

        return $this->encodeUnsignedVint64($this->zigZagEncode($number));
    }

    final public function encodeSignedVint64(int $number): string {

        return $this->encodeUnsignedVint64($this->zigZagEncode($number));
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function encodeUnsignedVint32(int $number): string {

        if ($number > self::INT_MAX_UNSIGNED_32BIT || $number < self::INT_MIN_UNSIGNED_32BIT) {

            throw new Exception('Integer value is outside of supported range', ExceptionCode::VINTCODEC_UNSIGNED_VINT32_OUT_OF_RANGE->value, [
                'value' => $number,
                'min' => self::INT_MIN_UNSIGNED_32BIT,
                'max' => self::INT_MAX_UNSIGNED_32BIT,
            ]);
        }

        return $this->encodeUnsignedVint64($number);
    }

    final public function encodeUnsignedVint64(int $number): string {

        if ($number <= 0x7F && $number >= 0) {
            return pack('C', $number);
        }

        $extraBytes = [];
        $extraBytesCount = 0;
        $mask = 0x80;

        while (true) {
            $cur = $number & 0xFF;
            $next = ($number >> 8) & PHP_INT_MAX;

            if ($next === 0 && ($cur & $mask) === 0) {
                $number = $cur;

                break;
            }

            if ($extraBytesCount === 8) {
                break;
            }

            $extraBytes[] = $cur;
            $extraBytesCount++;

            $mask |= $mask >> 1;
            $number = $next;
        }

        if ($extraBytesCount < 8) {
            $mask <<= 1;
        }

        $firstByte = $mask | $number;

        return pack('C*', $firstByte, ...array_reverse($extraBytes));
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function readSignedVint32(StreamReader $stream): int {

        $number = $this->zigZagDecode($this->readUnsignedVint64($stream));
        if ($number > self::INT_MAX_SIGNED_32BIT || $number < self::INT_MIN_SIGNED_32BIT) {
            throw new Exception('Integer value is outside of supported range', ExceptionCode::VINTCODEC_SIGNED_VINT32_OUT_OF_RANGE->value, [
                'value' => $number,
                'min' => self::INT_MIN_SIGNED_32BIT,
                'max' => self::INT_MAX_SIGNED_32BIT,
            ]);
        }

        return $number;
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function readSignedVint64(StreamReader $stream): int {
        return $this->zigZagDecode($this->readUnsignedVint64($stream));
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function readUnsignedVint32(StreamReader $stream): int {

        $number = $this->readUnsignedVint64($stream);
        if ($number > self::INT_MAX_UNSIGNED_32BIT || $number < self::INT_MIN_UNSIGNED_32BIT) {
            throw new Exception('Integer value is outside of supported range', ExceptionCode::VINTCODEC_UNSIGNED_VINT32_OUT_OF_RANGE->value, [
                'value' => $number,
                'min' => self::INT_MIN_UNSIGNED_32BIT,
                'max' => self::INT_MAX_UNSIGNED_32BIT,
            ]);
        }

        return $number;
    }

    /**
     * @throws \Cassandra\Exception
     */
    final public function readUnsignedVint64(StreamReader $stream): int {

        $firstByte = $stream->readByte();
        if ($firstByte <= 0x7F) {
            return $firstByte;
        }

        $extraBytesCount = 0;
        while (($firstByte & 0x80) !== 0) {
            $extraBytesCount++;
            $firstByte <<= 1;
        }

        $extraBytes = $stream->read($extraBytesCount);

        $decodedValue = ($firstByte & 0x7F) >> $extraBytesCount;

        for ($i = 0; $i < $extraBytesCount; $i++) {
            $decodedValue <<= 8;
            $decodedValue |= ord($extraBytes[$i]);
        }

        return $decodedValue;
    }

    final public function zigZagDecode(int $number): int {
        return (($number >> 1) & PHP_INT_MAX) ^ -($number & 1);
    }

    final public function zigZagEncode(int $number): int {
        return ($number << 1) ^ ($number >> self::INT_BIT_SIZE_MINUS_1);
    }
}
