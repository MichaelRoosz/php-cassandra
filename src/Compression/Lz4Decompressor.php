<?php

/*
 * MIT License
 *
 * Copyright (c) 2019 Stephan J. Müller
 * Copyright (c) 2023 Michael J. Roosz
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
*/

declare(strict_types=1);

namespace Cassandra\Compression;

use Cassandra\ExceptionCode;

final class Lz4Decompressor {
    protected string $input;

    protected int $inputLength;

    protected int $inputOffset;
    protected string $output;
    protected int $outputLength;
    protected int $outputOffset;

    public function __construct(?string $compressedData = null, int $inputOffset = 0, int $inputLength = 0) {
        if ($compressedData !== null) {
            $this->setInput($compressedData, $inputOffset, $inputLength);
        } else {
            $this->input = '';
            $this->inputOffset = 0;

            $this->inputLength = 0;
            $this->outputLength = 0;

            $this->output = '';
            $this->outputOffset = 0;
        }
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    public function decompress(bool $validateChecksums = true): string {
        do {
            $magic = $this->readMagicBytes();

            if ($this->readVersionOneFrame($magic, $validateChecksums)) {
                continue;
            }

            if ($this->readSkipableFrame($magic)) {
                continue;
            }

            if ($this->readLegacyFrame($magic)) {
                continue;
            }

            throw new Exception(
                'invalid lz4 frame data - invalid magic number',
                ExceptionCode::COMPRESSION_INVALID_MAGIC->value,
                [
                    'stage' => 'read_magic',
                    'magicBytes' => $magic,
                    'magicBytesHex' => sprintf('0x%08X', $magic),
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                ]
            );
        } while ($this->inputOffset < $this->inputLength);

        return $this->output;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    public function decompressBlock(): string {
        $this->decompressBlockAtOffset($this->inputOffset, $this->inputLength);

        return $this->output;
    }

    public function setInput(string $compressedData, int $inputOffset = 0, int $inputLength = 0): void {
        $this->input = $compressedData;
        $this->inputOffset = $inputOffset;

        $this->inputLength = $inputLength > 0 ? $inputLength : strlen($compressedData);
        $this->outputLength = 0;

        $this->output = '';
        $this->outputOffset = 0;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function decompressBlockAtOffset(int $inputOffset, int $inputLength): void {
        while ($inputOffset < $inputLength) {
            $token = ord($this->input[$inputOffset++]);
            $nLiterals = $token >> 4;

            if ($nLiterals === 0xF) {
                do {
                    if ($inputOffset >= $inputLength) {
                        throw new Exception(
                            'invalid lz4 block data - input overflow while reading number of literals',
                            ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                            [
                                'stage' => 'read_literals_length',
                                'inputOffset' => $inputOffset,
                                'inputLength' => $inputLength,
                                'token' => $token,
                                'nLiteralsPartial' => $nLiterals,
                            ]
                        );
                    }
                    $nLiterals += $summand = ord($this->input[$inputOffset++]);
                } while ($summand === 0xFF);
            }

            if ($nLiterals > 0) {
                if ($inputOffset + $nLiterals > $inputLength) {
                    throw new Exception(
                        'invalid lz4 block data - input overflow while reading literals',
                        ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                        [
                            'stage' => 'read_literals',
                            'inputOffset' => $inputOffset,
                            'inputLength' => $inputLength,
                            'requiredBytes' => $nLiterals,
                            'token' => $token,
                            'nLiterals' => $nLiterals,
                        ]
                    );
                }
                $this->output .= substr($this->input, $inputOffset, $nLiterals);
                $this->outputLength += $nLiterals;
                $inputOffset += $nLiterals;
            }

            if ($inputOffset === $inputLength) {
                break;
            }

            if ($inputOffset + 2 > $inputLength) {
                throw new Exception(
                    'invalid lz4 block data - input overflow while reading offset',
                    ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                    [
                        'stage' => 'read_offset',
                        'inputOffset' => $inputOffset,
                        'inputLength' => $inputLength,
                        'requiredBytes' => 2,
                        'outputLength' => $this->outputLength,
                    ]
                );
            }

            $offset = ord($this->input[$inputOffset++]) | (ord($this->input[$inputOffset++]) << 8);
            if ($offset < 1) {
                throw new Exception(
                    'invalid lz4 block data - illegal offset value',
                    ExceptionCode::COMPRESSION_ILLEGAL_VALUE->value,
                    [
                        'stage' => 'validate_offset',
                        'offset' => $offset,
                        'outputLength' => $this->outputLength,
                    ]
                );
            }

            $matchStart = $this->outputLength - $offset;
            if ($matchStart < 0) {
                throw new Exception(
                    'invalid lz4 block data - output underflow for given match start',
                    ExceptionCode::COMPRESSION_OUTPUT_UNDERFLOW->value,
                    [
                        'stage' => 'compute_match_start',
                        'matchStart' => $matchStart,
                        'offset' => $offset,
                        'outputLength' => $this->outputLength,
                    ]
                );
            }

            if ($matchStart > $this->outputLength) {
                throw new Exception(
                    'invalid lz4 block data - output overflow for given match',
                    ExceptionCode::COMPRESSION_OUTPUT_OVERFLOW->value,
                    [
                        'stage' => 'compute_match_start',
                        'matchStart' => $matchStart,
                        'outputLength' => $this->outputLength,
                    ]
                );
            }

            $matchLength = $token & 0xF;
            if ($matchLength === 0xF) {
                do {
                    if ($inputOffset >= $inputLength) {
                        throw new Exception(
                            'invalid lz4 block data - input overflow while reading match length',
                            ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                            [
                                'stage' => 'read_match_length',
                                'inputOffset' => $inputOffset,
                                'inputLength' => $inputLength,
                                'token' => $token,
                                'matchLengthPartial' => $matchLength,
                            ]
                        );
                    }
                    $matchLength += $summand = ord($this->input[$inputOffset++]);
                } while ($summand === 0xFF);
            }
            $matchLength += 4;

            $j = $matchStart;
            $k = $matchLength;
            while ($k--) {
                $this->output .= $this->output[$j++];
            }

            $this->outputLength += $matchLength;
        }
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function readLegacyFrame(int $magicBytes): bool {
        if ($magicBytes !== 0x184C2102) {
            return false;
        }

        if ($this->inputOffset + 4 > $this->inputLength) {
            throw new Exception(
                'invalid lz4 frame data - input overflow while reading block size',
                ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                [
                    'stage' => 'legacy_block_size',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'requiredBytes' => 4,
                ]
            );
        }

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('V', $this->input, $this->inputOffset);
        if ($unpacked === false) {
            throw new Exception(
                'invalid lz4 frame data - cannot decode block size',
                ExceptionCode::COMPRESSION_DECODE_FAILURE->value,
                [
                    'stage' => 'legacy_block_size_unpack',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                ]
            );
        }
        $blockSize = $unpacked[1];
        $this->inputOffset += 4;

        if ($blockSize > 0) {
            if ($this->inputOffset + $blockSize > $this->inputLength) {
                throw new Exception(
                    'invalid lz4 frame data - input overflow while reading block data',
                    ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                    [
                        'stage' => 'legacy_block_data',
                        'inputOffset' => $this->inputOffset,
                        'inputLength' => $this->inputLength,
                        'blockSize' => $blockSize,
                    ]
                );
            }

            $this->decompressBlockAtOffset($this->inputOffset, $this->inputOffset + $blockSize);
            $this->inputOffset += $blockSize;
        }

        return true;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function readMagicBytes(): int {
        if ($this->inputOffset + 4 > $this->inputLength) {
            throw new Exception(
                'invalid lz4 frame data - input overflow while reading magic number',
                ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                [
                    'stage' => 'read_magic',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'requiredBytes' => 4,
                ]
            );
        }

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('V', $this->input, $this->inputOffset);
        if ($unpacked === false) {
            throw new Exception(
                'invalid lz4 frame data - cannot decode magic number',
                ExceptionCode::COMPRESSION_DECODE_FAILURE->value,
                [
                    'stage' => 'unpack_magic',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                ]
            );
        }

        $this->inputOffset += 4;

        return $unpacked[1];
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function readSkipableFrame(int $magicBytes): bool {
        if ($magicBytes < 0x184D2A50 || $magicBytes > 0x184D2A5F) {
            return false;
        }

        if ($this->inputOffset + 4 > $this->inputLength) {
            throw new Exception(
                'invalid lz4 frame data - input overflow while reading skipable frame size',
                ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                [
                    'stage' => 'skipable_frame_size',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'requiredBytes' => 4,
                    'magicBytes' => $magicBytes,
                    'magicBytesHex' => sprintf('0x%08X', $magicBytes),
                ]
            );
        }

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('V', $this->input, $this->inputOffset);
        if ($unpacked === false) {
            throw new Exception(
                'invalid lz4 frame data - cannot decode skipable frame size',
                ExceptionCode::COMPRESSION_DECODE_FAILURE->value,
                [
                    'stage' => 'skipable_frame_size_unpack',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'magicBytes' => $magicBytes,
                ]
            );
        }
        $skipableFrameSize = $unpacked[1];
        $this->inputOffset += 4;

        if ($this->inputOffset + $skipableFrameSize > $this->inputLength) {
            throw new Exception(
                'invalid lz4 frame data - input overflow while reading skipable frame data',
                ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                [
                    'stage' => 'skipable_frame_data',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'skipableFrameSize' => $skipableFrameSize,
                ]
            );
        }

        $this->inputOffset += $skipableFrameSize;

        return true;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function readVersionOneBlock(int $flgBlockChecksum, bool $validateChecksums): bool {
        if ($this->inputOffset + 4 > $this->inputLength) {
            throw new Exception(
                'invalid lz4 frame data - input overflow while reading block size',
                ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                [
                    'stage' => 'v1_block_size',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'requiredBytes' => 4,
                ]
            );
        }

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('V', $this->input, $this->inputOffset);
        if ($unpacked === false) {
            throw new Exception(
                'invalid lz4 frame data - cannot decode block size',
                ExceptionCode::COMPRESSION_DECODE_FAILURE->value,
                [
                    'stage' => 'v1_block_size_unpack',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                ]
            );
        }
        $blockSizeRaw = $unpacked[1];
        $this->inputOffset += 4;

        if ($blockSizeRaw === 0x00000000) { // EndMark
            return false;
        }

        $isUncompressed = $blockSizeRaw & 0x80;
        $blockSize = $blockSizeRaw & 0x7F;
        $blockStart = $this->inputOffset;

        if ($blockSize > 0) {
            if ($this->inputOffset + $blockSize > $this->inputLength) {
                throw new Exception(
                    'invalid lz4 frame data - input overflow while reading block data',
                    ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                    [
                        'stage' => 'v1_block_data',
                        'inputOffset' => $this->inputOffset,
                        'inputLength' => $this->inputLength,
                        'blockSize' => $blockSize,
                        'isUncompressed' => (bool) $isUncompressed,
                    ]
                );
            }

            if ($isUncompressed) {
                $this->output .= substr($this->input, $this->inputOffset, $blockSize);
                $this->outputLength += $blockSize;
            } else {
                $this->decompressBlockAtOffset($this->inputOffset, $this->inputOffset + $blockSize);
            }

            $this->inputOffset += $blockSize;
        }

        if ($flgBlockChecksum) {
            if ($this->inputOffset + 4 > $this->inputLength) {
                throw new Exception(
                    'invalid lz4 frame data - input overflow while reading block checksum',
                    ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                    [
                        'stage' => 'v1_block_checksum',
                        'inputOffset' => $this->inputOffset,
                        'inputLength' => $this->inputLength,
                        'requiredBytes' => 4,
                    ]
                );
            }

            if ($validateChecksums) {
                /** @var false|array<int> $unpacked */
                $unpacked = unpack('V', $this->input, $this->inputOffset);
                if ($unpacked === false) {
                    throw new Exception(
                        'invalid lz4 frame data - cannot decode block checksum',
                        ExceptionCode::COMPRESSION_CHECKSUM_DECODE_FAILURE->value,
                        [
                            'stage' => 'v1_block_checksum_unpack',
                            'inputOffset' => $this->inputOffset,
                            'inputLength' => $this->inputLength,
                        ]
                    );
                }
                $blockChecksum = $unpacked[1];

                $this->validateChecksum('block', $this->input, $blockStart, $blockSize, $blockChecksum);
            }

            $this->inputOffset += 4;
        }

        return true;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function readVersionOneFrame(int $magicBytes, bool $validateChecksums): bool {
        if ($magicBytes !== 0x184D2204) {
            return false;
        }

        $headerStart = $this->inputOffset;

        if ($this->inputOffset + 2 > $this->inputLength) {
            throw new Exception(
                'invalid lz4 frame data - input overflow while reading header flg and bd',
                ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                [
                    'stage' => 'v1_header_flg_bd',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'requiredBytes' => 2,
                ]
            );
        }
        $flg = ord($this->input[$this->inputOffset++]);

        //$bd = ord($this->input[$this->inputOffset++]);
        $this->inputOffset++; // skip bd

        $version = ($flg & 0xC0) >> 6;
        //$flgBlockIndependence = $flg & 0x20;
        $flgBlockChecksum = $flg & 0x10;
        $flgContentSize = $flg & 0x8;
        $flgContentChecksum = $flg & 0x4;
        $flgDictionaryId = $flg & 0x1;
        //$flgBlockMaxSize = ($bd & 0x70) >> 4;

        if ($version !== 0x01) {
            throw new Exception(
                'invalid lz4 frame data - invalid version',
                ExceptionCode::COMPRESSION_INVALID_VERSION->value,
                [
                    'stage' => 'v1_header_version',
                    'version' => $version,
                    'flg' => $flg,
                ]
            );
        }

        if ($flgContentSize) {
            if ($this->inputOffset + 8 > $this->inputLength) {
                throw new Exception(
                    'invalid lz4 frame data - input overflow while reading content size',
                    ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                    [
                        'stage' => 'v1_header_content_size',
                        'inputOffset' => $this->inputOffset,
                        'inputLength' => $this->inputLength,
                        'requiredBytes' => 8,
                    ]
                );
            }
            $this->inputOffset += 8;
        }

        if ($flgDictionaryId) {
            if ($this->inputOffset + 4 > $this->inputLength) {
                throw new Exception(
                    'invalid lz4 frame data - input overflow while reading dictionary id',
                    ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                    [
                        'stage' => 'v1_header_dictionary_id',
                        'inputOffset' => $this->inputOffset,
                        'inputLength' => $this->inputLength,
                        'requiredBytes' => 4,
                    ]
                );
            }
            $this->inputOffset += 4;
        }

        if ($this->inputOffset + 1 > $this->inputLength) {
            throw new Exception(
                'invalid lz4 frame data - input overflow while reading header checksum',
                ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                [
                    'stage' => 'v1_header_checksum',
                    'inputOffset' => $this->inputOffset,
                    'inputLength' => $this->inputLength,
                    'requiredBytes' => 1,
                ]
            );
        }
        $headerChecksum = ord($this->input[$this->inputOffset++]);

        if ($validateChecksums) {
            $this->validateHeaderChecksum($this->input, $headerStart, $this->inputOffset - 1 - $headerStart, $headerChecksum);
        }

        do {
            $moreBlocks = $this->readVersionOneBlock($flgBlockChecksum, $validateChecksums);
        } while ($moreBlocks);

        if ($flgContentChecksum) {
            if ($this->inputOffset + 4 > $this->inputLength) {
                throw new Exception(
                    'invalid lz4 frame data - input overflow while reading content checksum',
                    ExceptionCode::COMPRESSION_INPUT_OVERFLOW->value,
                    [
                        'stage' => 'v1_content_checksum',
                        'inputOffset' => $this->inputOffset,
                        'inputLength' => $this->inputLength,
                        'requiredBytes' => 4,
                    ]
                );
            }

            if ($validateChecksums) {
                /** @var false|array<int> $unpacked */
                $unpacked = unpack('V', $this->input, $this->inputOffset);
                if ($unpacked === false) {
                    throw new Exception(
                        'invalid lz4 frame data - cannot decode content checksum',
                        ExceptionCode::COMPRESSION_CHECKSUM_DECODE_FAILURE->value,
                        [
                            'stage' => 'v1_content_checksum_unpack',
                            'inputOffset' => $this->inputOffset,
                            'inputLength' => $this->inputLength,
                        ]
                    );
                }
                $contentChecksum = $unpacked[1];

                $this->validateChecksum('content', $this->output, 0, strlen($this->output), $contentChecksum);
            }

            $this->inputOffset += 4;
        }

        return true;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function validateChecksum(string $type, string $in, int $dataStart, int $dataLength, int $checksum): void {
        $data = substr($in, $dataStart, $dataLength);

        $currentChecksum = hash('xxh32', $data, true, ['seed' => 0]);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('N', $currentChecksum);
        if ($unpacked === false) {
            throw new Exception(
                'invalid lz4 frame data - cannot decode ' . $type . ' checksum',
                ExceptionCode::COMPRESSION_CHECKSUM_DECODE_FAILURE->value,
                [
                    'stage' => 'checksum_unpack',
                    'checksumType' => $type,
                    'dataStart' => $dataStart,
                    'dataLength' => $dataLength,
                ]
            );
        }
        if ($unpacked[1] !== $checksum) {
            throw new Exception(
                'invalid lz4 frame data - invalid ' . $type . ' checksum',
                ExceptionCode::COMPRESSION_CHECKSUM_MISMATCH->value,
                [
                    'stage' => 'checksum_validate',
                    'checksumType' => $type,
                    'expected' => $checksum,
                    'actual' => $unpacked[1],
                    'dataStart' => $dataStart,
                    'dataLength' => $dataLength,
                ]
            );
        }
    }

    /**
     * @throws \Cassandra\Compression\Exception
     */
    protected function validateHeaderChecksum(string $in, int $headerStart, int $headerLength, int $checksum): void {
        $headerData = substr($in, $headerStart, $headerLength);

        $currentChecksum = hash('xxh32', $headerData, true, ['seed' => 0]);

        /** @var false|array<int> $unpacked */
        $unpacked = unpack('C4', $currentChecksum);
        if ($unpacked === false) {
            throw new Exception(
                'invalid lz4 frame data - cannot decode header checksum',
                ExceptionCode::COMPRESSION_CHECKSUM_DECODE_FAILURE->value,
                [
                    'stage' => 'header_checksum_unpack',
                    'headerStart' => $headerStart,
                    'headerLength' => $headerLength,
                ]
            );
        }
        if ($unpacked[3] !== $checksum) {
            throw new Exception(
                'invalid lz4 frame data - invalid header checksum',
                ExceptionCode::COMPRESSION_CHECKSUM_MISMATCH->value,
                [
                    'stage' => 'header_checksum_validate',
                    'expected' => $checksum,
                    'actual' => $unpacked[3],
                    'headerStart' => $headerStart,
                    'headerLength' => $headerLength,
                ]
            );
        }
    }
}
