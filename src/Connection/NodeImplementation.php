<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

abstract class NodeImplementation implements Node {
    protected const BUFFER_SIZE = 2048;

    protected string $readBuffer = '';
    protected int $readBufferLength = 0;
    protected int $readBufferOffset = 0;

    #[\Override]
    abstract public function close(): void;

    #[\Override]
    abstract public function getConfig(): NodeConfig;

    /**
     * Returns exactly $length bytes of data, or an empty string if not enough data is available.
     * If $waitForData is true, it will block until $length bytes are available.
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    public function read(int $length, bool $waitForData): string {

        $availableLength = $this->updateReadBuffer($length, $waitForData);
        if ($availableLength < $length) {
            return '';
        }

        $buffer = substr($this->readBuffer, $this->readBufferOffset, $length);
        $this->readBufferOffset += $length;

        return $buffer;
    }

    /**
     * Returns up to $maxLength bytes of data, or an empty string if no data is available.
     * If $waitForData is true, it will block until at least one byte is available.
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    public function readAvailableData(int $expectedLength, int $maxLength, bool $waitForData): string {

        $availableLength = $this->updateReadBuffer($expectedLength, $waitForData);
        if ($availableLength < 1) {
            return '';
        }

        $returnLength = min($maxLength, $availableLength);
        $data = substr($this->readBuffer, $this->readBufferOffset, $returnLength);
        $this->readBufferOffset += $returnLength;

        return $data;
    }

    /**
     * Returns some bytes of data, or an empty string if no data is available.
     * $upperBoundaryLength marks an upper boundary for the amount of data that will be returned, but more or less data may be returned.
     * If $waitForData is true, it will block until at least one byte is available.
     *
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    abstract public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    abstract public function write(string $data): void;

    /**
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    abstract public function writeRequest(Request $request): void;

    /**
     * Reads data from the data source and updates the buffer.
     * Returns the number of bytes available in the buffer.
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    protected function readFromNode(int $missingLength, bool $waitForData): int {

        $readMaxLength = max($missingLength, self::BUFFER_SIZE);
        $data = $this->readAvailableDataFromSource($missingLength, $readMaxLength, $waitForData);

        if ($data !== '') {

            $dataLength = strlen($data);

            if ($this->readBufferOffset < $this->readBufferLength) {
                $remainingLength = $this->readBufferLength - $this->readBufferOffset;
                $this->readBuffer = substr($this->readBuffer, $this->readBufferOffset, $remainingLength) . $data;
                $this->readBufferOffset = 0;
                $this->readBufferLength = $remainingLength + $dataLength;
            } else {
                $this->readBuffer = $data;
                $this->readBufferOffset = 0;
                $this->readBufferLength = $dataLength;
            }
        }

        return $this->readBufferLength - $this->readBufferOffset;
    }

    /**
     * Updates the buffer with data from the data source if needed.
     * Returns the number of bytes available in the buffer.
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    protected function updateReadBuffer(int $expectedLength, bool $waitForData): int {

        $availableLength = $this->readBufferLength - $this->readBufferOffset;
        $missingLength = $expectedLength - $availableLength;

        if ($missingLength > 0) {
            $availableLength = $this->readFromNode($missingLength, $waitForData);
        }

        return $availableLength;
    }
}
