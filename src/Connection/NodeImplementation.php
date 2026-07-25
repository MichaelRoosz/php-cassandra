<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

abstract class NodeImplementation implements Node {
    /**
     * Timeouts are handled as fractional seconds; INF means "no timeout" and is
     * passed on to select() as a null seconds value, which blocks indefinitely.
     */
    protected const NO_TIMEOUT = INF;

    private const BUFFER_SIZE = 2048;

    private string $readBuffer = '';
    private int $readBufferLength = 0;
    private int $readBufferOffset = 0;

    #[\Override]
    abstract public function close(): void;

    #[\Override]
    abstract public function getConfig(): NodeConfig;

    /**
     * Returns exactly $length bytes of data, or an empty string if not enough data is available.
     *
     * If $waitForData is true this blocks until the data source yields something, but a single
     * call still performs a single read: a short read (the peer sent only part of what we asked
     * for) returns an empty string. Whatever arrived stays buffered, so callers that need all
     * $length bytes must call again until they get a non-empty result.
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
     * Split fractional seconds into the (seconds, microseconds) pair that
     * socket_select() and stream_select() expect.
     *
     * {@see self::NO_TIMEOUT} maps to a null seconds value, which makes both
     * functions wait indefinitely; the microseconds value is ignored in that
     * case.
     *
     * @return array{?int, int}
     */
    protected function splitTimeout(float $timeout): array {
        if (is_infinite($timeout)) {
            return [null, 0];
        }

        $seconds = (int) $timeout;
        $microseconds = (int) round(($timeout - (float) $seconds) * 1_000_000.0);

        if ($microseconds >= 1_000_000) {
            $seconds++;
            $microseconds -= 1_000_000;
        }

        return [$seconds, $microseconds];
    }

    /**
     * Reads data from the data source and updates the buffer.
     * Returns the number of bytes available in the buffer.
     * 
     * @throws \Cassandra\Exception\NodeException
     */
    private function readFromNode(int $missingLength, bool $waitForData): int {

        $readMaxLength = max($missingLength, self::BUFFER_SIZE);
        $data = $this->readAvailableDataFromSource($missingLength, $readMaxLength, $waitForData);

        if ($data !== '') {

            $dataLength = strlen($data);

            if ($this->readBufferOffset === 0 && $this->readBufferLength > 0) {
                // Nothing consumed yet (e.g. a large body arriving in pieces):
                // append instead of rebuilding the buffer, which would copy the
                // whole accumulated prefix on every partial read.
                $this->readBuffer .= $data;
                $this->readBufferLength += $dataLength;
            } elseif ($this->readBufferOffset < $this->readBufferLength) {
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
    private function updateReadBuffer(int $expectedLength, bool $waitForData): int {

        $availableLength = $this->readBufferLength - $this->readBufferOffset;
        $missingLength = $expectedLength - $availableLength;

        if ($missingLength > 0) {
            $availableLength = $this->readFromNode($missingLength, $waitForData);
        }

        return $availableLength;
    }
}
