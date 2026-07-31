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

    /**
     * Bytes taken off the wire so far, see {@see Node::getReceivedByteCount()}.
     */
    private int $receivedByteCount = 0;

    #[\Override]
    abstract public function close(): void;

    #[\Override]
    abstract public function getConfig(): NodeConfig;

    #[\Override]
    public function getReceivedByteCount(): int {

        return $this->receivedByteCount;
    }

    /**
     * Returns exactly $length bytes of data, or an empty string if not enough data is available.
     *
     * A read may block until the data source yields something, but a single call still performs
     * a single read: a short read (the peer sent only part of what we asked for) returns an empty
     * string. Whatever arrived stays buffered, so callers that need all $length bytes must call
     * again until they get a non-empty result. Because $readDeadline is absolute rather than a
     * duration, calling again cannot hand the read a fresh budget.
     *
     * @param ?float $readDeadline see {@see Node::read()}
     *
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    public function read(int $length, ?float $readDeadline): string {

        $availableLength = $this->updateReadBuffer($length, $readDeadline);
        if ($availableLength < $length) {
            return '';
        }

        $buffer = substr($this->readBuffer, $this->readBufferOffset, $length);
        $this->readBufferOffset += $length;

        return $buffer;
    }

    /**
     * Returns some bytes of data, or an empty string if no data is available.
     * $upperBoundaryLength marks an upper boundary for the amount of data that will be returned, but more or less data may be returned.
     *
     * @param ?float $readDeadline see {@see Node::read()}
     *
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    abstract public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string;

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
     * A timeout as it goes into an exception's context.
     *
     * {@see self::NO_TIMEOUT} is INF, which has no JSON representation, so an
     * exception carrying it could not be serialised by whatever the application
     * logs with. It is spelled out instead.
     */
    protected function describeTimeout(float $timeout): float|string {

        if (is_finite($timeout)) {
            return $timeout;
        }

        return $timeout > 0.0 ? 'INF' : '-INF';
    }

    /**
     * Whether a read with this deadline is allowed to block at all.
     */
    protected function mayBlock(?float $readDeadline): bool {

        return $readDeadline === null || microtime(true) < $readDeadline;
    }

    /**
     * Narrow what is left of the transport's stall window to the caller's read
     * deadline, so that a single blocking read never outlives either.
     *
     * Returns null once the deadline has passed. That is deliberately not the
     * same outcome as the stall window running out: the caller simply asked for
     * no more time, which says nothing about the connection, so the read comes
     * back empty-handed instead of raising. See {@see Node::read()}.
     */
    protected function narrowToReadDeadline(float $remaining, ?float $readDeadline): ?float {

        if ($readDeadline === null) {
            return $remaining;
        }

        $untilDeadline = $readDeadline - microtime(true);
        if ($untilDeadline <= 0.0) {
            return null;
        }

        return min($remaining, $untilDeadline);
    }

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
    private function readFromNode(int $missingLength, ?float $readDeadline): int {

        $readMaxLength = max($missingLength, self::BUFFER_SIZE);
        $data = $this->readAvailableDataFromSource($missingLength, $readMaxLength, $readDeadline);

        if ($data !== '') {

            $dataLength = strlen($data);

            $newReceivedByteCount = $this->receivedByteCount + $dataLength;
            if (!is_int($newReceivedByteCount)) {
                $this->receivedByteCount = $dataLength;
            } else {
                $this->receivedByteCount = $newReceivedByteCount;
            }

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
    private function updateReadBuffer(int $expectedLength, ?float $readDeadline): int {

        $availableLength = $this->readBufferLength - $this->readBufferOffset;
        $missingLength = $expectedLength - $availableLength;

        if ($missingLength > 0) {
            $availableLength = $this->readFromNode($missingLength, $readDeadline);
        }

        return $availableLength;
    }
}
