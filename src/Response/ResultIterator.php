<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Response\Result\RowClassInterface;
use Cassandra\Response\Result\RowsResult;
use Iterator;

/**
 * @implements Iterator<\Cassandra\Response\Result\RowClassInterface|array<array-key, mixed>|false>
 */
final class ResultIterator implements Iterator {
    private int $currentRow;
    private bool $needToRewindRow;

    public function __construct(
        private RowsResult $rowsResult,
    ) {
        $this->currentRow = 0;
        $this->needToRewindRow = false;
    }

    /**
     * @return \Cassandra\Response\Result\RowClassInterface|array<array-key, mixed>|false
     *
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    public function current(): RowClassInterface|array|false {

        if ($this->needToRewindRow) {
            $this->rowsResult->rewindOneRow();
            $this->needToRewindRow = false;
        }

        if ($this->rowsResult->isFetchObjectConfigurationSet()) {
            $row = $this->rowsResult->fetchObject();
        } else {
            $row = $this->rowsResult->fetch();
        }

        $this->needToRewindRow = true;

        return $row;
    }

    /**
     * The current position in this result set
     */
    #[\Override]
    public function key(): int {
        return $this->currentRow;
    }

    /**
     * Move forward to next element
     *
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    public function next(): void {
        ++$this->currentRow;

        if ($this->needToRewindRow) {
            // current() already consumed the row at the previous position.
            $this->needToRewindRow = false;

            return;
        }

        // current() was never called for the previous position (e.g. next()
        // invoked back-to-back): skip that row so the stream cursor stays
        // aligned with the iteration position.
        $this->rowsResult->fetch();
    }

    /**
     * Reset the result set
     */
    #[\Override]
    public function rewind(): void {
        $this->currentRow = 0;
        $this->rowsResult->rewind();
        $this->needToRewindRow = false;
    }

    /**
     * Checks if current position is valid
     */
    #[\Override]
    public function valid(): bool {
        return (($this->currentRow >= 0) && ($this->currentRow < $this->rowsResult->getRowCount()));
    }
}
