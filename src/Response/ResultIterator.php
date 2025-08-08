<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Response\Result\RowsResult;
use Iterator;

/**
 * @implements Iterator<RowClassInterface|array<array-key, mixed>|false>
 */
final class ResultIterator implements Iterator {
    protected int $currentRow;
    protected bool $needToRewindRow;

    public function __construct(
        protected RowsResult $rowsResult,
    ) {
        $this->currentRow = 0;
        $this->needToRewindRow = false;
    }

    /**
     * @return RowClassInterface|array<array-key, mixed>|false
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
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
     */
    #[\Override]
    public function next(): void {
        ++$this->currentRow;
        $this->needToRewindRow = false;
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
