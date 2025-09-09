<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration\Data;

use Cassandra\Response\Result\RowClassInterface;

final class TestRow implements RowClassInterface {
    /** @var array<mixed> */
    private array $args;

    /** @var array<mixed> */
    private array $row;

    /**
     * @param array<mixed> $rowData
     * @param array<mixed> $additionalArguments
     */
    public function __construct(array $rowData, array $additionalArguments = []) {
        $this->row = $rowData;
        $this->args = $additionalArguments;
    }

    public function __get(string $name): mixed {
        return $this->row[$name] ?? null;
    }

    public function filename(): string {

        $filename = $this->row['filename'] ?? '';

        if (!is_string($filename)) {
            throw new \InvalidArgumentException('filename must be a string');
        }

        return $filename;
    }

    public function ukey(): string {

        $ukey = $this->row['ukey'] ?? '';

        if (!is_string($ukey)) {
            throw new \InvalidArgumentException('ukey must be a string');
        }

        $suffix = $this->args['suffix'] ?? '';

        if (!is_string($suffix)) {
            throw new \InvalidArgumentException('suffix must be a string');
        }

        return $ukey . $suffix;
    }
}
