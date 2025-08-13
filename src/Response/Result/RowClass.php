<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

/**
 * @api
 * @psalm-consistent-constructor
 */
class RowClass implements RowClassInterface {
    /**
     * @var array<mixed>
     */
    private array $rowData;

    /**
     * @param array<mixed> $rowData
     * @param array<mixed> $additionalArguments
     */
    public function __construct(array $rowData, array $additionalArguments = []) {
        $this->rowData = $rowData;
    }

    public function __get(string $name): mixed {
        return $this->rowData[$name] ?? null;
    }
}
