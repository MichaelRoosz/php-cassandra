<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

/**
 * @psalm-consistent-constructor
 */
interface RowClassInterface {
    /**
     * @param array<mixed> $rowData
     * @param array<mixed> $additionalArguments
     *
     * @throws \Throwable implementations may fail while constructing a mapped row
     */
    public function __construct(array $rowData, array $additionalArguments = []);
}
