<?php

declare(strict_types=1);

namespace Cassandra\Response;

/**
 * @psalm-consistent-constructor
 */
interface RowClassInterface {
    /**
     * @param array<mixed> $rowData
     * @param array<mixed> $additionalArguments
     */
    public function __construct(array $rowData, array $additionalArguments = []);
}
