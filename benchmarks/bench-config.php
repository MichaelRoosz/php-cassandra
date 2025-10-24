<?php

declare(strict_types=1);

/**
 * Common benchmark configuration
 * Shared across all drivers for consistent comparison
 */

return [
    'benchInsertAndSelectWithoutTypeInfo' => ['revs' => 40, 'iterations' => 20],
    'benchInsertAndSelectWithTypeInfo' => ['revs' => 40, 'iterations' => 20],
    'benchPagedQuery' => ['revs' => 40, 'iterations' => 20],
    'benchPreparedInsert' => ['revs' => 40, 'iterations' => 20],
    'benchSimpleSelect' => ['revs' => 40, 'iterations' => 20],
];
