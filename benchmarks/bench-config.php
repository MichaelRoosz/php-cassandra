<?php

declare(strict_types=1);

/**
 * Common benchmark configuration
 * Shared across all drivers for consistent comparison
 */

return [
    'benchInsertAndSelectWithoutTypeInfo' => [
        'rounds' => 100,
        'iterations' => 30,
        'description' => '100 inserts + 100 selects per round (without type hints)',
    ],
    'benchInsertAndSelectWithTypeInfo' => [
        'rounds' => 100,
        'iterations' => 30,
        'description' => '100 inserts + 100 selects per round (with type hints)',
    ],
    'benchPagedQuery' => [
        'rounds' => 100,
        'iterations' => 30,
        'description' => '1 paged query per round (500 rows, page size 50)',
    ],
    'benchPreparedInsert' => [
        'rounds' => 100,
        'iterations' => 30,
        'description' => '100 inserts per round (prepared statement)',
    ],
    'benchSimpleSelect' => [
        'rounds' => 700,
        'iterations' => 30,
        'description' => '1 simple select per round',
    ],
];
