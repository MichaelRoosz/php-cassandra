<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayObject;

/**
 * @extends ArrayObject<array-key, mixed>
 * @psalm-consistent-constructor
 */
abstract class RowClass extends ArrayObject {
}
