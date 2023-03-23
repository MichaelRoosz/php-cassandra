<?php

declare(strict_types=1);

namespace Cassandra\Type;

trait CommonResetValue
{
    protected function resetValue(): void
    {
        $this->_value = null;
    }
}
