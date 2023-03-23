<?php

declare(strict_types=1);

namespace Cassandra\Type;

trait Common
{
    /**
     * @throws \Cassandra\Type\Exception
     */
    protected function binaryOfValue(): string
    {
        if ($this->_value === null) {
            throw new Exception('Value is null');
        }

        return static::binary($this->_value);
    }

    protected function resetValue(): void {
        $this->_value = null;
    }
}
