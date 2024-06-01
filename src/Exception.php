<?php

declare(strict_types=1);

namespace Cassandra;

use Exception as PhpException;
use Throwable;

class Exception extends PhpException {
    /**
     * @var array<string, string|int|array<int|string, int|string>> $context
     */
    protected array $context;

    /**
     * @param array<string, string|int|array<int|string, int|string>> $context
     */
    public function __construct(string $message = '', int $code = 0, array $context = [], ?Throwable $previous = null) {
        parent::__construct($message, $code, $previous);

        $this->context = $context;
    }

    /**
     * @return array<string, string|int|array<int|string, int|string>> $context
     */
    public function getContext(): array {
        return $this->context;
    }
}
