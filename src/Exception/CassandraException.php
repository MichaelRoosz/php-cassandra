<?php

declare(strict_types= 1);

namespace Cassandra\Exception;

use Exception as PhpException;
use Throwable;

abstract class CassandraException extends PhpException {
    /**
     * @var array<mixed> $context
     */
    protected array $context;

    /**
     * @param array<mixed> $context
     */
    public function __construct(string $message, int $code, array $context = [], ?Throwable $previous = null) {

        if ($context && getenv('APP_CASSANDRA_DEBUG') === '1') {
            $contextAsJson = json_encode($context);
            if ($contextAsJson !== false) {
                $message = $message . ' - context: ' . $contextAsJson;
            }
        }

        parent::__construct($message, $code, $previous);

        $this->context = $context;
    }

    /**
     * @return array<mixed> $context
     */
    public function context(): array {
        return $this->context;
    }

    /**
     * @return array<mixed> $context
     */
    public function getContext(): array {
        return $this->context;
    }
}
