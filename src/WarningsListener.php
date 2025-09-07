<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Request\Request;
use Cassandra\Response\Response;

interface WarningsListener {
    /**
     * @param array<string> $warnings
     */
    public function onWarnings(array $warnings, Request $request, Response $response): void;
}
