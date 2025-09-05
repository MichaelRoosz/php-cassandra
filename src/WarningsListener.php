<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Response\Response;

interface WarningsListener {
    /**
     * @param array<string> $warnings
     */
    public function onWarnings(Response $response, array $warnings): void;
}
