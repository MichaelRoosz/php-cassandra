<?php

declare(strict_types=1);

namespace Cassandra\Response\Result\Data;

final class VoidData extends ResultData {
    public function __construct() {
        parent::__construct();
    }
}
