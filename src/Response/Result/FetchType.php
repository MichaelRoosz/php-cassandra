<?php

declare(strict_types=1);

namespace Cassandra\Response\Result;

enum FetchType {
    case ASSOC;
    case BOTH;
    case NUM;
}
