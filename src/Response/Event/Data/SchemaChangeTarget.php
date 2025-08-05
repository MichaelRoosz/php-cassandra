<?php

declare(strict_types=1);

namespace Cassandra\Response\Event\Data;

enum SchemaChangeTarget: string {
    case AGGREGATE = 'AGGREGATE';
    case FUNCTION = 'FUNCTION';
    case KEYSPACE = 'KEYSPACE';
    case TABLE = 'TABLE';
    case TYPE = 'TYPE';
}
