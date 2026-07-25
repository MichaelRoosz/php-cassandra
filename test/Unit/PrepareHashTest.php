<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Request\Prepare;

final class PrepareHashTest extends AbstractUnitTestCase {
    public function testAQueryAndKeyspaceAreNotRunTogether(): void {
        // Regression: the cache key concatenated them, so a cached statement could run as another.
        $this->assertNotSame(
            (new Prepare('SELECT * FROM t1'))->getHash(),
            (new Prepare('SELECT * FROM t', new PrepareOptions(keyspace: '1')))->getHash(),
        );
    }
}
