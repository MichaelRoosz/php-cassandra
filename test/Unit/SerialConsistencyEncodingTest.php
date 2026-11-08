<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;
use Cassandra\SerialConsistency;

final class SerialConsistencyEncodingTest extends AbstractUnitTestCase {
    public function testASerialConsistencyIsEncodedAsItsProtocolValue(): void {
        // Regression: the enum itself was handed to pack(), which sent 1 (ONE) with a PHP warning.
        $query = new Query('SELECT 1', [], Consistency::ONE, new QueryOptions(serialConsistency: SerialConsistency::LOCAL_SERIAL));
        $query->setVersion(ProtocolVersion::V4);
        $query->setStream(1);

        $this->assertSame(pack('n', SerialConsistency::LOCAL_SERIAL->value), substr($query->getBody(), -2));
    }
}
