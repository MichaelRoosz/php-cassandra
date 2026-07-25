<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Request\Options;
use Cassandra\Request\Query;

/**
 * A request carries no stream id until it is given one.
 *
 * The protocol reserves only negative stream ids (for server-initiated
 * streams), so 0 is an ordinary id that the connection hands out like any
 * other. "Not assigned yet" therefore has to be its own state rather than a
 * reserved id, or a request that was never given one would silently share a
 * stream with whichever request legitimately holds 0.
 */
final class RequestStreamIdTest extends AbstractUnitTestCase {
    public function testAssignedStreamIdIsEncodedIntoTheFrameHeader(): void {
        $request = new Query('SELECT * FROM ks.tbl', [], Consistency::ONE);
        $request->setStream(0x1234);

        $header = substr((string) $request, 0, 9);

        /** @var array{stream: int} $parsed */
        $parsed = unpack('Cversion/Cflags/nstream/Copcode/Nlength', $header);

        $this->assertSame(0x1234, $parsed['stream']);
        $this->assertSame(0x1234, $request->getStream());
    }

    public function testEncodingARequestWithoutAStreamIdFails(): void {
        $request = new Query('SELECT * FROM ks.tbl', [], Consistency::ONE);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_STREAM_NOT_ASSIGNED->value);

        (string) $request;
    }

    public function testStreamIdOfAnUnassignedRequestIsNull(): void {
        $this->assertNull((new Options())->getStream());
    }

    public function testStreamIdZeroIsAnOrdinaryAssignedId(): void {
        $request = new Options();
        $request->setStream(0);

        // Distinguishable from "not assigned", which is what lets the pool
        // hand out 0 like any other id.
        $this->assertNotNull($request->getStream());
        $this->assertSame(0, $request->getStream());

        // And it really does reach the wire as stream 0.
        /** @var array{stream: int} $parsed */
        $parsed = unpack('Cversion/Cflags/nstream/Copcode/Nlength', substr((string) $request, 0, 9));
        $this->assertSame(0, $parsed['stream']);
    }
}
