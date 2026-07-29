<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Exception\RequestException;
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\StatementException;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;
use Cassandra\Response\Event\StatusChangeEvent;
use Cassandra\Response\Result;
use Cassandra\Statement;
use ReflectionMethod;
use Cassandra\Connection\Session;
use Cassandra\Connection\StatementRegistry;
use Cassandra\Connection\StreamIdPool;
use ReflectionProperty;

/**
 * How the client behaves when the server is slow or has nothing to say.
 *
 * Both are driven against a minimal CQL server (see
 * Support/fake-cassandra-server.php) rather than mocks, because what is being
 * tested is precisely the interaction between the transport timeouts and the
 * client-side deadlines.
 */
final class RequestTimeoutTest extends AbstractUnitTestCase {
    private const RECEIVE_TIMEOUT_SECONDS = 1;

    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    /** @var ?resource $serverStdout */
    private $serverStdout = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testAFailedHandshakeLeavesNoConnectionBehind(): void {
        // The STARTUP is never answered, so connect() gives up on it — and a
        // request timeout deliberately keeps the connection, which for a
        // handshake means a socket the node will accept nothing else on. It has
        // to be closed here, or the client would go on reporting isConnected(),
        // hand that socket to every later request, and never probe it either,
        // the heartbeat being gated on a completed handshake.
        $connection = $this->connect('refuse-startup', requestTimeoutInSeconds: 1.0, autoConnect: false);

        try {
            $connection->connect();
            $this->fail('expected the handshake to time out');
        } catch (RequestTimeoutException $e) {
        }

        $this->assertFalse($connection->isConnected(), 'a half-finished handshake must not leave a usable-looking connection');
    }

    public function testAFollowUpRequestIsNotSentOnAReplacementConnection(): void {
        // A repreparation or auto-prepare re-sends on the stream id its
        // statement already holds. That id was handed out by the connection the
        // statement was sent on, and a new connection starts its id space over,
        // so opening one here would register the statement at an id the new
        // connection is free to give to somebody else — and the statement was
        // abandoned along with its connection anyway, so it could never be
        // resolved. It has to be given up on instead.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0);

        $statement = $connection->queryAsync('SELECT * FROM SLOW');
        $connection->disconnect();

        $this->assertTrue($statement->isAbandoned());

        $method = new ReflectionMethod(Session::class, 'chainAsyncRequest');

        try {
            $method->invoke(self::sessionOf($connection), new Query('SELECT * FROM SLOW'), $statement);
            $this->fail('expected the follow-up request to be given up on');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_CHAINED_REQUEST_CONNECTION_GONE->value, $e->getCode());
            $this->assertFalse($connection->isConnected(), 'no replacement connection may be opened for it');
            $this->assertTrue($statement->isAbandoned());
        }
    }

    public function testAHeartbeatIsStillSentWithTheTransportTimeoutDisabled(): void {
        // With no stall window there is nothing to return a blocking read but
        // data, so a connection that died would never be probed unless the read
        // is bounded by when the heartbeat is next due. It is, so the probe goes
        // out and its silence is noticed on schedule.
        $connection = $this->connect(
            'deaf',
            heartbeatIntervalInSeconds: 0.5,
            heartbeatTimeoutInSeconds: 1.0,
            receiveTimeoutSeconds: 0.0,
        );

        $start = microtime(true);

        try {
            $connection->waitForNextEvent(timeoutInSeconds: 20.0);
            $this->fail('expected the unanswered heartbeat to be detected');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_HEARTBEAT_TIMEOUT->value, $e->getCode());
            $this->assertLessThan(6.0, microtime(true) - $start, 'roughly interval + timeout, not the whole wait');
        }
    }

    public function testAllRequestsThatRanOutAreFinishedTogether(): void {
        // Three statements sent together run out together. They must all be
        // given up on in one go and reported as one failure, rather than one
        // per wait with the rest left dangling.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: 1.0);

        $statements = [
            $connection->queryAsync('SELECT 1 FROM SLOW'),
            $connection->queryAsync('SELECT 2 FROM SLOW'),
            $connection->queryAsync('SELECT 3 FROM SLOW'),
        ];

        try {
            $connection->waitForStatements($statements);
            $this->fail('expected the requests to time out');
        } catch (RequestTimeoutException $e) {
            // The statements themselves, so the caller can resend exactly those
            // without matching stream ids up by hand.
            $this->assertSame($statements, $e->getTimedOutStatements());

            foreach ($statements as $statement) {
                $this->assertTrue($statement->isTimedOut());
            }

            $this->assertCount(3, $this->orphanedStreamsOf($connection));
        }
    }

    public function testAnAutoPreparedAsyncQueryUsesThePreparedResultCache(): void {
        // The synchronous path gets this for free by recursing into
        // syncRequest() for its PREPARE; the async path has to look the cache
        // up for the request it is about to send, not for the one the caller
        // handed in. Without that, every queryAsync() with untyped values pays
        // for a PREPARE it already has the answer to.
        $connection = $this->connect('idle');

        $connection->queryAsync('SELECT * FROM quick WHERE id = ?', [1])->getResult();
        $connection->queryAsync('SELECT * FROM quick WHERE id = ?', [2])->getResult();
        $connection->queryAsync('SELECT * FROM quick WHERE id = ?', [3])->getResult();

        $this->assertSame(1, $this->preparesSeenByServer(), 'the query should have been prepared once, then served from the cache');
    }

    public function testAnExpiredBudgetIsReportedWithoutWaitingOutAnotherTransportTimeout(): void {
        // The budget is long gone by the time the caller starts waiting, so
        // there is nothing left to wait for: reporting it must not first block
        // in a read until the transport's much longer stall window is over.
        $connection = $this->connect(
            'defer-slow',
            delaySeconds: 30.0,
            requestTimeoutInSeconds: 0.5,
            receiveTimeoutSeconds: 5.0,
        );

        $statement = $connection->queryAsync('SELECT * FROM SLOW');
        usleep(1_000_000);

        $start = microtime(true);

        try {
            $connection->waitForStatements([$statement]);
            $this->fail('expected the request timeout to fire');
        } catch (RequestTimeoutException $e) {
            $this->assertSame([$statement], $e->getTimedOutStatements());

            // Comfortably under the 5s stall window rather than close to zero:
            // what is being ruled out is a whole extra window, and a loaded
            // machine must not be able to look like one.
            $this->assertLessThan(
                3.0,
                microtime(true) - $start,
                'an already expired budget must not cost another transport stall window'
            );
        }
    }

    public function testAnOverdueRequestIsGivenUpOnWhileWaitingForEventsButNotReportedThere(): void {
        // Statements and events share the connection, so a statement that runs
        // out of time while the caller waits for events is finished here — but
        // an event listener never asked about it, so its wait is not
        // interrupted for it. The caller finds out from the statement.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: 1.0);

        $statement = $connection->queryAsync('SELECT * FROM SLOW');

        $this->assertNull(
            $connection->waitForNextEvent(timeoutInSeconds: 3.0),
            'the event wait must run its course rather than raise the statement'
        );

        $this->assertTrue($statement->isTimedOut(), 'the budget was still enforced');
        $this->assertSame([$statement->getStreamId()], array_keys($this->orphanedStreamsOf($connection)));
        $this->assertTrue($connection->isConnected(), 'only the statement is affected');

        // Asking about the statement is what surfaces it.
        $this->expectException(RequestTimeoutException::class);
        $statement->getResult();
    }

    public function testAnUnboundedStatementDoesNotSuppressAnotherStatementsBudget(): void {
        // No connection-wide budget, so only the per-request option bounds
        // anything. The unbounded statement has no deadline to contribute, but
        // it must not cost the one beside it its own — otherwise the wait has
        // nothing to end it and the bounded statement is never given up on.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: null);

        $bounded = $connection->queryAsync('SELECT 1 FROM SLOW', options: new QueryOptions(requestTimeoutInSeconds: 1.0));
        $unbounded = $connection->queryAsync('SELECT 2 FROM SLOW');

        $start = microtime(true);

        try {
            // No wait bound either: the statements' own budgets are all there is.
            $connection->waitForStatements([$bounded, $unbounded]);
            $this->fail('expected the bounded request to time out');
        } catch (RequestTimeoutException $e) {
            $this->assertSame([$bounded], $e->getTimedOutStatements());
        }

        $this->assertLessThan(
            5.0,
            microtime(true) - $start,
            'the 1s budget must end the wait, not run on until something else does'
        );

        $this->assertTrue($bounded->isTimedOut());
        $this->assertFalse($unbounded->isTimedOut(), 'it asked for no budget, so it still has none');
        $this->assertTrue($connection->isConnected());
    }

    public function testAPolledStatementStillRunsOutOfTime(): void {
        // Polling never waits, so nothing that runs out is a failure of the
        // poll to report — but the budget still has to be kept, or a statement
        // nobody ever blocks on would stay pending, and hold its stream id,
        // for good.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: 0.5);

        $statement = $connection->queryAsync('SELECT * FROM SLOW');

        $this->assertNull($statement->tryGetResult(), 'nothing has arrived yet');

        usleep(1_000_000);

        try {
            $statement->tryGetResult();
            $this->fail('expected the budget to be enforced for a polling caller too');
        } catch (RequestTimeoutException $e) {
            $this->assertTrue($statement->isTimedOut());
            $this->assertSame([$statement->getStreamId()], array_keys($this->orphanedStreamsOf($connection)));
            $this->assertTrue($connection->isConnected(), 'only the statement is affected');
        }
    }

    public function testARequestTimeoutFiresOnTimeUnderAMuchLongerTransportTimeout(): void {
        // The budget is handed to the read, not merely consulted before it
        // starts, so a short request timeout under a long stall window is
        // reported when it actually runs out rather than whenever the read the
        // client happens to be sitting in comes back.
        $connection = $this->connect(
            'defer-slow',
            delaySeconds: 30.0,
            requestTimeoutInSeconds: 0.5,
            receiveTimeoutSeconds: 15.0,
        );

        $start = microtime(true);

        try {
            $connection->query('SELECT * FROM SLOW');
            $this->fail('expected the request timeout to fire');
        } catch (RequestTimeoutException $e) {
            $elapsed = microtime(true) - $start;

            $this->assertGreaterThan(0.4, $elapsed, 'it must still wait its budget out');
            $this->assertLessThan(4.0, $elapsed, 'and not a moment of the 15s stall window beyond it');
        }
    }

    public function testARequestTimeoutFiresWithTheTransportTimeoutDisabled(): void {
        // `['sec' => 0, 'usec' => 0]` means "no timeout", as it does for the
        // socket option itself. The request budget is what bounds the read now,
        // so disabling the transport timeout no longer disables deadlines with
        // it.
        $connection = $this->connect(
            'defer-slow',
            delaySeconds: 30.0,
            requestTimeoutInSeconds: 1.0,
            receiveTimeoutSeconds: 0.0,
        );

        $start = microtime(true);

        try {
            $connection->query('SELECT * FROM SLOW');
            $this->fail('expected the request timeout to fire');
        } catch (RequestTimeoutException $e) {
            $elapsed = microtime(true) - $start;

            $this->assertGreaterThan(0.9, $elapsed);
            $this->assertLessThan(5.0, $elapsed, 'a disabled stall window must not mean an unbounded read');
        }
    }

    public function testAStatementNotBeingWaitedOnStillRunsOutOfTimeDuringAWait(): void {
        // Every request in flight keeps its own budget during any wait, not
        // just the ones the wait was handed. Bounding the read by the waited
        // set alone would let the short statement below sit past its deadline —
        // holding its stream id — for as long as the long one takes.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: null);

        $short = $connection->queryAsync('SELECT 1 FROM SLOW', options: new QueryOptions(requestTimeoutInSeconds: 0.5));
        $long = $connection->queryAsync('SELECT 2 FROM SLOW', options: new QueryOptions(requestTimeoutInSeconds: 30.0));

        $start = microtime(true);

        // Only the long statement is asked about, so only its budget and this
        // bound can end the wait; the 3s bound is what actually returns.
        $connection->waitForStatements([$long], timeoutInSeconds: 3.0);

        $this->assertTrue(
            $short->isTimedOut(),
            'a statement outside the waited set must still be given up on when its budget runs out'
        );

        $orphaned = $this->orphanedStreamsOf($connection);
        $this->assertSame([$short->getStreamId()], array_keys($orphaned));

        // The point of the test: it has to be noticed when its own budget ran
        // out, not merely by the time the wait happens to end. Bounding the
        // read by the waited set alone would park it at the 3s wait bound.
        $this->assertLessThan(
            1.5,
            $orphaned[$short->getStreamId()] - $start,
            'the short budget must bound the read, so the statement is parked when it expires'
        );

        $this->assertFalse($long->isTimedOut(), 'the waited statement still has 30s to go');
        $this->assertGreaterThan(2.5, microtime(true) - $start, 'the caller-supplied bound is what ends the wait');
        $this->assertTrue($connection->isConnected(), 'neither statement takes the connection down');
    }

    public function testAStatementTheConnectionNoLongerKnowsIsRejectedRatherThanSpunOn(): void {
        // A statement that is still pending but not registered here — one from
        // another Connection, or left over from before this one was replaced —
        // can never be resolved on this socket. Reporting "not ready yet" would
        // send an unbounded wait round a loop with nothing to end it: the wait
        // bounds itself by the deadlines of the statements it was given, and
        // nothing arriving here can ever retire that one.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: null);

        $statement = $connection->queryAsync('SELECT * FROM SLOW');

        // Exactly what a replaced connection leaves behind: a statement waiting
        // on a stream id this connection no longer associates with it.
        (new ReflectionProperty(StatementRegistry::class, 'statements'))->setValue(self::statementsOf($connection), []);

        $start = microtime(true);

        try {
            $connection->waitForStatements([$statement]);
            $this->fail('expected the statement to be rejected');
        } catch (StatementException $e) {
            $this->assertSame(ExceptionCode::STATEMENT_NOT_ON_THIS_CONNECTION->value, $e->getCode());
            $this->assertLessThan(1.0, microtime(true) - $start, 'it must fail at once rather than spin');
        }
    }

    public function testAsyncDeadlineRunsFromSendTimeNotFromWhenWaitingStarts(): void {
        // 3s budget, nearly all of it burned before the wait even begins, so
        // the wait itself should end almost at once. Were the budget restarted
        // when the wait began, it would instead last the full 3s.
        $connection = $this->connect(
            'slow-query',
            delaySeconds: 30.0,
            requestTimeoutInSeconds: 3.0,
            receiveTimeoutSeconds: 0.2,
        );

        $statement = $connection->queryAsync('SELECT * FROM system.local');
        usleep(2_800_000);

        $start = microtime(true);

        try {
            $connection->waitForStatements([$statement]);
            $this->fail('expected the request timeout to fire');
        } catch (RequestTimeoutException $e) {
            $elapsed = microtime(true) - $start;
            $this->assertLessThan(1.5, $elapsed, 'the budget should already have been nearly used up');
        }
    }

    public function testAsyncRequestTakesAPerCallTimeoutOverride(): void {
        // The counterpart of the argument syncRequest() takes: the connection
        // would allow 30s, the call asks for 1s for this statement alone.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: 30.0);

        $statement = $connection->asyncRequest(
            new Query('SELECT * FROM SLOW'),
            requestTimeoutInSeconds: 1.0,
        );

        $this->assertSame(1.0, $statement->getRequestTimeout(), 'the override must win over the connection default');

        $start = microtime(true);

        try {
            $connection->waitForStatements([$statement]);
            $this->fail('expected the per-call timeout to fire');
        } catch (RequestTimeoutException $e) {
            $this->assertSame([$statement], $e->getTimedOutStatements());
            $this->assertLessThan(10.0, microtime(true) - $start, 'the statement must be held to the overridden budget');
        }
    }

    public function testAWaitBoundDoesNotProbeTheConnectionWhenNothingWasRead(): void {
        // A poll that returns before it reads has learned nothing about the
        // connection, so it must not judge the heartbeat either: the probe's
        // answer can be sitting unread in the receive buffer, and taking that
        // for silence would cost a healthy connection.
        $connection = $this->connect(
            'idle',
            heartbeatIntervalInSeconds: 0.2,
            heartbeatTimeoutInSeconds: 0.4,
        );

        // A resolved statement, so that the poll below has nothing to read for.
        $statement = $connection->queryAsync('SELECT * FROM system.local');
        $connection->waitForStatements([$statement]);
        $this->assertTrue($statement->isResultReady());

        // Long enough for the probe to fall due, and for a read to send it.
        usleep(300_000);
        $connection->tryReadNextResponse();
        $this->assertNotNull($this->pendingHeartbeatOf($connection), 'the probe should be outstanding by now');

        // Past the heartbeat timeout, with its answer left unread.
        usleep(600_000);

        // Neither of these reads — every statement they were given is resolved,
        // and a limit of zero forbids it outright — so neither may declare the
        // connection dead.
        $this->assertSame(0, $connection->tryResolveStatements([$statement]));
        $this->assertSame(0, $connection->drainAvailableResponses(0));
        $this->assertTrue($connection->isConnected(), 'a poll that never read must not drop the connection');

        // A poll that does read finds the answer and clears the probe.
        $connection->drainAvailableResponses();
        $this->assertNull($this->pendingHeartbeatOf($connection), 'the answered probe should have been retired');
        $this->assertTrue($connection->isConnected());
    }

    public function testAWaitBoundEndsTheWaitWithoutTouchingTheStatements(): void {
        // The statement has a 30s budget but the caller only wants to wait 1s:
        // the wait ends, the statement keeps waiting, and nothing is given up on.
        $connection = $this->connect('defer-slow', delaySeconds: 20.0, requestTimeoutInSeconds: 30.0);

        $statement = $connection->queryAsync('SELECT * FROM SLOW');

        $start = microtime(true);
        $ready = $connection->waitForAnyStatement([$statement], timeoutInSeconds: 1.0);
        $elapsed = microtime(true) - $start;

        $this->assertNull($ready, 'nothing became ready within the wait bound');
        $this->assertLessThan(5.0, $elapsed, 'the wait bound, not the 30s budget, ended the wait');
        $this->assertFalse($statement->isTimedOut(), 'the statement still has budget left');
        $this->assertSame([], $this->orphanedStreamsOf($connection));
        $this->assertTrue($connection->isConnected());
    }

    public function testAWaitBoundIsHonouredWithTheTransportTimeoutDisabled(): void {
        $connection = $this->connect('idle', requestTimeoutInSeconds: null, receiveTimeoutSeconds: 0.0);

        $start = microtime(true);

        $this->assertNull($connection->waitForNextResponse(timeoutInSeconds: 1.0));

        $elapsed = microtime(true) - $start;

        $this->assertGreaterThan(0.9, $elapsed, 'the wait bound is a bound, not a hint');
        $this->assertLessThan(5.0, $elapsed);
        $this->assertTrue($connection->isConnected());
    }

    public function testAWaitBoundOfZeroDoesNotBlockOnTheTransport(): void {
        // A bound of 0 asks for a look, not a wait: it costs one read, but a
        // non-blocking one, so it must return long before the transport's stall
        // window is over.
        $connection = $this->connect(
            'defer-slow',
            delaySeconds: 30.0,
            requestTimeoutInSeconds: 30.0,
            receiveTimeoutSeconds: 5.0,
        );

        $statement = $connection->queryAsync('SELECT * FROM SLOW');

        $start = microtime(true);
        $connection->waitForStatements([$statement], timeoutInSeconds: 0.0);
        $elapsed = microtime(true) - $start;

        $this->assertLessThan(2.0, $elapsed, 'a bound of 0 must not wait on the transport');
        $this->assertFalse($statement->isResultReady());
        $this->assertFalse($statement->isTimedOut(), 'the statement still has its full budget');
    }

    public function testAWaitWithNothingElseBoundingItFallsBackToTheStallWindow(): void {
        // A transport read timeout is swallowed while something else still
        // bounds the wait — the caller's deadline or the next heartbeat will
        // end it, and a slow coordinator looks exactly like a quiet one. Here
        // neither exists: no wait bound, no request budget, no heartbeat. The
        // stall window is then the only judgement available and it is the
        // transport's own, so it must fail the connection rather than leave the
        // client blocked forever.
        $connection = $this->connect(
            'deaf',
            requestTimeoutInSeconds: null,
            heartbeatIntervalInSeconds: null,
            receiveTimeoutSeconds: 1.0,
        );

        $start = microtime(true);

        try {
            $connection->waitForNextEvent();
            $this->fail('an unbounded wait on a silent connection must not be unbounded in fact');
        } catch (NodeException $e) {
            $this->assertSame(ExceptionCode::SOCKET_TIMEOUT_DURING_READ->value, $e->getCode());
            $this->assertLessThan(10.0, microtime(true) - $start, 'the stall window, not forever');
            $this->assertFalse($connection->isConnected(), 'a connection that made no progress at all has failed');
        }
    }

    public function testAWaitWithNothingElseBoundingItStillRidesOutASlowAnswer(): void {
        // The counterpart: the same connection with a request in flight is
        // bounded by that request's budget, so a stall window elapsing while the
        // server is merely slow must be swallowed rather than fail anything.
        // The answer takes several times the 1s stall window.
        $connection = $this->connect(
            'slow-query',
            delaySeconds: 3.0,
            requestTimeoutInSeconds: 30.0,
            receiveTimeoutSeconds: 1.0,
        );

        $result = $connection->query('SELECT * FROM system.local');

        $this->assertInstanceOf(Result::class, $result);
        $this->assertTrue($connection->isConnected());
    }

    public function testDeadConnectionIsDetectedByTheHeartbeatLongBeforeTheRequestTimeout(): void {
        // The server never answers, so the query cannot tell "slow coordinator"
        // from "dead connection" on its own. The heartbeat can: it must fail the
        // query in interval + timeout rather than after the full 60s deadline.
        $connection = $this->connect(
            'deaf',
            requestTimeoutInSeconds: 60.0,
            heartbeatIntervalInSeconds: 0.5,
            heartbeatTimeoutInSeconds: 1.0,
        );

        $start = microtime(true);

        try {
            $connection->query('SELECT * FROM system.local');
            $this->fail('expected the heartbeat to declare the connection dead');
        } catch (ConnectionException $e) {
            $elapsed = microtime(true) - $start;
            $this->assertSame(ExceptionCode::CONNECTION_HEARTBEAT_TIMEOUT->value, $e->getCode());
            $this->assertLessThan(10.0, $elapsed, 'the heartbeat must not wait for the request timeout');
        }
    }

    public function testDriverHeartbeatIsNotSubjectToTheRequestTimeout(): void {
        // The heartbeat is the driver's own request, and the request timeout is
        // the shorter of the two here. It must still be held to the heartbeat
        // timeout alone: were it timed out as an ordinary statement it would be
        // reported to a caller who never sent it, and would park a stream id
        // every interval until the connection ran out of them.
        $connection = $this->connect(
            'deaf',
            requestTimeoutInSeconds: 0.5,
            heartbeatIntervalInSeconds: 0.2,
            heartbeatTimeoutInSeconds: 30.0,
        );

        $event = $connection->waitForNextEvent(timeoutInSeconds: 2.0);

        $this->assertNull($event, 'the wait must run its course rather than end on the heartbeat');
        $this->assertSame([], $this->orphanedStreamsOf($connection), 'the heartbeat must not park a stream id');
        $this->assertTrue($connection->isConnected(), 'the heartbeat timeout has not elapsed');
    }

    public function testEventWaitKeepsTheConnectionAliveWhileNothingHappens(): void {
        // The transport receive timeout is 1s and no event arrives for several
        // times that: an idle event stream must not be mistaken for a failure.
        $connection = $this->connect('idle');

        $start = microtime(true);
        $event = $connection->waitForNextEvent(timeoutInSeconds: 3.0);
        $elapsed = microtime(true) - $start;

        $this->assertNull($event, 'no event was pushed, so the wait should end empty-handed');
        $this->assertGreaterThan(2.5, $elapsed, 'the wait should last for the requested timeout, not the transport one');
        $this->assertTrue($connection->isConnected(), 'an idle event stream must leave the connection usable');

        // The connection is genuinely still usable.
        $connection->query('SELECT * FROM system.local');
    }

    public function testEventWaitReturnsAnEventThatArrivesAfterSeveralTransportTimeouts(): void {
        $connection = $this->connect('event', delaySeconds: 2.5);

        $event = $connection->waitForNextEvent(timeoutInSeconds: 10.0);

        $this->assertInstanceOf(StatusChangeEvent::class, $event);
    }

    public function testHoldingBackTooManyStreamIdsReplacesTheConnection(): void {
        // Two requests are given up on while only one held stream id is
        // allowed: the connection has accumulated too much debris to keep.
        $connection = $this->connect(
            'defer-slow',
            delaySeconds: 30.0,
            requestTimeoutInSeconds: 1.0,
            maxOrphanedStreams: 1,
        );

        $statements = [
            $connection->queryAsync('SELECT 1 FROM SLOW'),
            $connection->queryAsync('SELECT 2 FROM SLOW'),
        ];

        try {
            $connection->waitForStatements($statements);
            $this->fail('expected the connection to be replaced');
        } catch (ConnectionException $e) {
            // Raised rather than done quietly: the caller's connection is gone.
            $this->assertSame(ExceptionCode::CONNECTION_TOO_MANY_ORPHANED_STREAMS->value, $e->getCode());
            $this->assertFalse($connection->isConnected());
        }
    }

    public function testPerCallTimeoutArgumentBoundsTheHighLevelAsyncHelpers(): void {
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: 30.0);

        $statement = $connection->queryAsync('SELECT * FROM SLOW', requestTimeoutInSeconds: 1.0);

        $this->assertSame(1.0, $statement->getRequestTimeout(), 'the override must win over the connection default');

        $start = microtime(true);

        try {
            $connection->waitForStatements([$statement]);
            $this->fail('expected the per-call timeout to fire');
        } catch (RequestTimeoutException $e) {
            $this->assertSame([$statement], $e->getTimedOutStatements());
            $this->assertLessThan(10.0, microtime(true) - $start, 'the statement must be held to the overridden budget');
        }
    }

    public function testPerCallTimeoutArgumentBoundsTheHighLevelHelpers(): void {
        // The same override syncRequest()/asyncRequest() take, without having
        // to build an options object for it.
        $connection = $this->connect('slow-query', delaySeconds: 30.0, requestTimeoutInSeconds: 30.0);

        $start = microtime(true);

        try {
            $connection->query('SELECT * FROM system.local', requestTimeoutInSeconds: 1.0);
            $this->fail('expected the per-call timeout to fire');
        } catch (RequestTimeoutException $e) {
            $this->assertLessThan(10.0, microtime(true) - $start, 'the argument must win over the connection default');
        }
    }

    public function testPerCallTimeoutArgumentOutranksTheRequestsOwnOptions(): void {
        // Precedence runs argument, then request options, then connection: the
        // request asks for 1s but the call overrules it with room to spare.
        $connection = $this->connect('slow-query', delaySeconds: 1.0, requestTimeoutInSeconds: 1.0);

        $result = $connection->query(
            'SELECT * FROM system.local',
            options: new QueryOptions(requestTimeoutInSeconds: 1.0),
            requestTimeoutInSeconds: 30.0,
        );

        $this->assertInstanceOf(Result::class, $result);
    }

    public function testPerRequestTimeoutFromOptionsBoundsAnAsyncStatement(): void {
        $connection = $this->connect('slow-query', delaySeconds: 30.0, requestTimeoutInSeconds: 30.0);

        $statement = $connection->queryAsync(
            'SELECT * FROM system.local',
            options: new QueryOptions(requestTimeoutInSeconds: 1.0),
        );

        $this->assertSame(1.0, $statement->getRequestTimeout());

        $start = microtime(true);

        try {
            $connection->waitForStatements([$statement]);
            $this->fail('expected the per-request timeout to fire');
        } catch (RequestTimeoutException $e) {
            $elapsed = microtime(true) - $start;
            $this->assertLessThan(10.0, $elapsed, 'the statement must be held to its own budget');
        }
    }

    public function testPerRequestTimeoutFromOptionsBoundsAQuery(): void {
        // The connection would allow 30s; the request itself asks for 1s.
        $connection = $this->connect('slow-query', delaySeconds: 30.0, requestTimeoutInSeconds: 30.0);

        $start = microtime(true);

        try {
            $connection->query(
                'SELECT * FROM system.local',
                options: new QueryOptions(requestTimeoutInSeconds: 1.0),
            );
            $this->fail('expected the per-request timeout to fire');
        } catch (RequestTimeoutException $e) {
            $elapsed = microtime(true) - $start;
            $this->assertLessThan(10.0, $elapsed, 'the request option must win over the connection default');
        }
    }

    public function testSlowQueryFailsOnceTheRequestTimeoutIsReached(): void {
        $connection = $this->connect('slow-query', delaySeconds: 10.0, requestTimeoutInSeconds: 2.0);

        $start = microtime(true);

        try {
            $connection->query('SELECT * FROM system.local');
            $this->fail('expected the request timeout to fire');
        } catch (RequestTimeoutException $e) {
            $elapsed = microtime(true) - $start;
            $this->assertGreaterThan(1.5, $elapsed);
            $this->assertLessThan(8.0, $elapsed, "well under the server's 10s delay");
        }
    }

    public function testSlowQuerySucceedsWhenItFitsInTheRequestTimeout(): void {
        // Slower than the 1s transport receive timeout, well inside the request
        // timeout: the query must survive the transport timeouts in between.
        $connection = $this->connect('slow-query', delaySeconds: 3.0, requestTimeoutInSeconds: 10.0);

        $start = microtime(true);
        $connection->query('SELECT * FROM system.local');
        $elapsed = microtime(true) - $start;

        // What matters is that the query outlived the 1s transport timeout
        // without the connection being torn down, not how close to the
        // server's 3s delay it landed — the latter is only a wall clock the
        // test does not control.
        $this->assertGreaterThan(1.5, $elapsed, 'the server really did take longer than the transport timeout');
    }

    public function testStatementAbandonedByADroppedConnectionFailsImmediately(): void {
        $connection = $this->connect('idle', requestTimeoutInSeconds: 5.0);

        $statement = $connection->queryAsync('SELECT * FROM system.local');

        // Whatever closed the connection — a transport failure, or the caller —
        // the statement's stream id means nothing any more.
        $connection->disconnect();

        $start = microtime(true);

        try {
            $statement->getResult();
            $this->fail('expected the abandoned statement to be rejected');
        } catch (StatementException $e) {
            $elapsed = microtime(true) - $start;
            $this->assertTrue($statement->isAbandoned());
            $this->assertLessThan(0.5, $elapsed, 'an abandoned statement must fail at once, not wait out the request timeout');
            $this->assertSame(ExceptionCode::STATEMENT_ABANDONED->value, $e->getCode());
        }
    }

    public function testStreamIdIsReclaimedWhenARequestCannotBeEncoded(): void {
        // A request that fails to encode never reaches the node, so its stream
        // id was never in use. Keeping it would burn one id of the pool per
        // failure until the connection is replaced.
        $connection = $this->connect('idle');

        $options = new QueryOptions(autoPrepare: false);

        $before = $this->recycledStreamCountOf($connection);

        for ($i = 0; $i < 3; $i++) {
            try {
                $connection->query('SELECT * FROM quick WHERE id = ?', [new \stdClass()], options: $options);
                $this->fail('expected the request to fail encoding');
            } catch (RequestException $e) {
                $this->assertSame(ExceptionCode::REQUEST_VALUES_UNSUPPORTED_VALUE_TYPE->value, $e->getCode());
            }
        }

        $this->assertSame($before + 3, $this->recycledStreamCountOf($connection), 'each unused stream id must go back into the pool');

        $this->assertTrue($connection->isConnected(), 'the connection is fine — nothing reached the node');
        $this->assertSame([], $this->orphanedStreamsOf($connection), 'an unsent request is not an unanswered one');

        $connection->query('SELECT * FROM quick');
    }

    public function testStreamIdOfATimedOutStatementIsReclaimedWhenTheLateAnswerArrives(): void {
        $connection = $this->connect('defer-slow', delaySeconds: 2.0, requestTimeoutInSeconds: 0.5);

        $slow = $connection->queryAsync('SELECT * FROM SLOW');

        try {
            $connection->waitForStatements([$slow]);
            $this->fail('expected the slow statement to time out');
        } catch (RequestTimeoutException $e) {
        }

        $this->assertSame([$slow->getStreamId()], array_keys($this->orphanedStreamsOf($connection)));

        // Wait past the server's delay, then read: the late answer must be
        // discarded and its stream id handed back to the pool.
        usleep(2_500_000);
        $connection->query('SELECT * FROM quick');

        $this->assertSame([], $this->orphanedStreamsOf($connection), 'the late answer should have released the stream id');
    }

    public function testSyncQueryTimeoutKeepsTheConnectionAndReclaimsItsStreamId(): void {
        // Synchronous requests take a pooled stream id too, so giving up on one
        // costs that id rather than the whole connection (and with it the
        // prepared statement cache, which disconnecting would clear).
        $connection = $this->connect('defer-slow', delaySeconds: 2.0, requestTimeoutInSeconds: 0.5);

        try {
            $connection->query('SELECT * FROM SLOW');
            $this->fail('expected the request timeout to fire');
        } catch (RequestTimeoutException $e) {
        }

        $this->assertTrue($connection->isConnected(), 'a sync timeout must no longer drop the connection');
        $this->assertCount(1, $this->orphanedStreamsOf($connection), 'the abandoned stream id must be held back');

        // The same connection keeps serving requests.
        $connection->query('SELECT * FROM quick');

        // Once the late answer turns up, the parked id returns to the pool.
        usleep(2_500_000);
        $connection->query('SELECT * FROM quick');

        $this->assertSame([], $this->orphanedStreamsOf($connection));
    }

    public function testTimingOutOneAsyncStatementLeavesTheConnectionAndItsOtherStatementsIntact(): void {
        // The slow statement gives up after 1s; the fast one was answered long
        // before that and must still be readable, on the same connection.
        $connection = $this->connect('defer-slow', delaySeconds: 4.0, requestTimeoutInSeconds: 1.0);

        $slow = $connection->queryAsync('SELECT * FROM SLOW');
        $fast = $connection->queryAsync('SELECT * FROM quick');

        try {
            $connection->waitForStatements([$slow]);
            $this->fail('expected the slow statement to time out');
        } catch (RequestTimeoutException $e) {
        }

        $this->assertTrue($slow->isTimedOut());
        $this->assertTrue($connection->isConnected(), 'the connection must survive one statement timing out');
        $this->assertFalse($fast->isAbandoned(), 'the other statement must be untouched');

        // Both the pending statement and the connection still work.
        $fast->getResult();
        $connection->query('SELECT * FROM quick');
    }

    public function testUnansweredHeartbeatIsDetectedWhileWaitingForEvents(): void {
        // The server stops answering after the handshake, so the heartbeat sent
        // by the idle event wait goes unanswered and the connection is dropped.
        $connection = $this->connect('deaf', heartbeatIntervalInSeconds: 0.5, heartbeatTimeoutInSeconds: 1.0);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_HEARTBEAT_TIMEOUT->value);

        $connection->waitForNextEvent(timeoutInSeconds: 10.0);
    }

    public function testWaitingForTheNextResponseWithNothingOverdueReturnsNull(): void {
        // Nothing is in flight, so the wait expiring is not a failure of
        // anything: it reports the same way as an event wait that saw no event.
        $connection = $this->connect('idle', requestTimeoutInSeconds: 1.0);

        $this->assertNull($connection->waitForNextResponse());

        $this->assertTrue($connection->isConnected());
        $this->assertSame([], $this->orphanedStreamsOf($connection), 'nothing was in flight, so nothing should be orphaned');

        $connection->query('SELECT * FROM quick');
    }

    private function connect(
        string $mode,
        float $delaySeconds = 0.0,
        ?float $requestTimeoutInSeconds = 30.0,
        ?float $heartbeatIntervalInSeconds = null,
        float $heartbeatTimeoutInSeconds = 5.0,
        float $receiveTimeoutSeconds = self::RECEIVE_TIMEOUT_SECONDS,
        int $maxOrphanedStreams = 24,
        bool $autoConnect = true,
    ): Connection {
        $port = $this->startServer($mode, $delaySeconds);

        // Deadlines are handed to the read itself, so this no longer bounds
        // when one is noticed; it is varied here to prove exactly that — a
        // transport timeout far longer than the budget, or none at all, must
        // make no difference to when a deadline fires.
        $transportTimeout = [
            'sec' => (int) $receiveTimeoutSeconds,
            'usec' => (int) round(($receiveTimeoutSeconds - (int) $receiveTimeoutSeconds) * 1_000_000),
        ];

        $node = new SocketNodeConfig(
            host: '127.0.0.1',
            port: $port,
            socketOptions: [
                SO_RCVTIMEO => $transportTimeout,
                SO_SNDTIMEO => $transportTimeout,
            ],
        );

        $connection = new Connection(
            nodes: [$node],
            options: new ConnectionOptions(
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
                maxOrphanedStreams: $maxOrphanedStreams,
                heartbeatIntervalInSeconds: $heartbeatIntervalInSeconds,
                heartbeatTimeoutInSeconds: $heartbeatTimeoutInSeconds,
            ),
        );

        if ($autoConnect) {
            $connection->connect();
        }

        return $connection;
    }

    /**
     * @return array<int, float>
     */
    private function orphanedStreamsOf(Connection $connection): array {
        /** @var array<int, float> $orphaned */
        $orphaned = (new ReflectionProperty(StreamIdPool::class, 'orphanedStreams'))->getValue($this->streamIdPoolOf($connection));

        return $orphaned;
    }

    /**
     * How many PREPAREs the server has seen, from the lines it reports.
     */
    private function pendingHeartbeatOf(Connection $connection): ?Statement {
        return self::heartbeatOf($connection)->getProbe();
    }

    private function preparesSeenByServer(): int {
        if ($this->serverStdout === null) {
            return 0;
        }

        $reported = stream_get_contents($this->serverStdout);

        return $reported === false ? 0 : substr_count($reported, 'prepared ');
    }

    private function recycledStreamCountOf(Connection $connection): int {
        /** @var \SplQueue<int> $recycled */
        $recycled = (new ReflectionProperty(StreamIdPool::class, 'recycledStreams'))->getValue($this->streamIdPoolOf($connection));

        return count($recycled);
    }

    private function startServer(string $mode, float $delaySeconds): int {

        $descriptors = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $process = proc_open(
            ['php', __DIR__ . '/Support/fake-cassandra-server.php', $mode, (string) $delaySeconds],
            $descriptors,
            $pipes
        );

        if ($process === false) {
            $this->fail('could not start the fake Cassandra server');
        }

        $this->serverProcess = $process;

        // The server picks a free port and reports it as "ready <port>", which
        // also tells us it is listening before the client tries to connect.
        $ready = fgets($pipes[1]);
        if ($ready === false || !preg_match('/^ready (\d+)$/', trim($ready), $matches)) {
            $this->fail('fake Cassandra server did not start listening');
        }

        // Anything the server reports from here on is read without blocking,
        // so a test that never asks about it is not held up by it.
        stream_set_blocking($pipes[1], false);
        $this->serverStdout = $pipes[1];

        return (int) $matches[1];
    }

    private function stopServer(): void {
        if ($this->serverProcess === null) {
            return;
        }

        proc_terminate($this->serverProcess);
        proc_close($this->serverProcess);
        $this->serverProcess = null;
        $this->serverStdout = null;
    }

    private function streamIdPoolOf(Connection $connection): StreamIdPool {
        /** @var StreamIdPool $pool */
        $pool = (new ReflectionProperty(Session::class, 'streamIds'))->getValue(self::sessionOf($connection));

        return $pool;
    }
}
