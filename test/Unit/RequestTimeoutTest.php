<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\EventListener as EventListenerInterface;
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
use Cassandra\Response\Event;
use Cassandra\Response\Event\StatusChangeEvent;
use Cassandra\Response\Result;
use Cassandra\Statement;
use ReflectionMethod;
use Cassandra\Connection\NodeConnector;
use Cassandra\Connection\NodeHealth;
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

    public function testAFollowUpRequestIsNotSentOnceTheConnectionHasBeenReplaced(): void {
        // The same as above, but with the connection already back up by the time
        // the follow-up is chained — which is what a warnings listener issuing a
        // request of its own can do, since it runs before the dispatcher gets
        // that far. There is a node again, so nothing about it being non-null
        // says the statement's stream id still means anything: the id space
        // started over with the new connection, so writing on that id would
        // register the statement at a number the new connection is free to hand
        // to somebody else, and releasing it afterwards would be refused for the
        // stale generation and leak it for good.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0);

        $statement = $connection->queryAsync('SELECT * FROM SLOW');
        $connection->disconnect();
        $connection->connect();

        $this->assertTrue($connection->isConnected());

        $method = new ReflectionMethod(Session::class, 'chainAsyncRequest');

        try {
            $method->invoke(self::sessionOf($connection), new Query('SELECT * FROM SLOW'), $statement);
            $this->fail('expected the follow-up request to be given up on');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_CHAINED_REQUEST_CONNECTION_GONE->value, $e->getCode());
            $this->assertTrue($statement->isAbandoned());
            $this->assertSame(0, self::statementsOf($connection)->getCount(), 'nothing may be registered on the replacement connection');
        }

        // The replacement connection is untouched by any of it.
        $this->assertInstanceOf(Result::class, $connection->query('SELECT 1'));
    }

    public function testAHeartbeatDoesNotKillAConnectionThatIsStillDeliveringAnAnswer(): void {
        // The heartbeat exists to tell a dead connection from a quiet one, and
        // a connection carrying bytes is neither. A single answer can take
        // longer to arrive than the heartbeat timeout is long — a wide page, a
        // blob column, a slow link — and while one is being assembled no frame
        // completes, so nothing but the bytes themselves distinguishes that
        // transfer from silence. The probe's own answer cannot arrive first
        // either: frames are serialised on one socket, so a SUPPORTED queued
        // behind a large response comes after it.
        //
        // Judged on the probe's clock alone, this is the common case of an idle
        // connection woken by a big query: the wait that sends the request
        // finds the probe due and sends it, and the answer then takes longer
        // than the timeout to stream in.
        $connection = $this->connect(
            'trickle-result',
            delaySeconds: 3.0,
            requestTimeoutInSeconds: 60.0,
            heartbeatIntervalInSeconds: 0.2,
            heartbeatTimeoutInSeconds: 1.0,
        );

        // Long enough for the probe to fall due, so it is outstanding when the
        // transfer begins.
        usleep(300_000);

        $start = microtime(true);

        $connection->query('SELECT * FROM big');

        $this->assertGreaterThan(
            1.0,
            microtime(true) - $start,
            'the answer must have taken longer than the heartbeat timeout, or this proves nothing'
        );
        $this->assertTrue($connection->isConnected(), 'bytes were arriving the whole time, so nothing died');
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

    public function testAHeartbeatReplyIsNotHandedBackAsACallersResponse(): void {
        // The probe is the driver's own request, sent on a schedule the caller
        // knows nothing about. Handing its SUPPORTED back would answer a wait
        // for the next response with something nobody asked for, once every
        // heartbeat interval, and an application pumping the connection this way
        // would have to learn to recognise and skip it. The rest of the
        // machinery already passes the probe over; so does this.
        $connection = $this->connect(
            'idle',
            heartbeatIntervalInSeconds: 0.2,
            heartbeatTimeoutInSeconds: 5.0,
        );

        $requestsSentBeforeWait = $this->claimedStreamIdCountOf($connection);

        $this->assertNull(
            $connection->waitForNextResponse(timeoutInSeconds: 1.0),
            'the only traffic was the driver\'s own heartbeat, so none of it was the caller\'s to receive'
        );

        // Which proves nothing unless a probe really did go out in the meantime:
        // an idle connection that was never probed would return null here as
        // well. Every claim takes a fresh id until the space runs out, so the
        // counter is how many requests this connection has sent.
        $this->assertGreaterThan(
            $requestsSentBeforeWait,
            $this->claimedStreamIdCountOf($connection),
            'the wait was several heartbeat intervals long, so at least one probe should have been sent'
        );

        // And the probes were answered and accounted for rather than merely
        // hidden: the connection is still up, its ids came back, and the next
        // response read off it is the caller's own.
        $this->assertTrue($connection->isConnected());
        $this->assertSame([], $this->orphanedStreamsOf($connection), 'an answered probe holds nothing back');
        $this->assertInstanceOf(Result::class, $connection->query('SELECT * FROM quick'));
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

    public function testAMalformedResponseHeaderDropsTheConnectionInsteadOfDesynchronisingIt(): void {
        // A response header the reader refuses is not one bad answer: its nine
        // bytes are already off the buffer and the body they announced is still
        // on it, with nothing left that knows how long it is. Kept, the
        // connection would read every later response at the wrong offset —
        // failing request after request at best, and at worst parsing the drift
        // into a well-formed frame and handing somebody another request's
        // answer.
        //
        // Told apart from the reader failures that consume the whole frame
        // first (an unknown opcode, a result kind this driver has no class for),
        // which cost one request and deliberately leave the connection alone.
        $connection = $this->connect('bad-response-header', requestTimeoutInSeconds: 5.0);

        try {
            $connection->query('SELECT 1');
            $this->fail('expected the malformed header to be reported');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_PROTOCOL_VERSION_MISMATCH->value, $e->getCode());
        }

        $this->assertFalse($connection->isConnected(), 'a reader that lost its place must not keep its connection');

        // And the next request opens a fresh one and is answered on it, rather
        // than inheriting the drift.
        $this->assertInstanceOf(Result::class, $connection->query('SELECT 2'));
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

    public function testAReplacedConnectionsFailureLeavesTheNewConnectionsRequestsAlone(): void {
        // A sync wait that finds its connection replaced reports that and
        // unwinds — and on the way out it has a stream id to dispose of. That id
        // was handed out by the connection that is gone, whose id space the new
        // one has started over on, so the same number may well be registered to
        // somebody else by now: a request the listener sent on the connection it
        // opened. Disposing of it without asking which run of the id space it
        // belongs to unregisters that live request, which nothing can then
        // resolve, and strands the id it holds — outstanding for good, and not
        // even counted as orphaned, so maxOrphanedStreams never notices.
        //
        // The numbers line up on their own: both connections spend ids 0 and 1
        // on the handshake, so the query below and the listener's own request
        // are both id 2.
        $connection = $this->connect('event-then-reorder', delaySeconds: 0.5, requestTimeoutInSeconds: 30.0);

        $nested = null;
        $connection->registerEventListener(new class($connection, $nested) implements EventListenerInterface {
            public function __construct(
                private Connection $connection,
                private ?Statement &$nested,
            ) {
            }

            public function onEvent(Event $event): void {
                // The first event only: the statement it left behind is what
                // says the replacement has already happened.
                if ($this->nested !== null) {
                    return;
                }

                // Replaces the connection from inside the read that dispatched
                // this event, then puts a request on the new one.
                $this->connection->disconnect();
                $this->connection->connect();
                $this->nested = $this->connection->queryAsync('SELECT * FROM fresh');
            }
        });

        try {
            $connection->query('SELECT * FROM SLOW');
            $this->fail('expected the replaced connection to be reported');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_WAIT_CONNECTION_REPLACED->value, $e->getCode());
        }

        $this->assertInstanceOf(Statement::class, $nested, 'the listener must have run from inside the wait');
        $this->assertSame(
            $this->currentStreamGenerationOf($connection),
            $nested->getStreamGeneration(),
            'the listener\'s request must be on the connection that replaced the first',
        );

        // The request the listener sent is still the connection's to answer,
        // and is answered.
        $this->assertInstanceOf(Result::class, $nested->getResult());

        // Its id went back into circulation with the answer rather than being
        // left outstanding by the disposal that ran over it.
        $this->assertSame([], $this->orphanedStreamsOf($connection));
        $this->assertSame([], $this->outstandingStreamsOf($connection));
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
        // set alone would let the two statements below sit past their deadlines
        // — holding their stream ids — for as long as the long one takes.
        //
        // Two of them, with budgets well apart, because that is what makes the
        // check below independent of the wall clock: what distinguishes the two
        // implementations is not when either was parked but whether they were
        // parked at *different* times. Held to its own budget each is given up
        // on when that budget runs out, an interval apart; bounded by the waited
        // set instead, neither is noticed until the wait itself ends and both
        // are then parked in the same pass, microseconds apart. Comparing the
        // two against each other rather than against the clock means a machine
        // that stalls the process — which shifts both equally — cannot fail this.
        $connection = $this->connect('defer-slow', delaySeconds: 30.0, requestTimeoutInSeconds: null);

        $short = $connection->queryAsync('SELECT 1 FROM SLOW', options: new QueryOptions(requestTimeoutInSeconds: 0.5));
        $middle = $connection->queryAsync('SELECT 2 FROM SLOW', options: new QueryOptions(requestTimeoutInSeconds: 2.0));
        $long = $connection->queryAsync('SELECT 3 FROM SLOW', options: new QueryOptions(requestTimeoutInSeconds: 30.0));

        $start = microtime(true);

        // Only the long statement is asked about, so only its budget and this
        // bound can end the wait; the 3s bound is what actually returns.
        $connection->waitForStatements([$long], timeoutInSeconds: 3.0);
        $waitEndedAt = microtime(true);

        $this->assertTrue(
            $short->isTimedOut(),
            'a statement outside the waited set must still be given up on when its budget runs out'
        );
        $this->assertTrue($middle->isTimedOut(), 'and so must the second one');

        $orphaned = $this->orphanedStreamsOf($connection);
        $parkedIds = array_keys($orphaned);
        sort($parkedIds);
        $expectedIds = [$short->getStreamId(), $middle->getStreamId()];
        sort($expectedIds);
        $this->assertSame($expectedIds, $parkedIds);

        // The point of the test: each was noticed when its own budget ran out,
        // not merely by the time the wait happened to end. The budgets are 1.5s
        // apart, so anything approaching that gap can only have come from the
        // two deadlines being kept separately; bounding the read by the waited
        // set alone collapses it to the width of a single expire() pass.
        $this->assertGreaterThan(
            0.75,
            $orphaned[$middle->getStreamId()] - $orphaned[$short->getStreamId()],
            'the two statements must be parked on their own budgets, an interval apart, not together at the wait bound'
        );

        $this->assertFalse($long->isTimedOut(), 'the waited statement still has 30s to go');
        $this->assertGreaterThan(2.5, $waitEndedAt - $start, 'the caller-supplied bound is what ends the wait');
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

    public function testAsyncStatementAnsweredByANestedReadIsNotReportedAsATimeout(): void {
        // The counterpart of testSyncRequestIsNotLostToAReadNestedInsideItsWait()
        // for a statement. An async statement needs nothing put aside — a nested
        // read resolves it in place — but the wait it was resolved underneath
        // still has to hand that answer back rather than the budget that ran out
        // while the listener's own request was being waited for.
        //
        // What makes that safe is an ordering two methods apart, which is the
        // reason this is pinned here: readResponseUntil() reads the clock before
        // it dispatches the frame, so a listener that blocks for a second cannot
        // turn a deadline that had not passed when the frame arrived into one
        // that has by the time the pass acts on it. Get that the other way round
        // and this wait comes back to a deadline it thinks has expired and a
        // statement no longer among the pending ones — because it was answered —
        // and reports a timeout for a request the node answered on time.
        //
        // The timings put the answer inside the nested wait: the statement's
        // answer at 0.5s, its budget at 0.75s, the listener's own query only at
        // 1.0s. So the wait is still in the pass that dispatched the event when
        // its budget passes, with its answer already on it.
        $connection = $this->connect('event-then-reorder', delaySeconds: 0.5, requestTimeoutInSeconds: 30.0);

        $nestedResults = 0;
        $connection->registerEventListener(new class($connection, $nestedResults) implements EventListenerInterface {
            public function __construct(
                private Connection $connection,
                private int &$nestedResults,
            ) {
            }

            public function onEvent(Event $event): void {
                $this->connection->query('SELECT 1');
                $this->nestedResults++;
            }
        });

        $statement = $connection->queryAsync('SELECT * FROM SLOW', requestTimeoutInSeconds: 0.75);

        $result = $statement->getResult();

        $this->assertInstanceOf(Result::class, $result);
        $this->assertSame(1, $nestedResults, 'the listener must have run a request of its own inside the wait');
        $this->assertTrue($statement->isResultReady(), 'the answered statement must not be left reporting a timeout');
        $this->assertFalse($statement->isTimedOut());

        // The id the statement was sent on was released when its answer
        // arrived, so giving up on it afterwards would have parked an id that
        // is back in circulation — and, once the pool wraps, one another
        // request is using.
        $this->assertSame([], $this->orphanedStreamsOf($connection), 'an answered statement must not orphan its stream id');

        // And the connection is untouched by any of it.
        $connection->query('SELECT * FROM quick');
    }

    public function testASyncWaitIsRefusedOnceItsConnectionHasBeenReplaced(): void {
        // A sync wait matches its answer on the stream id alone, which is only
        // meaningful on the connection that handed the id out. Every failure
        // inside the driver that replaces the connection raises on its own, so
        // what is left is the one that does not: a listener — which runs from
        // inside the nested read, in the middle of this very wait — calling
        // disconnect() itself.
        //
        // Resuming would then reconnect on the next pass, and the fresh id space
        // is free to hand the same number to somebody else, whose answer would
        // match the test below and be returned as this caller's result. So the
        // wait asks the pool which run of the id space it is on, which is what
        // the statement path is told by isAbandoned().
        $connection = $this->connect('event-then-reorder', delaySeconds: 0.5, requestTimeoutInSeconds: 30.0);

        $disconnects = 0;
        $connection->registerEventListener(new class($connection, $disconnects) implements EventListenerInterface {
            public function __construct(
                private Connection $connection,
                private int &$disconnects,
            ) {
            }

            public function onEvent(Event $event): void {
                $this->connection->disconnect();
                $this->disconnects++;
            }
        });

        try {
            $connection->query('SELECT * FROM SLOW');
            $this->fail('expected the replaced connection to be reported');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_WAIT_CONNECTION_REPLACED->value, $e->getCode());
        }

        $this->assertSame(1, $disconnects, 'the listener must have run from inside the wait');

        // Reported rather than waited out: the wait must not have gone back for
        // another read, which would have opened a connection of its own.
        $this->assertFalse($connection->isConnected());
    }

    public function testATransportFailureOnASyncRequestIsRecordedAgainstTheNodeOnlyOnce(): void {
        // The node hangs up mid-request, so the read fails as the transport
        // failure it is. That is reported by the read loop, which drops the
        // connection — and the sync path wraps the same read in a catch of its
        // own as a safety net. Both reporting it would count two failures for
        // one, and the cooldown a node is put into is graded by that count, so
        // a single hang-up would cost the next connect twice the wait.
        $connection = $this->connect('close-on-query', requestTimeoutInSeconds: 10.0);

        $nodeConfig = $this->nodeConfigOf($connection);

        try {
            $connection->query('SELECT 1');
            $this->fail('expected the hang-up to surface as a transport failure');
        } catch (NodeException $e) {
        }

        $this->assertFalse($connection->isConnected(), 'the failed connection is dropped');
        $this->assertSame(
            1,
            $this->recordedFailuresFor($connection, $nodeConfig),
            'one hang-up is one failure, however many layers caught it'
        );
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

    public function testAWaitForAnyStatementHandsBackAStatementAnsweredAsTheBoundExpired(): void {
        // The answer is sitting unread in the receive buffer and the wait bound
        // has already passed, so the read that finds it and the bound that ends
        // the wait fall in the same pass. The statement is ready either way —
        // returning null then would have the caller conclude that nothing
        // arrived.
        $connection = $this->connect('slow-query', delaySeconds: 0.3, requestTimeoutInSeconds: 30.0);

        $statement = $connection->queryAsync('SELECT * FROM system.local');

        usleep(800_000);

        $ready = $connection->waitForAnyStatement([$statement], timeoutInSeconds: 0.0);

        $this->assertTrue($statement->isResultReady(), 'the read inside the wait resolved the statement');
        $this->assertSame($statement, $ready, 'the statement answered in the final pass must be handed back');
        $this->assertTrue($connection->isConnected());
    }

    public function testAWaitForAnyStatementOverNoStatementsReturnsAtOnce(): void {
        // Nothing was asked about, so nothing can become ready. Without a wait
        // bound of its own there would be nothing to end the wait either, and it
        // would read for good on a connection that is perfectly healthy.
        $connection = $this->connect('idle', requestTimeoutInSeconds: 30.0, heartbeatIntervalInSeconds: 0.2);

        $start = microtime(true);
        $ready = $connection->waitForAnyStatement([], timeoutInSeconds: null);
        $elapsed = microtime(true) - $start;

        $this->assertNull($ready);
        $this->assertLessThan(1.0, $elapsed, 'an empty set must not be waited on');
        $this->assertTrue($connection->isConnected());
    }

    public function testAWaitForStatementsReportsOneThatCanNeverResolveWhateverItsPlace(): void {
        // The unresolvable statement sits behind one that is merely slow. The
        // wait has to look past the first unresolved statement to find it, or
        // the caller is left waiting out the bound for an answer that cannot
        // come.
        $connection = $this->connect('defer-slow', delaySeconds: 20.0, requestTimeoutInSeconds: 30.0);

        $slow = $connection->queryAsync('SELECT * FROM SLOW');

        $foreign = $connection->queryAsync('SELECT * FROM SLOW');

        // Only this one is taken off the connection's books, which is what a
        // statement sent on another connection looks like from here. The slow
        // one stays pending, so it is the statement the wait would otherwise
        // stop at.
        $pending = new ReflectionProperty(StatementRegistry::class, 'statements');
        /** @var array<int, Statement> $inFlight */
        $inFlight = $pending->getValue(self::statementsOf($connection));
        unset($inFlight[$foreign->getStreamId()]);
        $pending->setValue(self::statementsOf($connection), $inFlight);

        $this->assertFalse($slow->isResultReady(), 'the slow statement must still be pending');

        $start = microtime(true);

        try {
            $connection->waitForStatements([$slow, $foreign], timeoutInSeconds: 5.0);
            $this->fail('expected the unresolvable statement to be reported');
        } catch (StatementException $e) {
            $this->assertSame(ExceptionCode::STATEMENT_NOT_ON_THIS_CONNECTION->value, $e->getCode());
            $this->assertLessThan(3.0, microtime(true) - $start, 'it must not be waited on first');
        }
    }

    public function testAWaitTimeoutOfNotANumberIsRejectedByEveryWait(): void {
        // NAN passes every comparison rather than failing them, so a deadline
        // derived from it is never reached — and it also answers "may this read
        // block?" with no, so the wait that could never end would not even have
        // waited. It is refused at the entry point instead, before any of that
        // reaches the read loop.
        $connection = $this->connect('idle', requestTimeoutInSeconds: 30.0);

        $statement = $connection->queryAsync('SELECT * FROM t');

        // Each wait is called for its refusal, never for what it returns, so
        // they are wrapped as void rather than given a common return type the
        // two that return nothing cannot honour.
        $waits = [
            'waitForNextEvent' => static function (Connection $c): void {
                $c->waitForNextEvent(NAN);
            },
            'waitForNextResponse' => static function (Connection $c): void {
                $c->waitForNextResponse(NAN);
            },
            'waitForStatements' => static function (Connection $c) use ($statement): void {
                $c->waitForStatements([$statement], NAN);
            },
            'waitForAnyStatement' => static function (Connection $c) use ($statement): void {
                $c->waitForAnyStatement([$statement], NAN);
            },
            'waitForAllPendingStatements' => static function (Connection $c): void {
                $c->waitForAllPendingStatements(NAN);
            },
        ];

        foreach ($waits as $name => $wait) {
            $start = microtime(true);

            try {
                $wait($connection);
                $this->fail($name . ' accepted a wait timeout of NAN');
            } catch (ConnectionException $e) {
                $this->assertSame(ExceptionCode::CONNECTION_INVALID_WAIT_TIMEOUT->value, $e->getCode(), $name);
                $this->assertLessThan(1.0, microtime(true) - $start, $name . ' must refuse before it waits or reads');
            }
        }

        // Refused rather than acted on: the connection and the statement it was
        // asked about are untouched, so a caller that fixes the argument can
        // simply wait again.
        $this->assertTrue($connection->isConnected());
        $this->assertFalse($statement->isTimedOut());
        $this->assertFalse($statement->isAbandoned());
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

    public function testHoldingBackTooManyStreamIdsStartsTheIdSpaceOver(): void {
        // The debris is the connection's, so it goes away with it. Left behind,
        // it would be counted against the connection that replaces this one and
        // every later piece of bookkeeping — a poll that reads nothing included
        // — would go on raising the failure that already happened.
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
            $this->assertSame(ExceptionCode::CONNECTION_TOO_MANY_ORPHANED_STREAMS->value, $e->getCode());
        }

        $this->assertSame([], $this->orphanedStreamsOf($connection), 'the orphaned ids belonged to the connection that is gone');

        // Raised once, for the caller whose connection it was; not again for
        // everyone who touches the client afterwards.
        $connection->tryResolveStatements([]);

        // And the statements themselves report what actually became of them:
        // they ran out of time, which is what parked their ids in the first
        // place — the connection being replaced came after.
        foreach ($statements as $statement) {
            $this->assertTrue($statement->isTimedOut());
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
        //
        // Unlike its neighbours this test has to outlive the server's delay, so
        // the delay cannot simply be set past anything the machine might do.
        // The budgets are therefore split rather than shortened: the connection
        // default is generous, so the quick queries below cannot go overdue on
        // a loaded machine, and only the slow query is given a short one of its
        // own — leaving an order of magnitude between it and the delay, which
        // is the margin that decides whether the timeout fires at all.
        $delaySeconds = 3.0;
        $connection = $this->connect('defer-slow', delaySeconds: $delaySeconds, requestTimeoutInSeconds: 30.0);

        try {
            $connection->query('SELECT * FROM SLOW', requestTimeoutInSeconds: 0.25);
            $this->fail('expected the request timeout to fire');
        } catch (RequestTimeoutException $e) {
        }

        $this->assertTrue($connection->isConnected(), 'a sync timeout must no longer drop the connection');
        $this->assertCount(1, $this->orphanedStreamsOf($connection), 'the abandoned stream id must be held back');

        // The same connection keeps serving requests.
        $connection->query('SELECT * FROM quick');

        // Once the late answer turns up, the parked id returns to the pool.
        // Read for it until it does rather than sleeping the delay out and
        // reading once: what is being pinned is that the late answer releases
        // the id, not how promptly the server got round to sending it.
        $this->drainUntilStreamIdsAreReclaimed($connection, withinSeconds: $delaySeconds + 10.0);

        $this->assertSame([], $this->orphanedStreamsOf($connection), 'the late answer should have released the stream id');
    }

    public function testSyncRequestIsNotLostToAReadNestedInsideItsWait(): void {
        // A synchronous request is the one kind the connection registers no
        // statement for, so its answer is delivered by being returned up the
        // stack to the loop that read it. Anything reached from inside a read
        // can read again — an event listener issuing a request of its own, here
        // — and that nested read takes the answer off the wire while looking for
        // a different stream id. Without somewhere to put it, the frame is
        // dropped and the caller waits out its whole budget for a request the
        // node answered on time.
        //
        // The server pushes the event before answering, and answers the
        // listener's own query only afterwards, so the nested wait is
        // guaranteed to be the one reading when the answer arrives.
        $connection = $this->connect('event-then-reorder', delaySeconds: 0.3, requestTimeoutInSeconds: 3.0);

        $nestedResults = 0;
        $connection->registerEventListener(new class($connection, $nestedResults) implements EventListenerInterface {
            public function __construct(
                private Connection $connection,
                private int &$nestedResults,
            ) {
            }

            public function onEvent(Event $event): void {
                $this->connection->query('SELECT 1');
                $this->nestedResults++;
            }
        });

        $started = microtime(true);
        $result = $connection->query('SELECT * FROM SLOW');
        $elapsed = microtime(true) - $started;

        $this->assertInstanceOf(Result::class, $result);
        $this->assertSame(1, $nestedResults, 'the listener must have run a request of its own inside the wait');
        $this->assertLessThan(3.0, $elapsed, 'the answer was there; the wait must not have run to its budget');
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

    public function testUnansweredHeartbeatDoesNotPutTheNodeIntoItsCooldown(): void {
        // The connection goes, but the node keeps its record. An unanswered
        // probe says this socket is finished, not that the node is bad — and a
        // node in its cooldown is tried last, which on a single-node
        // configuration means the reconnect has to wait for a node it has no
        // alternative to. Same reasoning as the orphaned-stream limit, which
        // has always left the record alone.
        $connection = $this->connect('deaf', heartbeatIntervalInSeconds: 0.5, heartbeatTimeoutInSeconds: 1.0);

        $nodeConfig = $this->nodeConfigOf($connection);

        try {
            $connection->waitForNextEvent(timeoutInSeconds: 10.0);
            $this->fail('expected the unanswered heartbeat to be detected');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_HEARTBEAT_TIMEOUT->value, $e->getCode());
        }

        $this->assertFalse($connection->isConnected(), 'the connection itself is dropped');
        $this->assertTrue(
            $this->nodeHealthOf($connection)->isAvailable($nodeConfig),
            'but the node is still one this connection would open on straight away'
        );
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

    /**
     * How many requests this connection has sent, counted through the stream id
     * pool: ids are handed out from a rising counter and only recycled once it
     * has run through the whole space, so before that the counter is exactly the
     * number of claims — the driver's own heartbeats among them.
     */
    private function claimedStreamIdCountOf(Connection $connection): int {
        /** @var int $nextStreamId */
        $nextStreamId = (new ReflectionProperty(StreamIdPool::class, 'nextStreamId'))->getValue($this->streamIdPoolOf($connection));

        return $nextStreamId;
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
     * Which run of the id space this connection is currently handing ids out
     * from, which is what tells an id claimed on a connection that has since
     * been replaced from the same number handed out by the one that replaced it.
     */
    private function currentStreamGenerationOf(Connection $connection): int {
        return $this->streamIdPoolOf($connection)->getGeneration();
    }

    /**
     * Read until every parked stream id has been handed back, or until
     * $withinSeconds is up.
     *
     * A parked id is only released when the late answer it is waiting for
     * arrives, so something has to read for that answer — but how long the
     * server takes to send it is the server's business, and sleeping a fixed
     * stretch and then reading once makes the test a bet on that. Polling ends
     * as soon as the id is back, and only gives up once waiting any longer
     * would mean the answer is not coming at all, which is the failure the
     * caller then asserts.
     *
     * @throws \Cassandra\Exception\CassandraException
     */
    private function drainUntilStreamIdsAreReclaimed(Connection $connection, float $withinSeconds): void {
        $deadline = microtime(true) + $withinSeconds;

        do {
            $connection->drainAvailableResponses();

            if ($this->orphanedStreamsOf($connection) === []) {
                return;
            }

            usleep(20_000);
        } while (microtime(true) < $deadline);
    }

    /**
     * The single node these tests are pointed at.
     */
    private function nodeConfigOf(Connection $connection): SocketNodeConfig {
        /** @var NodeConnector $nodeConnector */
        $nodeConnector = (new ReflectionProperty(Session::class, 'nodeConnector'))->getValue(self::sessionOf($connection));

        /** @var array<SocketNodeConfig> $nodes */
        $nodes = (new ReflectionProperty(NodeConnector::class, 'nodes'))->getValue($nodeConnector);

        return $nodes[0];
    }

    /**
     * How each node this connection knows about has been behaving.
     */
    private function nodeHealthOf(Connection $connection): NodeHealth {
        /** @var NodeConnector $nodeConnector */
        $nodeConnector = (new ReflectionProperty(Session::class, 'nodeConnector'))->getValue(self::sessionOf($connection));

        /** @var NodeHealth $health */
        $health = (new ReflectionProperty(NodeConnector::class, 'health'))->getValue($nodeConnector);

        return $health;
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
     * The ids this connection has handed out and not had back, which is what
     * makes the pool their owner. An id that leaves this set without being
     * recycled or parked is lost for the life of the connection.
     *
     * @return array<int>
     */
    private function outstandingStreamsOf(Connection $connection): array {
        /** @var array<int, true> $outstanding */
        $outstanding = (new ReflectionProperty(StreamIdPool::class, 'outstanding'))->getValue($this->streamIdPoolOf($connection));

        return array_keys($outstanding);
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

    /**
     * How many failures this connection has recorded against a node, which is
     * what decides how long its cooldown is.
     */
    private function recordedFailuresFor(Connection $connection, SocketNodeConfig $nodeConfig): int {
        /** @var array<string, array{failures: int, cooldown_until: float}> $statusByKey */
        $statusByKey = (new ReflectionProperty(NodeHealth::class, 'statusByKey'))->getValue($this->nodeHealthOf($connection));

        return $statusByKey[$nodeConfig->host . ':' . $nodeConfig->port]['failures'] ?? 0;
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
