<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request;
use Cassandra\Response;
use Cassandra\Statement;
use Cassandra\StatementStatus;

/**
 * Getting a request onto the wire, and — on the sync path — waiting for its
 * answer.
 *
 * The three ways a request is sent differ in what they owe the stream id they
 * claim, which is what most of the care here is about. An id that is claimed
 * and then lost to a failure on the way to the node is gone for the lifetime of
 * the connection, so every path that can fail between claiming an id and
 * registering the statement that holds it puts the id back.
 *
 * {@see self::sendSyncRequest()} writes and then waits, so its id is released
 * when the answer arrives and parked when it cannot be known whether one did.
 * {@see self::sendAsyncRequest()} writes and hands back a
 * {@see \Cassandra\Statement} that holds the id until the answer arrives.
 * {@see self::chainAsyncRequest()} sends a follow-up request on the id an
 * existing statement already holds — the repreparation and auto-prepare paths —
 * so the statement is given a new budget rather than a new id.
 *
 * @internal this is not part of the public API and may change at any time
 */
final class RequestExecutor {
    private Deadline $deadlines;

    private ResponseDispatcher $dispatcher;

    private PreparedResultCache $preparedResultCache;

    private Session $session;

    private StatementRegistry $statements;

    private StreamIdPool $streamIds;

    public function __construct(
        Session $session,
        ResponseDispatcher $dispatcher,
        StatementRegistry $statements,
        StreamIdPool $streamIds,
        Deadline $deadlines,
        PreparedResultCache $preparedResultCache,
    ) {
        $this->session = $session;
        $this->dispatcher = $dispatcher;
        $this->statements = $statements;
        $this->streamIds = $streamIds;
        $this->deadlines = $deadlines;
        $this->preparedResultCache = $preparedResultCache;
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     */
    public function chainAsyncRequest(Request\Request $request, Statement $statement): void {

        $streamId = $statement->getStreamId();

        $streamGeneration = $statement->getStreamGeneration();

        if ($statement->isAbandoned() || $streamGeneration !== $this->streamIds->getGeneration()) {
            $statement->setStatus(StatementStatus::ABANDONED);

            throw new ConnectionException(
                'The connection this statement was sent on was closed before its follow-up request could be sent, so the request was given up on. Send it again.',
                ExceptionCode::CONNECTION_CHAINED_REQUEST_CONNECTION_GONE->value,
                [
                    'operation' => 'chainAsyncRequest',
                    'stream_id' => $streamId,
                    'request_class' => get_class($request),
                    'stream_generation' => $streamGeneration,
                    'current_stream_generation' => $this->streamIds->getGeneration(),
                ]
            );
        }

        // Checked ahead of the try below rather than inside it, because this is
        // the one failure whose stream id is not this statement's to give back:
        // another statement is registered at it, so the disposal that try owes a
        // send that never happened would put a live request's id back into
        // circulation — and {@see StreamIdPool::release()} could not tell, since
        // the id really is outstanding, just not ours. Nothing has been written
        // or claimed yet either, so the statement is all there is to finish.
        if ($this->statements->has($streamId)) {
            $statement->setStatus(StatementStatus::ABANDONED);

            throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                'operation' => 'chainAsyncRequest',
                'stream_id' => $streamId,
            ]);
        }

        // The statement is not among the pending ones at this point: the
        // response that triggered the follow-up took it out of the map. Nothing
        // else would mark it if the follow-up never goes out, so it is done
        // here — a statement left neither pending nor finished would wait on a
        // stream id that the next connection may well hand to somebody else,
        // and would be resolved by that request's answer. This covers every way
        // of failing, not just the write: reaching a node and claiming the
        // stream id can fail too, and leave the statement just as stranded.
        $requestWasSent = false;

        try {
            // Deliberately not getConnectedNode(): that would open a fresh
            // connection, and this request cannot go on one. The stream id it
            // carries was handed out by the connection that is gone, whose id
            // space a new connection starts over — sending on it would register
            // a statement at an id the new connection is free to hand to
            // somebody else, and the statement was marked as abandoned along
            // with its connection anyway, so it could never be resolved.
            $node = $this->session->getNode();
            if ($node === null) {
                throw new ConnectionException(
                    'The connection this statement was sent on was closed before its follow-up request could be sent, so the request was given up on. Send it again.',
                    ExceptionCode::CONNECTION_CHAINED_REQUEST_CONNECTION_GONE->value,
                    [
                        'operation' => 'chainAsyncRequest',
                        'stream_id' => $streamId,
                        'request_class' => get_class($request),
                    ]
                );
            }

            $request->setVersion($this->session->getProtocolVersion());
            $this->applyDefaultKeyspace($request);
            $request->setStream($streamId);

            $sentAt = 0.0;

            try {
                $node->writeRequest($request);

                // Read after the write rather than before it, for the reason
                // {@see HeartbeatMonitor::recordProbe()} gives about its own
                // probe, and as the {@see Statement} constructor does for a
                // request sent by {@see self::sendAsyncRequest()}: a write that
                // waits on a slow transport is not time the node spent failing
                // to answer, and charging it to the follow-up's budget would let
                // the request be overdue before the node has seen it.
                $sentAt = microtime(true);

                $this->session->recordNodeSuccess($node->getConfig());
            } catch (NodeException $e) {
                $this->session->handleNodeException($node);

                throw $e;
            }

            $requestWasSent = true;
        } finally {
            if (!$requestWasSent) {
                $statement->setStatus(StatementStatus::ABANDONED);

                // Nothing reached the node and no statement holds this id any
                // more, so it goes back into circulation instead of being
                // burned for the lifetime of the connection. One release covers
                // every way of failing inside this try: a node failure among
                // them took the whole pool with it, and
                // {@see StreamIdPool::release()} passes over an id whose pool
                // has been started over rather than leaving that to be told
                // apart here. The one failure whose id is somebody else's is
                // kept out of the try altogether, see the guard above.
                $this->streamIds->release($streamId, $streamGeneration);
            }
        }

        $statement->setRequest($request);
        $statement->setResponse(null);

        // A follow-up request (repreparation, auto-prepare) is a new wait, so
        // it gets its own budget rather than inheriting the original one.
        $statement->setSentAt($sentAt);

        // Registered last, once the statement says what it is now waiting for
        // and until when. The registry works out the earliest deadline among
        // everything in flight and remembers it until its contents change, so a
        // statement put back into it before its new budget was set would be
        // reckoned with under the old one; see {@see StatementRegistry::$revision}.
        $this->statements->register($streamId, $statement);
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function sendAsyncRequest(Request\Request $request, ?float $requestTimeoutInSeconds = null): Statement {

        $this->deadlines->assertValidRequestTimeout($requestTimeoutInSeconds, 'asyncRequest');

        $node = $this->session->getConnectedNode();

        $request->setVersion($this->session->getProtocolVersion());

        // Before the auto-prepare below is worked out, so that the PREPARE it
        // derives from this request's options inherits the keyspace along with
        // everything else.
        $this->applyDefaultKeyspace($request);

        $originalRequest = $request;

        // Resolved before a stream id is claimed, as the sync path does: this
        // can fail on the caller's request, and an id claimed beforehand would
        // be burned for the lifetime of the connection by a failure that never
        // reached the node.
        $autoPrepareRequest = $this->dispatcher->getAutoPrepareRequestIfNeeded($request);
        if ($autoPrepareRequest !== null) {
            $request = $autoPrepareRequest;
        }

        // Resolved once, here, rather than separately for the wait below and
        // for the statement: an explicit argument wins over what the request's
        // options asked for, and only then does the connection default apply
        // (null all the way through, which is what {@see Deadline} reads as
        // "use the connection's"). The auto-prepared PREPARE is consulted last
        // because it stands in for the query and inherits its timeout, so it
        // only has anything to say where the caller's own request had nothing.
        $requestTimeoutInSeconds ??= $originalRequest->getRequestTimeout() ?? $request->getRequestTimeout();

        $streamId = $this->session->getNewStreamId($requestTimeoutInSeconds);

        // Read straight after the claim, while it is still the run of the id
        // space that claim came from: nothing between here and the disposals
        // below reads, so nothing can start the pool over in between.
        $streamGeneration = $this->streamIds->getGeneration();

        $originalRequest->setStream($streamId);
        $request->setStream($streamId);

        // Whether the id is either in use or already back in circulation.
        // Everything between claiming it and registering the statement that
        // carries it can fail, and an id left behind by one of those failures
        // is lost for the lifetime of the connection.
        $streamIdAccountedFor = false;

        // Whether the request is on the wire, which decides how the id is
        // disposed of below: one the node has seen may still be answered on.
        $requestReachedNode = false;

        try {
            // Looked up for the request that is about to go out rather than for
            // the one the caller handed in, so that an auto-prepared query is
            // spared the PREPARE just as an explicit prepareAsync() is. The sync
            // path gets this for free by recursing into sendSyncRequest() for
            // its PREPARE.
            if ($request instanceof Request\Prepare) {
                $cachedResult = $this->preparedResultCache->get($request);
                if ($cachedResult !== null) {
                    $statement = new Statement(
                        connection: $this->session->getConnection(),
                        streamId: $streamId,
                        streamGeneration: $streamGeneration,
                        request: $request,
                        originalRequest: $originalRequest,
                        requestTimeoutInSeconds: $requestTimeoutInSeconds,
                    );

                    if ($autoPrepareRequest !== null) {
                        // Nothing has been written yet: what this statement is
                        // really waiting for is the EXECUTE that handling the
                        // cached result chains onto it below.
                        $statement->setStatus(StatementStatus::AUTO_PREPARING);
                    }

                    // From here the statement owns the id: handling the cached
                    // result either resolves it, which puts the id back below,
                    // or chains a follow-up request that takes the id over —
                    // and where the handling fails,
                    // {@see StatementRegistry::releaseAfterFailedResponseHandling()}
                    // puts it back.
                    $streamIdAccountedFor = true;

                    $handled = false;

                    try {
                        $response = $this->dispatcher->handleResponse($statement->getRequest(), $cachedResult, $statement);
                        $handled = true;
                    } finally {
                        if (!$handled) {
                            $this->statements->releaseAfterFailedResponseHandling($statement, $streamId);
                        }
                    }

                    if ($response !== null) {
                        $statement->setResponse($response);
                        $this->streamIds->release($streamId, $streamGeneration);
                    }

                    return $statement;
                }
            }

            if ($this->statements->has($streamId)) {
                // Accounted for by the statement already registered at this id,
                // not by this call: the disposals below must leave it alone, or
                // an id a live request is still waiting on would go back into
                // circulation — and {@see StreamIdPool::release()} could not tell,
                // since the id really is outstanding, just not ours.
                $streamIdAccountedFor = true;

                throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                    'operation' => 'sendAsyncRequest',
                    'stream_id' => $streamId,
                ]);
            }

            try {
                $node->writeRequest($request);
                $requestReachedNode = true;

                $this->session->recordNodeSuccess($node->getConfig());
            } catch (NodeException $e) {
                $this->session->handleNodeException($node);

                throw $e;
            }

            $statement = new Statement(
                connection: $this->session->getConnection(),
                streamId: $streamId,
                streamGeneration: $streamGeneration,
                request: $request,
                originalRequest: $originalRequest,
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
            );

            $this->statements->register($streamId, $statement);
            $streamIdAccountedFor = true;

            if ($autoPrepareRequest !== null) {
                $statement->setStatus(StatementStatus::AUTO_PREPARING);
            } else {
                $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
            }

            return $statement;
        } finally {
            if (!$streamIdAccountedFor) {
                if ($requestReachedNode) {
                    // Written, but no statement ended up holding the id, so an
                    // answer may still turn up on it: recycling it would let
                    // that answer resolve somebody else's request. Held back
                    // instead, and released once the answer proves the node is
                    // done with it, as {@see StatementRegistry::expire()} does.
                    $this->streamIds->park($streamId, $streamGeneration);
                } else {
                    // Claimed, and then something on the way to sending failed
                    // without the request ever reaching the node — an
                    // unencodable request, say, or a node failure, which took
                    // the whole pool with it and leaves
                    // {@see StreamIdPool::release()} to pass this over. Nothing
                    // will ever answer on the id, so it goes back into
                    // circulation rather than being burned.
                    $this->streamIds->release($streamId, $streamGeneration);
                }
            }
        }
    }

    /**
     * The body of {@see \Cassandra\Connection::syncRequest()}, carrying the repreparation depth
     * of the chain this call belongs to.
     *
     * @param ?float $requestTimeoutInSeconds see {@see \Cassandra\Connection::syncRequest()}
     * @param int $repreparationDepth how many repreparations this call is
     * already nested inside, the counterpart of
     * {@see Statement::getRepreparationCount()} for requests that have no
     * statement to carry the count. The sync path recurses back into this
     * method for each round and unwinds in order, so the depth is exactly the
     * number of rounds this call has behind it.
     *
     * Handed down the call chain rather than kept on the connection, so that a
     * request started from inside one of these calls — an event or warnings
     * listener issuing a query of its own while a repreparation is being
     * handled — begins its own chain at zero instead of inheriting this one's
     * depth and running out of repreparations early.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     */
    public function sendSyncRequest(Request\Request $request, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): Response\Response {

        $this->deadlines->assertValidRequestTimeout($requestTimeoutInSeconds, 'syncRequest');

        $node = $this->session->getConnectedNode();

        // An explicit argument wins over what the request's options asked for,
        // which in turn wins over the connection default.
        $requestTimeoutInSeconds ??= $request->getRequestTimeout();

        $request->setVersion($this->session->getProtocolVersion());

        // Before the cache is consulted below, because the prepared-result cache
        // is keyed on the keyspace as well as on the query — two connections on
        // different keyspaces preparing the same CQL are two different prepared
        // statements, and looking one up before the keyspace is filled in would
        // ask under the wrong key.
        $this->applyDefaultKeyspace($request);

        if ($request instanceof Request\Prepare) {
            $cachedResult = $this->preparedResultCache->get($request);
            if ($cachedResult !== null) {
                return $cachedResult;
            }
        }

        $autoPrepareRequest = $this->dispatcher->getAutoPrepareRequestIfNeeded($request);
        if ($autoPrepareRequest !== null) {

            $prepareResponse = $this->sendSyncRequest($autoPrepareRequest, $requestTimeoutInSeconds, $repreparationDepth);
            if (!($prepareResponse instanceof Response\Result\PreparedResult)) {
                throw new ConnectionException('Unexpected response type during prepare', ExceptionCode::CONNECTION_PREPARE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result\PreparedResult::class,
                    'received' => get_class($prepareResponse),
                ]);
            }

            $response = $this->dispatcher->handleAutoPrepareResult($prepareResponse, originalRequest: $request, requestTimeoutInSeconds: $requestTimeoutInSeconds, repreparationDepth: $repreparationDepth);
            if ($response === null) {
                throw new ConnectionException('Unexpected null response during autoPrepare', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE->value, [
                    'expected' => Response\Result::class,
                    'received' => 'null',
                ]);
            }

            return $response;
        }

        $streamId = $this->session->getNewStreamId($requestTimeoutInSeconds);
        $streamGeneration = $this->streamIds->getGeneration();
        $request->setStream($streamId);

        $writeSucceeded = false;

        try {
            $node->writeRequest($request);
            $writeSucceeded = true;

            // As on the async paths: the node took the request, which is enough
            // to clear a failure recorded against it. Whether it answers is a
            // separate question, and recorded separately below.
            $this->session->recordNodeSuccess($node->getConfig());
        } catch (NodeException $e) {
            $this->session->handleNodeException($node);

            throw $e;
        } finally {
            if (!$writeSucceeded) {
                // Nothing reached the node — an unencodable request, say, or a
                // node failure — so the stream id was never in use and goes
                // straight back into circulation; leaving it behind would burn
                // one id of the pool per failure for the lifetime of the
                // connection. A node failure took the whole pool with it, which
                // {@see StreamIdPool::release()} passes over of its own accord.
                $this->streamIds->release($streamId, $streamGeneration);
            }
        }

        $responseArrived = false;

        try {
            $response = $this->session->getNextResponseForStream(
                streamId: $streamId,
                streamGeneration: $streamGeneration,
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
                requestClass: get_class($request),
            );

            $responseArrived = true;

            $this->streamIds->release($streamId, $streamGeneration);

            $response = $this->dispatcher->handleResponse($request, $response, requestTimeoutInSeconds: $requestTimeoutInSeconds, repreparationDepth: $repreparationDepth);
            $this->session->recordNodeSuccess($node->getConfig());
        } catch (NodeException $e) {
            if ($this->session->getNode() === $node) {
                $this->session->handleNodeException($node);
            }

            throw $e;
        } finally {
            if (!$responseArrived) {
                // Nothing came back for this id and, unlike an async request,
                // there is no statement carrying it: without this it would be
                // lost for the life of the connection. A timeout has parked it
                // already and a node failure took the whole pool with it, so
                // this is for the rest — a malformed frame, say, which leaves
                // it undecidable whether the answer was consumed.
                $this->session->parkUnresolvedStream($streamId, $streamGeneration);
            }
        }

        if ($response === null) {
            throw new ConnectionException('Received unexpected null response from server.', ExceptionCode::CONNECTION_SYNC_NULL_RESPONSE->value, [
                'operation' => 'syncRequest',
                'request_class' => get_class($request),
            ]);
        }

        if ($response instanceof Response\Error) {
            throw $response->getException();
        }

        return $response;
    }

    /**
     * Put the connection's keyspace into a request that names none of its own.
     *
     * Done here rather than where the request was built, because whether it is
     * needed at all depends on the negotiated protocol version and that is only
     * settled once the connection is up — which, one line above every call to
     * this, it now is.
     *
     * Only from v5: before that a keyspace is a property of the node's session,
     * put there by the USE that {@see Session::connect()} sends, and the request
     * option does not exist at all — attaching one would make the request
     * unencodable.
     *
     * Which is why this takes one off rather than simply doing nothing wherever
     * there is no default to apply: a request object sent once on a v5
     * connection still carries the keyspace that send gave it. Below v5
     * encoding it would fail outright, and on a v5 connection that has been
     * moved off its keyspace it would quietly run the statement somewhere the
     * connection no longer admits to being. Only a keyspace this driver put
     * there is taken back; see {@see Request\Request::clearDefaultKeyspace()}.
     *
     * The request is modified in place, as {@see Request\Request::setStream()}
     * and setVersion() already are: a request handed to the executor is the
     * connection's to finish addressing.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    private function applyDefaultKeyspace(Request\Request $request): void {

        if ($this->session->getProtocolVersion()->value < ProtocolVersion::V5->value) {
            $request->clearDefaultKeyspace();

            return;
        }

        $keyspace = $this->session->getKeyspace();
        if ($keyspace === '') {
            $request->clearDefaultKeyspace();

            return;
        }

        $request->applyDefaultKeyspace($keyspace);
    }
}
