<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\NodeException;
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
     */
    public function chainAsyncRequest(Request\Request $request, Statement $statement): void {

        $streamId = $statement->getStreamId();

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
            $request->setStream($streamId);

            if ($this->statements->has($streamId)) {
                throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                    'operation' => 'chainAsyncRequest',
                    'stream_id' => $streamId,
                ]);
            }

            $writeSucceeded = false;
            $nodeFailed = false;
            $sentAt = 0.0;

            try {
                $node->writeRequest($request);
                $writeSucceeded = true;

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
                $nodeFailed = true;

                $this->session->handleNodeException($node);

                throw $e;
            } finally {
                if (!$writeSucceeded && !$nodeFailed) {
                    // Nothing reached the node, so the connection is fine and
                    // the stream id was never in use: it goes back into
                    // circulation instead of being burned. A node failure needs
                    // no such care, because it takes the whole pool with it.
                    $this->streamIds->release($streamId);
                }
            }

            $requestWasSent = true;
        } finally {
            if (!$requestWasSent) {
                $statement->setStatus(StatementStatus::ABANDONED);
            }
        }

        $this->statements->register($streamId, $statement);

        $statement->setRequest($request);
        $statement->setResponse(null);

        // A follow-up request (repreparation, auto-prepare) is a new wait, so
        // it gets its own budget rather than inheriting the original one.
        $statement->setSentAt($sentAt);
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

        $originalRequest = $request;

        // Resolved before a stream id is claimed, as the sync path does: this
        // can fail on the caller's request, and an id claimed beforehand would
        // be burned for the lifetime of the connection by a failure that never
        // reached the node.
        $autoPrepareRequest = $this->dispatcher->getAutoPrepareRequestIfNeeded($request);
        if ($autoPrepareRequest !== null) {
            $request = $autoPrepareRequest;
        }

        // Same precedence the Statement applies below: an explicit argument
        // wins over what the request's options asked for, and only then does
        // the connection default apply.
        $streamId = $this->session->getNewStreamId($requestTimeoutInSeconds ?? $originalRequest->getRequestTimeout());
        $originalRequest->setStream($streamId);
        $request->setStream($streamId);

        // Whether the id is either in use or already back in circulation.
        // Everything between claiming it and registering the statement that
        // carries it can fail, and an id left behind by one of those failures
        // is lost for the lifetime of the connection.
        $streamIdAccountedFor = false;

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
                        connection: $this->session->connection(),
                        streamId: $streamId,
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
                            $this->statements->releaseAfterFailedResponseHandling($statement, $streamId, $this->session->getNode() !== null);
                        }
                    }

                    if ($response !== null) {
                        $statement->setResponse($response);
                        $this->streamIds->release($streamId);
                    }

                    return $statement;
                }
            }

            if ($this->statements->has($streamId)) {
                throw new ConnectionException('Stream ID already in use', ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value, [
                    'operation' => 'sendAsyncRequest',
                    'stream_id' => $streamId,
                ]);
            }

            $writeSucceeded = false;
            $nodeFailed = false;

            try {
                $node->writeRequest($request);
                $writeSucceeded = true;

                $this->session->recordNodeSuccess($node->getConfig());
            } catch (NodeException $e) {
                $nodeFailed = true;

                // A node failure takes the whole pool with it, so this id needs
                // no care of its own — and putting it back would mean putting
                // it into the pool of the connection that replaces this one,
                // which hands the same ids out from scratch.
                $streamIdAccountedFor = true;

                $this->session->handleNodeException($node);

                throw $e;
            } finally {
                if (!$writeSucceeded && !$nodeFailed) {
                    // Nothing reached the node — an unencodable request, say —
                    // so the stream id was never in use and goes straight back
                    // into circulation.
                    $this->streamIds->release($streamId);
                    $streamIdAccountedFor = true;
                }
            }

            $statement = new Statement(
                connection: $this->session->connection(),
                streamId: $streamId,
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
                // Claimed, and then something on the way to sending failed
                // without the request ever reaching the node: no statement
                // holds this id and nothing will ever answer on it, so it goes
                // back into circulation rather than being burned.
                $this->streamIds->release($streamId);
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
                    'expected' => Response\Result::class,
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
        $request->setStream($streamId);

        $writeSucceeded = false;
        $nodeFailed = false;

        try {
            $node->writeRequest($request);
            $writeSucceeded = true;

            // As on the async paths: the node took the request, which is enough
            // to clear a failure recorded against it. Whether it answers is a
            // separate question, and recorded separately below.
            $this->session->recordNodeSuccess($node->getConfig());
        } catch (NodeException $e) {
            $nodeFailed = true;

            $this->session->handleNodeException($node);

            throw $e;
        } finally {
            if (!$writeSucceeded && !$nodeFailed) {
                // Nothing reached the node — an unencodable request, say — so
                // the stream id was never in use and goes straight back into
                // circulation; leaving it behind would burn one id of the pool
                // per failure for the lifetime of the connection. A node
                // failure needs no such care, because it takes the whole pool
                // with it.
                $this->streamIds->release($streamId);
            }
        }

        $responseArrived = false;

        try {
            $response = $this->session->getNextResponseForStream(
                streamId: $streamId,
                requestTimeoutInSeconds: $requestTimeoutInSeconds,
                requestClass: get_class($request),
            );

            $responseArrived = true;

            $this->streamIds->release($streamId);

            $response = $this->dispatcher->handleResponse($request, $response, requestTimeoutInSeconds: $requestTimeoutInSeconds, repreparationDepth: $repreparationDepth);
            $this->session->recordNodeSuccess($node->getConfig());
        } catch (NodeException $e) {
            $this->session->handleNodeException($node);

            throw $e;
        } finally {
            if (!$responseArrived) {
                // Nothing came back for this id and, unlike an async request,
                // there is no statement carrying it: without this it would be
                // lost for the life of the connection. A timeout has parked it
                // already and a node failure took the whole pool with it, so
                // this is for the rest — a malformed frame, say, which leaves
                // it undecidable whether the answer was consumed.
                $this->session->parkUnresolvedStream($streamId);
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
}
