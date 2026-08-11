<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Request;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Response;
use Cassandra\Statement;
use Cassandra\StatementStatus;
use Cassandra\Value\NotSet;
use Cassandra\Value\ValueBase;

/**
 * What becomes of an answer once it has been read off the wire.
 *
 * Most answers are simply handed back, but three kinds are the driver's own
 * business rather than the caller's, and all three end in another request being
 * sent before the caller sees anything:
 *
 * A query with values the driver cannot encode on its own is prepared first, so
 * that the node's own bind marker types can be used — {@see self::getAutoPrepareRequestIfNeeded()}
 * decides that before the query goes out, and {@see self::handleAutoPrepareResult()}
 * turns the prepared statement into the EXECUTE that replaces it.
 *
 * A prepared statement the node has forgotten (UNPREPARED) is prepared again
 * and re-executed, bounded by {@see self::MAX_REPREPARATIONS}.
 *
 * A prepared result is cached, so the next prepare of the same query costs no
 * round trip.
 *
 * The follow-up request is sent through the session rather than from here: on
 * the sync path it recurses back into {@see Session::sendSyncRequest()}, and on
 * the async path it is chained onto the statement the caller is holding, which
 * is what lets a statement outlive the request it was created for.
 *
 * @internal this is not part of the public API and may change at any time
 */
final class ResponseDispatcher {
    /**
     * How many times one request may be reprepared before the driver gives up.
     *
     * An UNPREPARED error is answered by preparing the statement again and
     * re-executing it, and the node may answer that with UNPREPARED as well —
     * a node that never keeps the prepared statement (an unlucky schema change,
     * a cache too small for the workload) will do so every time. Nothing else
     * ends that: the sync path recurses a level deeper each round, and the async
     * path chains a fresh request onto the statement, which restarts its budget
     * so it can never run out of time either. One retry covers the case this
     * exists for — the node forgot the statement once — and a couple more cover
     * a coordinator that changes under us; past that it is a loop, not a retry.
     */
    public const MAX_REPREPARATIONS = 3;

    private ListenerRegistry $listeners;

    private PreparedResultCache $preparedResultCache;

    private Session $session;

    public function __construct(Session $session, ListenerRegistry $listeners, PreparedResultCache $preparedResultCache) {
        $this->session = $session;
        $this->listeners = $listeners;
        $this->preparedResultCache = $preparedResultCache;
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    public function getAutoPrepareRequestIfNeeded(Request\Request $request): ?Request\Prepare {

        // auto-prepare query if bind markers are used and not all values are defined with type
        if (
            ($request instanceof Request\Query)
        ) {

            $queryOptions = $request->getOptions();
            $values = $request->getValues();

            if (
                $queryOptions->autoPrepare
                && $values
            ) {
                $hasUnresolvedValues = false;

                foreach ($values as $v) {
                    if (
                        $v !== null
                        && !($v instanceof ValueBase)
                        && !($v instanceof NotSet)
                    ) {
                        $hasUnresolvedValues = true;

                        break;
                    }
                }

                if ($hasUnresolvedValues) {

                    // The PREPARE inherits the timeout the query asked for, so
                    // that it is bounded like the request it stands in for even
                    // where the caller passed no argument of their own for the
                    // effective timeout to have been resolved from.
                    $prepareOptions = new PrepareOptions(
                        keyspace: $queryOptions->keyspace,
                        requestTimeoutInSeconds: $queryOptions->requestTimeoutInSeconds,
                    );
                    $prepareRequest = new Request\Prepare($request->getQuery(), $prepareOptions);
                    $prepareRequest->setVersion($this->session->getProtocolVersion());

                    // The options were copied off a query the executor has
                    // already addressed, so a keyspace among them is this
                    // connection's rather than the caller's; see
                    // {@see Request\Request::adoptDefaultKeyspaceMarkerFrom()}.
                    $prepareRequest->adoptDefaultKeyspaceMarkerFrom($request);

                    return $prepareRequest;
                }
            }
        }

        return null;
    }

    /**
     * @param int $repreparationDepth see {@see Session::sendSyncRequest()}
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
    public function handleAutoPrepareResult(Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): ?Response\Result {

        if (!($result instanceof Response\Result\PreparedResult)) {
            throw new ConnectionException('Unexpected result type while handling auto-prepared statement', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESULT_TYPE->value, [
                'operation' => 'reprepare_result',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($result),
            ]);
        }

        if ($statement !== null) {
            $originalRequest = $statement->getOriginalRequest();
        }

        if (!($originalRequest instanceof Request\Query)) {
            throw new ConnectionException('Original request is not an query request', ExceptionCode::CONNECTION_AUTO_PREPARE_ORIGINAL_NOT_QUERY->value, [
                'operation' => 'auto_prepare_execute',
                'request_class' => $originalRequest ? get_class($originalRequest) : null,
                'expected' => Request\Query::class,
            ]);
        }

        $newExecuteRequest = new Request\Execute(
            $result,
            $originalRequest->getValues(),
            $originalRequest->getConsistency(),
            ExecuteOptions::fromQueryOptions($originalRequest->getOptions())
        );

        // Built out of the query's options, so a keyspace among them is
        // whoever's it was on that query; see
        // {@see Request\Request::adoptDefaultKeyspaceMarkerFrom()}.
        $newExecuteRequest->adoptDefaultKeyspaceMarkerFrom($originalRequest);

        if ($statement !== null) {
            $this->session->chainAsyncRequest($newExecuteRequest, $statement);

            return null;
        }

        $response = $this->session->sendSyncRequest($newExecuteRequest, $requestTimeoutInSeconds, $repreparationDepth);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during re-execute after auto-preparation', ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE_REEXECUTE->value, [
                'operation' => 'auto_prepare_execute',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @param int $repreparationDepth see {@see Session::sendSyncRequest()}
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
    public function handleResponse(Request\Request $request, Response\Response $response, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): ?Response\Response {

        if ($response->hasWarnings()) {
            $this->listeners->notifyWarnings($response->getWarnings(), $request, $response);
        }

        return match (true) {
            $response instanceof Response\Error => $this->handleResponseError($request, $response, $statement, $requestTimeoutInSeconds, $repreparationDepth),
            $response instanceof Response\Result => $this->handleResponseResult($request, $response, $statement, $requestTimeoutInSeconds, $repreparationDepth),
            default => $response,
        };
    }

    /**
     * Stop repreparing a request the node keeps forgetting.
     *
     * See {@see self::MAX_REPREPARATIONS} for why something has to: neither the
     * request timeout nor anything else bounds the exchange on its own.
     *
     * $limit is that constant for a single prepared statement, and more for a
     * batch, which may legitimately need a round per statement it carries; see
     * {@see self::handleResponseError()}.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function assertRepreparationAllowed(int $repreparationsSoFar, Request\Prepare $prepareRequest, int $limit): void {

        if ($repreparationsSoFar < $limit) {
            return;
        }

        throw new ConnectionException(
            'The node answered a prepared statement with UNPREPARED again after it was reprepared, ' . $limit . ' times over, so the driver stopped repreparing it',
            ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value,
            [
                'operation' => 'unprepared_error_handling',
                'repreparations' => $repreparationsSoFar,
                'max_repreparations' => $limit,
                'query' => $prepareRequest->getQuery(),
            ]
        );
    }

    /**
     * @param int $repreparationDepth see {@see Session::sendSyncRequest()}
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
    private function handleReprepareResult(Response\Result $result, ?Request\Request $originalRequest = null, ?Statement $statement = null, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): ?Response\Result {

        if (!($result instanceof Response\Result\PreparedResult)) {
            throw new ConnectionException('Unexpected result type while handling reprepared statement', ExceptionCode::CONNECTION_REPREPARE_UNEXPECTED_RESULT_TYPE->value, [
                'operation' => 'reprepare_result',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($result),
            ]);
        }

        if ($statement !== null) {
            // The request that was refused, which for an auto-prepared query is
            // not the statement's original request, see
            // {@see \Cassandra\Statement::$requestBeingReprepared}. A batch is
            // never derived from another request that way, so it is its own
            // original and the fallback is what finds it.
            $originalRequest = $statement->getRequestBeingReprepared() ?? $statement->getOriginalRequest();
            $statement->setRequestBeingReprepared(null);
        }

        if ($originalRequest instanceof Request\Batch) {
            return $this->resendBatchAfterRepreparation($result, $originalRequest, $statement, $requestTimeoutInSeconds, $repreparationDepth);
        }

        if (!($originalRequest instanceof Request\Execute)) {
            throw new ConnectionException('Original request is not an execute request', ExceptionCode::CONNECTION_REPREPARE_ORIGINAL_NOT_EXECUTE->value, [
                'operation' => 'reprepare_execute',
                'request_class' => $originalRequest ? get_class($originalRequest) : null,
                'expected' => Request\Execute::class,
            ]);
        }

        // The values as the caller passed them, not the ones the refused EXECUTE
        // encoded. A repreparation is exactly the case where the bind marker
        // types may have moved under them — a schema change is one of the
        // reasons a node stops recognising a statement id — and an encoded value
        // is passed straight through by
        // {@see Request\Request::encodeQueryValuesForBindMarkerTypes()}, so
        // rebuilding from those would send the new statement id with the old
        // statement's encoding. See {@see Request\Execute::$unencodedValues};
        // {@see self::resendBatchAfterRepreparation()} does the same for a batch.
        $newExecuteRequest = new Request\Execute(
            $result,
            $originalRequest->getUnencodedValues(),
            $originalRequest->getConsistency(),
            $originalRequest->getOptions()
        );

        // Built out of the refused EXECUTE's options, so a keyspace among them
        // is whoever's it was on that request; see
        // {@see Request\Request::adoptDefaultKeyspaceMarkerFrom()}.
        $newExecuteRequest->adoptDefaultKeyspaceMarkerFrom($originalRequest);

        if ($statement !== null) {
            $this->session->chainAsyncRequest($newExecuteRequest, $statement);

            return null;
        }

        $response = $this->session->sendSyncRequest($newExecuteRequest, $requestTimeoutInSeconds, $repreparationDepth);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during re-execute after repreparation', ExceptionCode::CONNECTION_REPREPARE_UNEXPECTED_RESPONSE_REEXECUTE->value, [
                'operation' => 'reprepare_execute',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }

    /**
     * @param int $repreparationDepth see {@see Session::sendSyncRequest()}
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    private function handleResponseError(Request\Request $request, Response\Error $response, ?Statement $statement, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): ?Response\Response {

        if (!($response instanceof Response\Error\UnpreparedError)) {
            return $response;
        }

        // Which prepared statement the node has forgotten. An EXECUTE carries
        // exactly one, so it is whatever that request was built from; a BATCH
        // may carry several, and the node names the one it tripped on — which
        // is why the error's own context is what settles it there. Anything
        // else cannot carry a prepared statement at all, so an UNPREPARED for
        // one is not something to recover from and is handed to the caller.
        if ($request instanceof Request\Execute) {
            $previousResult = $request->getPreviousResult();

            // An EXECUTE is built either from the PREPARE's own result or, for
            // the second and later pages of a result set, from the page before
            // it ({@see Request\Execute::__construct()}). A page is not the
            // prepared statement, but it carries the one it was executed
            // against, which is what has to be prepared again; see
            // {@see Response\Result::$lastPreparedResult}.
            $forgottenResult = $previousResult instanceof Response\Result\PreparedResult
                ? $previousResult
                : $previousResult->getLastPreparedResult();

            if ($forgottenResult === null) {
                throw new ConnectionException('Unexpected previous result type for UNPREPARED error', ExceptionCode::CONNECTION_UNPREPARED_UNEXPECTED_PREV_RESULT_TYPE->value, [
                    'operation' => 'unprepared_error_handling',
                    'expected' => Response\Result\PreparedResult::class,
                    'received' => get_class($previousResult),
                ]);
            }

            $repreparationLimit = self::MAX_REPREPARATIONS;

        } elseif ($request instanceof Request\Batch) {
            $unknownStatementId = $response->getContext()->unknownStatementId;

            $forgottenResult = $request->findPreparedStatement($unknownStatementId);
            if ($forgottenResult === null) {
                throw new ConnectionException(
                    'The node reported a prepared statement this batch does not hold, so there is nothing to prepare again',
                    ExceptionCode::CONNECTION_UNPREPARED_BATCH_STATEMENT_NOT_FOUND->value,
                    [
                        'operation' => 'unprepared_error_handling',
                        'unknown_statement_id' => bin2hex($unknownStatementId),
                        'prepared_statements_in_batch' => $request->getDistinctPreparedStatementCount(),
                    ]
                );
            }

            // A node answers UNPREPARED for one statement at a time, so a batch
            // whose statements it has all forgotten — one that was restarted, or
            // whose prepared cache was emptied — needs a round per distinct
            // statement to be recovered. Held to the flat limit it would fail
            // for having done exactly what it was asked to. The flat limit is
            // still added on top, so a batch keeps the same slack an EXECUTE has
            // for a coordinator that changes under it.
            $repreparationLimit = self::MAX_REPREPARATIONS + $request->getDistinctPreparedStatementCount() - 1;

        } else {
            return $response;
        }

        $prevRequest = $forgottenResult->getRequest();
        if ($prevRequest === null) {
            throw new ConnectionException('Previous prepared result has no associated request', ExceptionCode::CONNECTION_UNPREPARED_PREV_NO_REQUEST->value, [
                'operation' => 'unprepared_error_handling',
            ]);
        }
        if (!($prevRequest instanceof Request\Prepare)) {
            throw new ConnectionException('Previous result is not a prepare request', ExceptionCode::CONNECTION_UNPREPARED_PREV_NOT_PREPARE_REQUEST->value, [
                'operation' => 'unprepared_error_handling',
                'request_class' => get_class($prevRequest),
                'expected' => Request\Prepare::class,
            ]);
        }

        $newPrepareRequest = new Request\Prepare($prevRequest->getQuery(), $prevRequest->getOptions());

        // Built out of the options of the PREPARE that produced the
        // statement id the node has just forgotten, so a keyspace among them
        // is whoever's it was on that PREPARE. It matters here more than
        // anywhere: the request it was taken from can have been prepared on
        // another connection, on another protocol version, which is exactly
        // the case {@see Request\Request::clearDefaultKeyspace()} exists for.
        //
        // It is also what lets the answer find its way back to the entry it
        // belongs to without anything being carried across the round trip; see
        // {@see Request\Batch::replacePreparedStatement()}.
        $newPrepareRequest->adoptDefaultKeyspaceMarkerFrom($prevRequest);

        $this->preparedResultCache->invalidate($newPrepareRequest);

        if ($statement !== null) {
            $this->assertRepreparationAllowed($statement->getRepreparationCount(), $newPrepareRequest, $repreparationLimit);
            $statement->recordRepreparation();

            $statement->setStatus(StatementStatus::REPREPARING);

            // Only for an EXECUTE, whose statement may have been created for a
            // QUERY that was auto-prepared into it; a batch is its own original
            // request, so {@see Statement::getOriginalRequest()} already names
            // it and {@see self::handleReprepareResult()} finds it there.
            if ($request instanceof Request\Execute) {
                $statement->setRequestBeingReprepared($request);
            }

            $this->session->chainAsyncRequest($newPrepareRequest, $statement);

            return null;
        }

        $this->assertRepreparationAllowed($repreparationDepth, $newPrepareRequest, $repreparationLimit);

        // Counted one deeper for everything the repreparation sends: the
        // PREPARE below and the request that follows it both belong to this
        // round, and it is the next UNPREPARED among them that has to find
        // the higher count.
        $repreparationDepth++;

        $prepareResponse = $this->session->sendSyncRequest($newPrepareRequest, $requestTimeoutInSeconds, $repreparationDepth);
        if (!($prepareResponse instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during repreparation', ExceptionCode::CONNECTION_REPREPARATION_UNEXPECTED_RESPONSE->value, [
                'operation' => 'unprepared_error_handling',
                'expected' => Response\Result::class,
                'received' => get_class($prepareResponse),
            ]);
        }

        return $this->handleReprepareResult($prepareResponse, originalRequest: $request, requestTimeoutInSeconds: $requestTimeoutInSeconds, repreparationDepth: $repreparationDepth);
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    private function handleResponseExecuteResult(Request\Execute $request, Response\Result $result): Response\Result {

        $result->setPreviousResult($request->getPreviousResult());

        return $result;
    }

    /**
     * @param int $repreparationDepth see {@see Session::sendSyncRequest()}
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
    private function handleResponsePrepareResult(Request\Prepare $request, Response\Result $result, ?Statement $statement, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): ?Response\Result {

        // Never onto a cached result. That object is the cache's entry, shared
        // by every hit, and it already carries the copy of the PREPARE it was
        // stored with — which {@see PreparedResultCache::store()} makes a copy
        // precisely so that the entry cannot follow a caller's request around:
        // a request is addressed on its way to the wire and keeps what it was
        // given, so one that is sent again after
        // {@see \Cassandra\Connection::setKeyspace()} would leave the entry
        // naming a keyspace other than the one it is filed under. Putting the
        // live request back here would undo that, and the repreparation path
        // rebuilds its PREPARE out of exactly this request
        // ({@see self::handleResponseError()}), so an UNPREPARED for the
        // statement id would prepare and execute against the wrong keyspace.
        //
        // Reached because a cache hit on the async path is handled like any
        // other answer ({@see RequestExecutor::sendAsyncRequest()}); the sync
        // path returns the entry without coming through here at all.
        if (!($result instanceof Response\Result\CachedPreparedResult)) {
            $result->setRequest($request);
        }

        if (
            ($result instanceof Response\Result\PreparedResult)
            && !($result instanceof Response\Result\CachedPreparedResult)
        ) {
            $this->preparedResultCache->store($request, $result, $this->session->getProtocolVersion());
        }

        if ($statement !== null) {
            if ($statement->isRepreparing()) {
                $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
                $result = $this->handleReprepareResult($result, statement: $statement, requestTimeoutInSeconds: $requestTimeoutInSeconds, repreparationDepth: $repreparationDepth);
            } elseif ($statement->isAutoPreparing()) {
                $statement->setStatus(StatementStatus::WAITING_FOR_RESULT);
                $result = $this->handleAutoPrepareResult($result, statement: $statement, requestTimeoutInSeconds: $requestTimeoutInSeconds, repreparationDepth: $repreparationDepth);
            }
        }

        return $result;
    }

    /**
     * @param int $repreparationDepth see {@see Session::sendSyncRequest()}
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
    private function handleResponseResult(Request\Request $request, Response\Result $result, ?Statement $statement, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): ?Response\Result {

        return match (true) {
            $request instanceof Request\Prepare => $this->handleResponsePrepareResult($request, $result, $statement, $requestTimeoutInSeconds, $repreparationDepth),
            $request instanceof Request\Execute => $this->handleResponseExecuteResult($request, $result),
            default => $result,
        };
    }

    /**
     * Put a freshly prepared statement into the batch it was prepared for, and
     * send the batch again.
     *
     * The counterpart of what {@see self::handleReprepareResult()} does for an
     * EXECUTE, and different in one way that matters: an EXECUTE is replaced by
     * a new request built around the new statement id, whereas a batch is
     * patched in place and re-sent as itself. It has to be — the other entries
     * of the batch are still the ones the caller appended, and the node may yet
     * report another of them as unprepared, which is the round after this one.
     *
     * @param int $repreparationDepth see {@see Session::sendSyncRequest()}
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
    private function resendBatchAfterRepreparation(Response\Result\PreparedResult $result, Request\Batch $batch, ?Statement $statement, ?float $requestTimeoutInSeconds = null, int $repreparationDepth = 0): ?Response\Result {

        $replaced = $batch->replacePreparedStatement($result);
        if ($replaced === 0) {
            // The PREPARE was built from an entry of this very batch, so its
            // answer always belongs to one; nothing matching means the batch was
            // changed under the repreparation, and re-sending it would send the
            // statement id the node has already refused.
            throw new ConnectionException(
                'The reprepared statement does not belong to any entry of this batch, so the batch cannot be sent again',
                ExceptionCode::CONNECTION_REPREPARE_BATCH_STATEMENT_NOT_REPLACED->value,
                [
                    'operation' => 'reprepare_batch',
                    'statement_id' => bin2hex($result->getPreparedData()->id),
                    'prepared_statements_in_batch' => $batch->getDistinctPreparedStatementCount(),
                ]
            );
        }

        if ($statement !== null) {
            $this->session->chainAsyncRequest($batch, $statement);

            return null;
        }

        $response = $this->session->sendSyncRequest($batch, $requestTimeoutInSeconds, $repreparationDepth);
        if (!($response instanceof Response\Result)) {
            throw new ConnectionException('Unexpected response type during re-send after repreparation', ExceptionCode::CONNECTION_REPREPARE_UNEXPECTED_RESPONSE_REBATCH->value, [
                'operation' => 'reprepare_batch',
                'expected' => Response\Result::class,
                'received' => get_class($response),
            ]);
        }

        return $response;
    }
}
