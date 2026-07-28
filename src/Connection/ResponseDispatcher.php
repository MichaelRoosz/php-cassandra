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
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function assertRepreparationAllowed(int $repreparationsSoFar, Request\Prepare $prepareRequest): void {

        if ($repreparationsSoFar < self::MAX_REPREPARATIONS) {
            return;
        }

        throw new ConnectionException(
            'The node answered this prepared statement with UNPREPARED again after it was reprepared, ' . self::MAX_REPREPARATIONS . ' times over, so the driver stopped repreparing it',
            ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value,
            [
                'operation' => 'unprepared_error_handling',
                'repreparations' => $repreparationsSoFar,
                'max_repreparations' => self::MAX_REPREPARATIONS,
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
            // The EXECUTE that was refused, which for an auto-prepared query is
            // not the statement's original request, see
            // {@see \Cassandra\Statement::$requestBeingReprepared}.
            $originalRequest = $statement->getRequestBeingReprepared() ?? $statement->getOriginalRequest();
            $statement->setRequestBeingReprepared(null);
        }

        if (!($originalRequest instanceof Request\Execute)) {
            throw new ConnectionException('Original request is not an execute request', ExceptionCode::CONNECTION_REPREPARE_ORIGINAL_NOT_EXECUTE->value, [
                'operation' => 'reprepare_execute',
                'request_class' => $originalRequest ? get_class($originalRequest) : null,
                'expected' => Request\Execute::class,
            ]);
        }

        $newExecuteRequest = new Request\Execute(
            $result,
            $originalRequest->getValues(),
            $originalRequest->getConsistency(),
            $originalRequest->getOptions()
        );

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

        // re-prepare query if it is unprepared
        if (
            ($request instanceof Request\Execute)
            && ($response instanceof Response\Error\UnpreparedError)
        ) {

            $prevResult = $request->getPreviousResult();
            if (!($prevResult instanceof Response\Result\PreparedResult)) {
                throw new ConnectionException('Unexpected previous result type for UNPREPARED error', ExceptionCode::CONNECTION_UNPREPARED_UNEXPECTED_PREV_RESULT_TYPE->value, [
                    'operation' => 'unprepared_error_handling',
                    'expected' => Response\Result\PreparedResult::class,
                    'received' => get_class($prevResult),
                ]);
            }

            $prevRequest = $prevResult->getRequest();
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

            $this->preparedResultCache->invalidate($newPrepareRequest);

            if ($statement !== null) {
                $this->assertRepreparationAllowed($statement->getRepreparationCount(), $newPrepareRequest);
                $statement->recordRepreparation();

                $statement->setStatus(StatementStatus::REPREPARING);
                $statement->setRequestBeingReprepared($request);

                $this->session->chainAsyncRequest($newPrepareRequest, $statement);

                return null;
            }

            $this->assertRepreparationAllowed($repreparationDepth, $newPrepareRequest);

            // Counted one deeper for everything the repreparation sends: the
            // PREPARE below and the EXECUTE that follows it both belong to this
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

            $response = $this->handleReprepareResult($prepareResponse, originalRequest: $request, requestTimeoutInSeconds: $requestTimeoutInSeconds, repreparationDepth: $repreparationDepth);
        }

        return $response;
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

        $result->setRequest($request);

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
}
