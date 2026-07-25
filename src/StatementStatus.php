<?php

declare(strict_types=1);

namespace Cassandra;

/**
 * Where an asynchronous statement stands.
 *
 * A statement normally goes CREATED → WAITING_FOR_RESULT → RESULT_READY, with a
 * detour through AUTO_PREPARING or REPREPARING when the driver has to prepare
 * the query first. TIMED_OUT and ABANDONED are the two dead ends: the statement
 * will never be answered and the request has to be sent again.
 */
enum StatementStatus: int {
    /**
     * The stream id this statement was waiting on no longer refers to it, so it
     * can never be resolved: either the connection carrying it was closed
     * before the answer arrived, or a follow-up request it needed (a
     * repreparation, an auto-prepared execute) failed to reach the node.
     */
    case ABANDONED = 800;

    /**
     * The query had to be prepared first, so a PREPARE was sent in this
     * statement's place; the original request follows once it is answered.
     */
    case AUTO_PREPARING = 200;

    /**
     * The initial state, before the connection has recorded what the statement
     * is waiting for.
     */
    case CREATED = 0;

    /**
     * The server no longer knows the prepared statement — after a schema
     * change, say — so it is being prepared again before the request is retried.
     */
    case REPREPARING = 300;

    /**
     * The answer has arrived and can be read from the statement.
     */
    case RESULT_READY = 700;

    /**
     * The client gave up waiting for this statement's answer. The connection
     * itself is untouched and its other statements are unaffected; only this
     * one is finished, and its stream id is held back until the late answer
     * arrives (or the connection closes) so that it cannot be handed to
     * another request in the meantime.
     */
    case TIMED_OUT = 900;

    /**
     * The request has been sent and the server's answer is outstanding.
     */
    case WAITING_FOR_RESULT = 100;
}
