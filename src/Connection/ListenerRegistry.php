<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\EventListener;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Request\Request;
use Cassandra\Response\Event;
use Cassandra\Response\Response;
use Cassandra\WarningsListener;
use Throwable;

/**
 * The application's callbacks for the two things a connection reports without
 * having been asked: server-pushed events, and the warnings a response carries
 * alongside its result.
 *
 * A listener runs from inside the read that took the frame off the wire, in the
 * middle of the connection resolving whatever that frame belongs to. One that
 * throws must therefore not be allowed to abort that work where it stands: a
 * warnings listener is invoked by {@see ResponseDispatcher::handleResponse()}
 * before the response has been put on its statement, so an exception escaping
 * from there would leave the statement neither pending nor answered — and
 * {@see Session::processResponse()} would give up on it, costing the caller a
 * result the node had in fact delivered, over a fault in their own callback.
 *
 * So a listener that throws is caught here and its failure put aside, and the
 * connection finishes the frame it was in the middle of. Whoever was reading is
 * then told with {@see self::throwDeferred()}, which every path that notifies
 * listeners calls once its own bookkeeping is complete. The failure is reported
 * rather than swallowed — this class has nowhere to log it — but as a
 * ConnectionException wrapping the original, which stays reachable through
 * {@see \Throwable::getPrevious()}.
 *
 * Putting it aside also lets the remaining listeners run: they are separate
 * subscribers to the same notification, and one of them being broken is no
 * reason for the others not to hear about it. Only the first failure of a
 * notification is reported; the ones after it would be reporting the same frame
 * a second time.
 */
final class ListenerRegistry {
    /**
     * The first listener failure of the notification in progress, kept until
     * {@see self::throwDeferred()} reports it.
     */
    private ?ConnectionException $deferredFailure = null;

    /**
     * @var array<EventListener> $eventListeners
     */
    private array $eventListeners = [];

    /**
     * @var array<WarningsListener> $warningsListeners
     */
    private array $warningsListeners = [];

    public function notifyEvent(Event $event): void {

        foreach ($this->eventListeners as $listener) {
            try {
                $listener->onEvent($event);
            } catch (Throwable $e) {
                $this->deferFailure($e, $listener, 'onEvent');
            }
        }
    }

    /**
     * @param array<string> $warnings
     */
    public function notifyWarnings(array $warnings, Request $request, Response $response): void {

        foreach ($this->warningsListeners as $listener) {
            try {
                $listener->onWarnings($warnings, $request, $response);
            } catch (Throwable $e) {
                $this->deferFailure($e, $listener, 'onWarnings');
            }
        }
    }

    public function registerEventListener(EventListener $eventListener): void {

        $this->eventListeners[] = $eventListener;
    }

    public function registerWarningsListener(WarningsListener $warningsListener): void {

        $this->warningsListeners[] = $warningsListener;
    }

    /**
     * Report a listener failure that was put aside, if there was one.
     *
     * Called by every path that notifies listeners, once that path has finished
     * what the frame required of it — the statement resolved, the stream id
     * disposed of — so that raising this can no longer strand anything. Nothing
     * accumulates across calls: a notification and the report of its failure are
     * always in the same pass.
     *
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function throwDeferred(): void {

        $failure = $this->deferredFailure;
        if ($failure === null) {
            return;
        }

        $this->deferredFailure = null;

        throw $failure;
    }

    public function unregisterEventListener(EventListener $eventListener): void {

        $this->eventListeners = array_filter($this->eventListeners, fn (EventListener $listener) => $listener !== $eventListener);
    }

    public function unregisterWarningsListener(WarningsListener $warningsListener): void {

        $this->warningsListeners = array_filter($this->warningsListeners, fn (WarningsListener $listener) => $listener !== $warningsListener);
    }

    /**
     * Keep a listener's failure until {@see self::throwDeferred()} reports it.
     *
     * Only the first of a notification is kept, see the class comment.
     */
    private function deferFailure(Throwable $e, EventListener|WarningsListener $listener, string $method): void {

        $this->deferredFailure ??= new ConnectionException(
            'A listener registered on this connection threw. The response it was being told about was handled before this was raised, so the connection and the request it belongs to are unaffected; see the previous exception for what the listener threw.',
            ExceptionCode::CONNECTION_LISTENER_FAILED->value,
            [
                'operation' => 'notifyListeners',
                'listener_class' => get_class($listener),
                'listener_method' => $method,
            ],
            $e
        );
    }
}
