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
 *
 * What is put aside belongs to the pass that put it there, which is what
 * {@see self::beginPass()} and {@see self::endPass()} enforce — see
 * {@see self::$deferredFailure}.
 */
final class ListenerRegistry {
    /**
     * The first listener failure of the pass in progress, kept until
     * {@see self::throwDeferred()} reports it.
     *
     * Scoped to a pass rather than simply left here until something asks,
     * because a pass does not always get to ask. Every path that notifies
     * listeners calls throwDeferred() once its own bookkeeping is complete, but
     * that call is only reached if the bookkeeping got there: handling the very
     * frame whose listeners ran can fail on its own — the repreparation limit
     * above all, see {@see ResponseDispatcher::MAX_REPREPARATIONS} — and then
     * the pass unwinds with an exception of its own and never reports what it
     * deferred. Left standing, that failure would be thrown by the next pass
     * that does reach throwDeferred(), telling a caller whose request had
     * nothing to do with it that a listener threw.
     *
     * So a pass takes custody of the field for its duration and gives it back
     * as it leaves ({@see self::beginPass()}, {@see self::endPass()}), which
     * settles both halves of it: a pass that unwinds abnormally discards only
     * what it deferred itself — its caller is already being told about a real
     * failure — and a pass nested inside another (a listener issuing a request
     * of its own, a repreparation reading its own answer) can neither see nor
     * clear the failure the outer pass is still going to report.
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

    /**
     * Begin a pass that may notify listeners, taking custody of what is put
     * aside for its duration; see {@see self::$deferredFailure}.
     *
     * The return value is the outer pass's own deferred failure, which the
     * caller hands back to {@see self::endPass()} and is not otherwise to be
     * looked at.
     */
    public function beginPass(): ?ConnectionException {

        $outerFailure = $this->deferredFailure;
        $this->deferredFailure = null;

        return $outerFailure;
    }

    /**
     * End the pass begun by {@see self::beginPass()}, discarding anything it
     * deferred and never reported, and giving the outer pass its own back.
     *
     * Must run however the pass ends, so it belongs in a finally. Where the
     * pass reported its failure the discard is a no-op — {@see self::throwDeferred()}
     * clears the field before it throws.
     */
    public function endPass(?ConnectionException $outerFailure): void {

        $this->deferredFailure = $outerFailure;
    }

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
     * accumulates across calls: what is reported here was deferred by the pass
     * that is asking, which {@see self::beginPass()} and {@see self::endPass()}
     * are what make true.
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
