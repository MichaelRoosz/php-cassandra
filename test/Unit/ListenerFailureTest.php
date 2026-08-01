<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\ListenerRegistry;
use Cassandra\EventListener;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options;
use Cassandra\Response\Event;
use Cassandra\Response\Ready;
use Cassandra\Response\Response;
use Cassandra\Response\StreamReader;
use Cassandra\WarningsListener;
use LogicException;
use RuntimeException;

class ListenerFailureTest extends AbstractUnitTestCase {
    public function testEventListenerFailureIsDeferredAndReported(): void {
        $registry = new ListenerRegistry();
        $registry->registerEventListener(self::throwingEventListener(new RuntimeException('boom')));

        // The notification itself must come back cleanly: it runs from inside
        // the read that took the frame off the wire, and the connection has
        // bookkeeping left to do on that frame.
        $registry->notifyEvent(self::statusChangeEvent());

        try {
            $registry->throwDeferred();
            $this->fail('the listener failure should have been reported');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_LISTENER_FAILED->value, $e->getCode());
            $this->assertInstanceOf(RuntimeException::class, $e->getPrevious());
            $this->assertSame('boom', $e->getPrevious()->getMessage());
        }
    }

    public function testFailureIsReportedOnlyOnce(): void {
        $registry = new ListenerRegistry();
        $registry->registerEventListener(self::throwingEventListener(new RuntimeException('boom')));

        $registry->notifyEvent(self::statusChangeEvent());

        $this->expectNotToPerformAssertions();

        try {
            $registry->throwDeferred();
        } catch (ConnectionException) {
            // Expected; the point of this test is the call below.
        }

        // Nothing may be left over for the next frame to trip on.
        $registry->throwDeferred();
    }

    public function testNothingIsReportedWhenEveryListenerSucceeds(): void {
        $registry = new ListenerRegistry();

        $listener = new class implements EventListener {
            public int $calls = 0;

            #[\Override]
            public function onEvent(Event $event): void {
                $this->calls++;
            }
        };
        $registry->registerEventListener($listener);

        $registry->notifyEvent(self::statusChangeEvent());
        $registry->throwDeferred();

        $this->assertSame(1, $listener->calls);
    }

    public function testOneBrokenListenerDoesNotStarveTheOthers(): void {
        $registry = new ListenerRegistry();

        $survivor = new class implements EventListener {
            public int $calls = 0;

            #[\Override]
            public function onEvent(Event $event): void {
                $this->calls++;
            }
        };

        $registry->registerEventListener(self::throwingEventListener(new RuntimeException('first')));
        $registry->registerEventListener($survivor);

        $registry->notifyEvent(self::statusChangeEvent());

        $this->assertSame(1, $survivor->calls, 'the listener after the broken one still ran');

        // Only the first failure of a notification is reported.
        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_LISTENER_FAILED->value);
        $registry->throwDeferred();
    }

    public function testWarningsListenerFailureIsDeferredAndReported(): void {
        $registry = new ListenerRegistry();
        $registry->registerWarningsListener(new class implements WarningsListener {
            /**
             * @param array<string> $warnings
             */
            #[\Override]
            public function onWarnings(array $warnings, \Cassandra\Request\Request $request, Response $response): void {
                throw new LogicException('warned');
            }
        });

        $response = new Ready(
            new Header(
                version: ProtocolVersion::V4,
                flags: 0,
                stream: 1,
                opcode: Opcode::RESPONSE_READY,
                length: 0,
            ),
            new StreamReader(''),
        );

        $registry->notifyWarnings(['a warning'], new Options(), $response);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_LISTENER_FAILED->value);
        $registry->throwDeferred();
    }

    private static function statusChangeEvent(): Event {
        $body = pack('n', strlen('STATUS_CHANGE')) . 'STATUS_CHANGE'
            . pack('n', strlen('UP')) . 'UP'
            . chr(4) . inet_pton('127.0.0.1')
            . pack('N', 9042);

        return new Event(
            new Header(
                version: ProtocolVersion::V4,
                flags: 0,
                stream: -1,
                opcode: Opcode::RESPONSE_EVENT,
                length: strlen($body),
            ),
            new StreamReader($body),
        );
    }

    private static function throwingEventListener(\Throwable $throwable): EventListener {
        return new class($throwable) implements EventListener {
            public function __construct(private \Throwable $throwable) {
            }

            #[\Override]
            public function onEvent(Event $event): void {
                throw $this->throwable;
            }
        };
    }
}
