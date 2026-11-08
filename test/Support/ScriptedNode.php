<?php

declare(strict_types=1);

namespace Cassandra\Test\Support;

use Cassandra\Connection\IoNode;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StreamException;
use Cassandra\Protocol\Opcode;
use Cassandra\Request\Request;
use Closure;

/**
 * A protocol v4 node in memory, answering each request with whatever the test's
 * responder returns. It answers OPTIONS and STARTUP itself unless told otherwise,
 * so a test only scripts the requests it cares about.
 */
final class ScriptedNode extends NodeImplementation implements IoNode {
    /**
     * Raised by the next read or write instead of performing it, then cleared.
     */
    public static ?\Cassandra\Exception\NodeException $failRead = null;

    public static ?\Cassandra\Exception\NodeException $failWrite = null;
    /**
     * Requests the node received, in order.
     *
     * @var list<Request>
     */
    public static array $received = [];

    /**
     * Returns [opcode, body] to answer with, a raw frame string, or null for no answer.
     *
     * @var ?Closure(Request): (null|string|array{Opcode, string})
     */
    public static ?Closure $responder = null;

    private string $buffer = '';

    public function __construct(private readonly NodeConfig $config) {
    }

    #[\Override]
    public function close(): void {
    }

    #[\Override]
    public function connect(): void {
    }

    public static function frame(Opcode $opcode, string $body, int $stream, int $flags = 0): string {
        return "\x84" . chr($flags & 0xFF) . pack('n', $stream & 0xFFFF) . chr($opcode->value) . pack('N', strlen($body)) . $body;
    }

    #[\Override]
    public function getConfig(): NodeConfig {
        return $this->config;
    }

    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        if (self::$failRead !== null) {
            $failure = self::$failRead;
            self::$failRead = null;

            throw $failure;
        }

        if ($this->buffer === '') {
            if (!$this->mayBlock($readDeadline)) {
                return '';
            }

            throw new StreamException('Scripted node has nothing more to say', ExceptionCode::STREAM_TIMEOUT_DURING_READ->value, ['read_timeout' => true]);
        }

        $chunk = substr($this->buffer, 0, $upperBoundaryLength);
        $this->buffer = substr($this->buffer, strlen($chunk));

        return $chunk;
    }

    public static function reset(?Closure $responder = null): void {
        self::$received = [];
        self::$responder = $responder;
        self::$failRead = null;
        self::$failWrite = null;
    }

    public static function string(string $value): string {
        return pack('n', strlen($value)) . $value;
    }

    #[\Override]
    public function write(string $data): void {
    }

    #[\Override]
    public function writeRequest(Request $request): void {
        if (self::$failWrite !== null) {
            $failure = self::$failWrite;
            self::$failWrite = null;

            throw $failure;
        }

        self::$received[] = $request;
        $stream = (int) $request->getStream();

        $answer = self::$responder !== null ? (self::$responder)($request) : null;
        $answer ??= match ($request->getOpcode()) {
            Opcode::REQUEST_OPTIONS => [Opcode::RESPONSE_SUPPORTED, pack('n', 0)],
            Opcode::REQUEST_STARTUP => [Opcode::RESPONSE_READY, ''],
            default => null,
        };

        if (is_string($answer)) {
            $this->buffer .= $answer;
        } elseif (is_array($answer)) {
            $this->buffer .= self::frame($answer[0], $answer[1], $stream);
        }
    }
}
