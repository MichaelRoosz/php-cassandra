<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\Node;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\ResponseReader;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Request;

/**
 * How much of a frame the reader is willing to assemble on a node's say-so.
 *
 * The body length is four bytes on the wire, so a header can announce up to
 * 4 GiB, while the protocol caps a frame body at 256 MB. Taken at face value a
 * corrupted — or hostile — length is a number the transport would buffer
 * towards one socket read at a time, spending gigabytes of memory before
 * anything went wrong enough to be noticed. It is refused at the header
 * instead, where nothing has been read towards it yet.
 */
final class ResponseFrameLimitTest extends AbstractUnitTestCase {
    public function testABodyLengthPastTheProtocolMaximumIsRefused(): void {
        $reader = new ResponseReader();

        // 256 MB + 1, the first length the protocol does not allow.
        $node = new FakeFrameNode(self::header(bodyLength: 256 * 1024 * 1024 + 1));

        try {
            $reader->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT);
            $this->fail('expected an oversized frame body to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_RESPONSE_BODY_TOO_LARGE->value, $e->getCode());
        }

        $this->assertSame(9, $node->bytesRead, 'only the header was read, never a byte of the body it announced');
    }

    public function testABodyLengthThatWentNegativeIsRefused(): void {
        // On 32-bit PHP unpack('N') hands anything past 2 GiB back as a negative
        // int, which would otherwise sail past an upper bound and reach read()
        // as a nonsense length. The frame is built with the top bit set, so this
        // is the same frame either way — a 64-bit build reads it as far too
        // large, a 32-bit one as negative, and both refuse it.
        $reader = new ResponseReader();

        $node = new FakeFrameNode("\x84\x00\x00\x07" . chr(Opcode::RESPONSE_RESULT->value) . "\xff\xff\xff\xff");

        try {
            $reader->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT);
            $this->fail('expected a nonsense frame body length to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_RESPONSE_BODY_TOO_LARGE->value, $e->getCode());
        }
    }

    public function testCompressedBodyCannotDeclareAnOversizedDecompressedLength(): void {
        $body = pack('N', 256 * 1024 * 1024 + 1);
        $header = "\x84" . chr(Flag::COMPRESSION) . "\x00\x07"
            . chr(Opcode::RESPONSE_RESULT->value) . pack('N', strlen($body));
        $node = new FakeFrameNode($header . $body);

        try {
            (new ResponseReader())->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT);
            $this->fail('expected an oversized decompressed frame body to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_RESPONSE_BODY_TOO_LARGE->value, $e->getCode());
            $this->assertSame(256 * 1024 * 1024 + 1, $e->getContext()['body_length'] ?? null);
        }

        $this->assertSame(13, $node->bytesRead, 'only the header and four-byte length prefix were buffered');
    }
    public function testCompressedBodyShorterThanLengthPrefixIsRefusedWithoutNativeWarning(): void {
        $header = "\x84" . chr(Flag::COMPRESSION) . "\x00\x07"
            . chr(Opcode::RESPONSE_RESULT->value) . pack('N', 1);
        $node = new FakeFrameNode($header . "\x00");

        set_error_handler(static function (int $severity, string $message): never {
            throw new \ErrorException($message, 0, $severity);
        });

        try {
            (new ResponseReader())->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT);
            $this->fail('expected a truncated compressed-frame prefix to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_CANNOT_READ_DECOMPRESSED_FRAME_LENGTH->value, $e->getCode());
        } finally {
            restore_error_handler();
        }
    }
    public function testEventFrameMustUseTheReservedNegativeStream(): void {
        $node = new FakeFrameNode(pack(
            'CCnCN',
            0x84,
            0,
            7,
            Opcode::RESPONSE_EVENT->value,
            0,
        ));

        try {
            (new ResponseReader())->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT);
            $this->fail('expected an event on a client request stream to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_EVENT_STREAM_ID_INVALID->value, $e->getCode());
        }
    }

    public function testOrdinaryResponseCannotUseANegativeStream(): void {
        $node = new FakeFrameNode(pack(
            'CCnCN',
            0x84,
            0,
            0xFFFF,
            Opcode::RESPONSE_RESULT->value,
            0,
        ));

        try {
            (new ResponseReader())->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT);
            $this->fail('expected a request response on a negative stream to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_RESPONSE_STREAM_ID_INVALID->value, $e->getCode());
        }
    }

    public function testTheLargestAllowedBodyIsStillAccepted(): void {
        // The cap is the protocol's, not one of the reader's own, so a frame
        // right at it is read like any other — it simply has no body to supply
        // here, which leaves the reader waiting for one rather than raising.
        $reader = new ResponseReader();

        $node = new FakeFrameNode(self::header(bodyLength: 256 * 1024 * 1024));

        $this->assertNull(
            $reader->readResponse($node, ProtocolVersion::V4, Node::DO_NOT_WAIT),
            'the header is accepted and the body is simply not there yet'
        );
    }

    private static function header(int $bodyLength): string {
        return "\x84\x00\x00\x07" . chr(Opcode::RESPONSE_RESULT->value) . pack('N', $bodyLength);
    }
}

/**
 * A node that replays a fixed buffer, returning an empty string rather than a
 * short read, and counts what was actually taken off it.
 */
final class FakeFrameNode implements Node {
    public int $bytesRead = 0;

    public function __construct(private string $buffer) {
    }

    public function close(): void {
    }

    public function getConfig(): NodeConfig {
        return new SocketNodeConfig(host: '127.0.0.1');
    }

    public function getReceivedByteCount(): int {
        return $this->bytesRead;
    }

    public function read(int $length, ?float $readDeadline): string {
        if (strlen($this->buffer) < $length) {
            return '';
        }

        $data = substr($this->buffer, 0, $length);
        $this->buffer = substr($this->buffer, $length);
        $this->bytesRead += $length;

        return $data;
    }

    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        return $this->read($expectedLength, $readDeadline);
    }

    public function write(string $data): void {
    }

    public function writeRequest(Request $request): void {
    }
}
