<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Exception\CompressionException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Error;
use Cassandra\Response\Event;
use Cassandra\Response\Response;
use Cassandra\Response\Result;
use Cassandra\Response\StreamReader;
use Throwable;
use TypeError;
use ValueError;

final class ResponseReader {
    /**
     * Largest frame body the protocol allows, and so the largest this reader
     * will try to assemble.
     *
     * The length is four bytes on the wire, i.e. up to 4 GiB, while the spec
     * caps a frame body at 256 MB — so a corrupted or hostile length is a
     * number this reader would otherwise spend gigabytes of memory buffering
     * towards, one socket read at a time, before anything went wrong enough to
     * notice. Refused at the header instead, where nothing has been read
     * towards it yet.
     */
    private const MAX_FRAME_BODY_LENGTH = 256 * 1024 * 1024;

    private ?Header $currentHeader;

    /**
     * Header of a frame whose complete body was consumed but whose response
     * could not be constructed.
     *
     * The session needs the stream id to finish the request that owned the
     * malformed answer. Without it an asynchronous statement stays registered
     * forever even though no later answer can arrive for it. Taken and cleared
     * by {@see self::takeFailedResponseHeader()} while the exception unwinds.
     */
    private ?Header $failedResponseHeader = null;

    /**
     * Whether a failure left this reader out of step with the frame stream, so
     * that carrying on would read the next response at the wrong offset.
     *
     * Two things can do that. {@see self::readHeader()} consumes the nine
     * header bytes and only then passes judgement on them, so a header it
     * refuses leaves the body of that frame in the transport's buffer with
     * nothing left that knows how long it is — the next read would take those
     * bytes for a header. And the transport itself can lose bytes it has
     * already taken off the wire, which on the v5 framing is what a refused
     * decompression is; see {@see self::readFromNode()}.
     *
     * Everything else that fails here has already consumed the whole frame
     * ({@see self::createResponse()}, and the v3/v4 decompression above it), so
     * the stream is still in step and the connection is worth keeping. The
     * owning request is finished through
     * {@see Session::finishFailedConsumedResponse()} instead.
     *
     * Told apart because the two need opposite handling, and the exception
     * alone cannot say which is which. {@see Session::readResponse()} and
     * {@see Session::readResponseUntil()} ask this before deciding whether a
     * reader failure costs the connection.
     */
    private bool $frameSyncLost = false;

    private Lz4Decompressor $lz4Decompressor;

    public function __construct() {
        $this->lz4Decompressor = new Lz4Decompressor();
        $this->currentHeader = null;
    }

    /**
     * Whether this reader can no longer be used on its connection, see
     * {@see self::$frameSyncLost}.
     */
    public function hasLostFrameSync(): bool {

        return $this->frameSyncLost;
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\CompressionException
     */
    public function readResponse(Node $node, ProtocolVersion $version, ?float $readDeadline): ?Response {

        $this->failedResponseHeader = null;

        if ($this->currentHeader === null) {
            $this->currentHeader = $this->readHeader($node, $version, $readDeadline);
            if ($this->currentHeader === null) {
                return null;
            }
        }

        $header = $this->currentHeader;

        if ($header->length === 0) {
            $this->currentHeader = null;

            return $this->createConsumedResponse($header, '');
        }

        // The deadline is absolute, so reading the body cannot hand the wait a
        // second full budget on top of the one the header already spent.
        $body = $this->readFromNode($node, $header->length, $readDeadline);
        if ($body === '') {
            return null;
        }

        $this->currentHeader = null;
        $this->failedResponseHeader = $header;

        if (
            $version->value < ProtocolVersion::V5->value
            && $header->length > 0
            && $header->flags & Flag::COMPRESSION
        ) {
            if (strlen($body) < 4) {
                throw new ConnectionException(
                    'Cannot read uncompressed length from compressed frame',
                    ExceptionCode::CONNECTION_CANNOT_READ_DECOMPRESSED_FRAME_LENGTH->value,
                    [
                        'compressed_body_length' => strlen($body),
                        'required_prefix_length' => 4,
                    ]
                );
            }

            /** @var false|array<int> $uncompressedLength */
            $uncompressedLength = unpack('N', substr($body, 0, 4));
            if ($uncompressedLength === false) {
                throw new ConnectionException(
                    'Cannot read uncompressed length from compressed frame',
                    ExceptionCode::CONNECTION_CANNOT_READ_DECOMPRESSED_FRAME_LENGTH->value,
                    []
                );
            }

            if (
                $uncompressedLength[1] < 0
                || $uncompressedLength[1] > self::MAX_FRAME_BODY_LENGTH
            ) {
                throw new ConnectionException(
                    'Decompressed response frame body exceeds the maximum length the protocol allows.',
                    ExceptionCode::CONNECTION_RESPONSE_BODY_TOO_LARGE->value,
                    [
                        'compressed_body_length' => strlen($body),
                        'body_length' => $uncompressedLength[1],
                        'max_body_length' => self::MAX_FRAME_BODY_LENGTH,
                    ]
                );
            }

            $this->lz4Decompressor->setInput(substr($body, 4));
            $body = $this->lz4Decompressor->decompressBlock($uncompressedLength[1]);

            if ($uncompressedLength[1] !== strlen($body)) {
                throw new ConnectionException(
                    'Decompressed frame length does not match expected length',
                    ExceptionCode::CONNECTION_DECOMPRESSED_FRAME_LENGTH_MISMATCH->value,
                    [
                        'expected_length' => $uncompressedLength[1],
                        'actual_length' => strlen($body),
                    ]
                );
            }
        }

        return $this->createConsumedResponse($header, $body);
    }

    /**
     * Forget a partially consumed response.
     *
     * A header whose body has not arrived yet is deliberately kept across calls,
     * so that a bounded read can pick up where it left off. That is only
     * meaningful on the connection the header was read from: the bytes it is
     * still waiting for sit in that transport's receive buffer and go away with
     * it. Carried over to the next connection it would take the first bytes of
     * a fresh stream for the body of a frame nobody is going to send, and every
     * response after that would be read at the wrong offset.
     *
     * Called whenever the connection is dropped, see
     * {@see Session::disconnect()}. Which is also what makes a reader that lost
     * its place usable again: the frames it was out of step with went away with
     * the socket, so the next connection starts from a clean stream.
     */
    public function reset(): void {
        $this->currentHeader = null;
        $this->failedResponseHeader = null;
        $this->frameSyncLost = false;
    }

    /**
     * Return and forget the last fully consumed frame that failed decoding.
     */
    public function takeFailedResponseHeader(): ?Header {
        $header = $this->failedResponseHeader;
        $this->failedResponseHeader = null;

        return $header;
    }

    /**
     * Construct a response after its entire frame has left the transport,
     * retaining the header only if construction fails.
     *
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     */
    private function createConsumedResponse(Header $header, string $body): Response {
        $this->failedResponseHeader = $header;
        $response = $this->createResponse($header, $body);
        $this->failedResponseHeader = null;

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     */
    private function createResponse(Header $header, string $body): Response {

        $responseClassMap = Response::getResponseClassMap();
        if (!isset($responseClassMap[$header->opcode->value])) {
            throw new ConnectionException('Unknown response type: ' . $header->opcode->value, ExceptionCode::CONNECTION_UNKNOWN_RESPONSE_TYPE->value, [
                'expected' => array_keys($responseClassMap),
                'received' => $header->opcode->value,
            ]);
        }

        $streamReader = new StreamReader($body);
        $resetStream = true;

        $responseClass = $responseClassMap[$header->opcode->value];

        switch ($responseClass) {
            case Result::class:
                $result = new Result($header, $streamReader);
                $resultKind = $result->getKind();

                $resultClassMap = Result::getResultClassMap();
                if (isset($resultClassMap[$resultKind->value])) {
                    $responseClass = $resultClassMap[$resultKind->value];
                } else {
                    throw new ConnectionException('Unknown result kind: ' . $resultKind->value, ExceptionCode::CONNECTION_UNKNOWN_RESULT_KIND->value, [
                        'expected' => array_keys($resultClassMap),
                        'received' => $resultKind->value,
                    ]);
                }

                break;

            case Event::class:
                $result = new Event($header, $streamReader);
                $eventType = $result->getType();

                $eventClassMap = Event::getEventClassMap();
                if (isset($eventClassMap[$eventType->value])) {
                    $responseClass = $eventClassMap[$eventType->value];
                } else {
                    throw new ConnectionException('Unknown event type: ' . $eventType->value, ExceptionCode::CONNECTION_UNKNOWN_EVENT_TYPE->value, [
                        'expected' => array_keys($eventClassMap),
                        'received' => $eventType->value,
                    ]);
                }

                break;

            case Error::class:
                $result = new Error($header, $streamReader);
                $errorCode = $result->getCode();

                $errorClassMap = Error::getErrorClassMap();
                if (isset($errorClassMap[$errorCode])) {
                    $responseClass = $errorClassMap[$errorCode];
                } else {
                    throw new ConnectionException('Unknown error code: ' . $errorCode, ExceptionCode::CONNECTION_UNKNOWN_ERROR_CODE->value, [
                        'expected' => array_keys($errorClassMap),
                        'received' => $errorCode,
                    ]);
                }

                break;

            default:
                $resetStream = false;

                break;
        }

        if ($resetStream) {
            $streamReader->extraDataOffset(0);
            $streamReader->offset(0);
        }

        try {
            return new $responseClass($header, $streamReader);
        } catch (ResponseException $e) {
            throw $e;
        } catch (Throwable $e) {
            throw new ResponseException(
                'The response could not be decoded',
                ExceptionCode::RESPONSE_DECODE_FAILED->value,
                [
                    'response_class' => $responseClass,
                    'opcode' => $header->opcode->name,
                    'stream_id' => $header->stream,
                    'protocol_version' => $header->version->inOptionFormat(),
                    'cause_class' => get_class($e),
                ],
                $e,
            );
        }
    }

    /**
     * Take bytes from the transport, treating a decompression failure as a loss
     * of frame sync.
     *
     * Only the v5 framing can raise one from a read, and there it always means
     * bytes that are gone rather than a value that could not be made sense of:
     * {@see FrameCodec} verifies the payload CRC32 and consumes the whole outer
     * frame before it decompresses, so a payload it then refuses was a slice of
     * the envelope stream this reader walks. Carrying on would assemble every
     * later envelope from the wrong bytes — the same predicament a refused
     * header leaves, and the reason both are reported through
     * {@see self::$frameSyncLost}.
     *
     * The v3/v4 decompression is a different matter and is not routed through
     * here: it runs on a body this reader has already taken whole, so a payload
     * refused there costs one response and leaves the stream in step.
     *
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\CompressionException
     */
    private function readFromNode(Node $node, int $length, ?float $readDeadline): string {

        try {
            return $node->read($length, $readDeadline);
        } catch (CompressionException $e) {
            $this->frameSyncLost = true;

            throw $e;
        }
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\CompressionException
     */
    private function readHeader(Node $node, ProtocolVersion $version, ?float $readDeadline): ?Header {

        $headerBytes = $this->readFromNode($node, 9, $readDeadline);
        if ($headerBytes === '') {
            return null;
        }

        // The nine bytes are off the buffer now, and the body they announce is
        // still on it with nothing left that knows how long it is. So every way
        // of failing below leaves this reader out of step with the frame
        // stream; it is cleared again once the header has been made sense of.
        // Marked here rather than at each throw so that a check added later
        // cannot forget to say so. See {@see self::$frameSyncLost}.
        $this->frameSyncLost = true;

        $headerVersion = ord($headerBytes[0]) - 0x80;

        if ($headerVersion !== $version->value) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('Unsupported or mismatched CQL binary protocol version received from server.', ExceptionCode::CONNECTION_PROTOCOL_VERSION_MISMATCH->value, [
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
                'received_protocol_version_as_int' => $headerVersion,
                'expected_protocol_version_as_int' => $version->value,
                'supported_protocol_versions' => ProtocolVersion::CASES_IN_OPTION_FORMAT,
            ]);
        }

        /**
         * @var false|array{
         *  flags: int,
         *  stream: int,
         *  opcode: int,
         *  length: int
         * } $headerData
         */
        $headerData = unpack('Cflags/nstream/Copcode/Nlength', $headerBytes, 1);
        if ($headerData === false) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('Cannot read response header', ExceptionCode::CONNECTION_CANNOT_READ_RESPONSE_HEADER->value, [
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
                'protocol_version' => $version,
            ]);
        }

        // Negative as well as too large: the length is an unsigned 32-bit field,
        // and on 32-bit PHP unpack('N') hands anything past 2 GiB back as a
        // negative int — which would otherwise sail past an upper bound and
        // reach read() as a nonsense length.
        if ($headerData['length'] < 0 || $headerData['length'] > self::MAX_FRAME_BODY_LENGTH) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('Response frame body exceeds the maximum length the protocol allows.', ExceptionCode::CONNECTION_RESPONSE_BODY_TOO_LARGE->value, [
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
                'protocol_version' => $version->inOptionFormat(),
                'body_length' => $headerData['length'],
                'max_body_length' => self::MAX_FRAME_BODY_LENGTH,
            ]);
        }

        // The stream id is a signed [short]; server-initiated responses (events)
        // use -1. unpack('n') gives it to us unsigned, so restore the sign.
        $stream = $headerData['stream'];
        if ($stream > 0x7FFF) {
            $stream -= 0x10000;
        }

        try {
            $header = new Header(
                version: $version,
                flags: $headerData['flags'],
                stream: $stream,
                opcode: Opcode::from($headerData['opcode']),
                length: $headerData['length'],
            );
        } catch (ValueError|TypeError $e) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('Invalid opcode type: ' . $headerData['opcode'], ExceptionCode::CONNECTION_INVALID_OPCODE_TYPE->value, [
                'opcode' => $headerData['opcode'],
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
                'protocol_version' => $version,
            ], $e);
        }

        if ($header->opcode === Opcode::RESPONSE_EVENT && $header->stream !== -1) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException(
                'Server event response must use the reserved stream id -1.',
                ExceptionCode::CONNECTION_EVENT_STREAM_ID_INVALID->value,
                [
                    'stream_id' => $header->stream,
                    'opcode' => $header->opcode->name,
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'protocol_version' => $version->inOptionFormat(),
                ]
            );
        }

        if ($header->opcode !== Opcode::RESPONSE_EVENT && $header->stream < 0) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException(
                'A response to a client request must use a non-negative stream id.',
                ExceptionCode::CONNECTION_RESPONSE_STREAM_ID_INVALID->value,
                [
                    'stream_id' => $header->stream,
                    'opcode' => $header->opcode->name,
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'protocol_version' => $version->inOptionFormat(),
                ]
            );
        }

        $this->frameSyncLost = false;

        return $header;
    }
}
