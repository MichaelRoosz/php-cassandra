<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Error;
use Cassandra\Response\Event;
use Cassandra\Response\Response;
use Cassandra\Response\Result;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class ResponseReader {
    private ?Header $currentHeader;
    private Lz4Decompressor $lz4Decompressor;

    public function __construct() {
        $this->lz4Decompressor = new Lz4Decompressor();
        $this->currentHeader = null;
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\CompressionException
     */
    public function readResponse(Node $node, ProtocolVersion $version, ?float $readDeadline): ?Response {

        if ($this->currentHeader === null) {
            $this->currentHeader = $this->readHeader($node, $version, $readDeadline);
            if ($this->currentHeader === null) {
                return null;
            }
        }

        $header = $this->currentHeader;

        if ($header->length === 0) {
            $this->currentHeader = null;

            return $this->createResponse($header, '');
        }

        // The deadline is absolute, so reading the body cannot hand the wait a
        // second full budget on top of the one the header already spent.
        $body = $node->read($header->length, $readDeadline);
        if ($body === '') {
            return null;
        }

        $this->currentHeader = null;

        if (
            $version->value < ProtocolVersion::V5->value
            && $header->length > 0
            && $header->flags & Flag::COMPRESSION
        ) {
            /** @var false|array<int> $uncompressedLength */
            $uncompressedLength = unpack('N', substr($body, 0, 4));
            if ($uncompressedLength === false) {
                throw new ConnectionException(
                    'Cannot read uncompressed length from compressed frame',
                    ExceptionCode::CONNECTION_CANNOT_READ_DECOMPRESSED_FRAME_LENGTH->value,
                    []
                );
            }

            $this->lz4Decompressor->setInput(substr($body, 4));
            $body = $this->lz4Decompressor->decompressBlock($uncompressedLength[1]);

            if ($uncompressedLength[1] !== strlen($body)) {
                throw new ConnectionException(
                    'Decompressed frame length does not match expected length',
                    ExceptionCode::CONNECTION_DECOMPRESSED_FRAME_LENGTH_MISMATCH->value,
                    [
                        'expected_length' => $uncompressedLength,
                        'actual_length' => strlen($body),
                    ]
                );
            }
        }

        return $this->createResponse($header, $body);
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
     * {@see Session::disconnect()}.
     */
    public function reset(): void {
        $this->currentHeader = null;
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

        return new $responseClass($header, $streamReader);
    }

    /**
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\ConnectionException
     */
    private function readHeader(Node $node, ProtocolVersion $version, ?float $readDeadline): ?Header {

        $headerBytes = $node->read(9, $readDeadline);
        if ($headerBytes === '') {
            return null;
        }

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

        return $header;
    }
}
