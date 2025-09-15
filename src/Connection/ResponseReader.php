<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\ReleaseConstants;
use Cassandra\Response\Error;
use Cassandra\Response\Event;
use Cassandra\Response\Response;
use Cassandra\Response\Result;
use Cassandra\Response\StreamReader;
use TypeError;
use ValueError;

final class ResponseReader {
    protected ?Header $currentHeader;
    protected Lz4Decompressor $lz4Decompressor;

    public function __construct() {
        $this->lz4Decompressor = new Lz4Decompressor();
        $this->currentHeader = null;
    }

    public function readResponse(Node $node, int $version, bool $waitForResponse): ?Response {

        if ($this->currentHeader === null) {
            $this->currentHeader = $this->readHeader($node, $version, $waitForResponse);
            if ($this->currentHeader === null) {
                return null;
            }
        }

        $header = $this->currentHeader;

        if ($header->length === 0) {
            $this->currentHeader = null;

            return $this->createResponse($header, '');
        }

        $body = $node->read($header->length, $waitForResponse);
        if ($body === '') {
            return null;
        }

        $this->currentHeader = null;

        if ($version < 5 && $header->length > 0 && $header->flags & Flag::COMPRESSION) {
            $this->lz4Decompressor->setInput($body);
            $body = $this->lz4Decompressor->decompressBlock();
        }

        return $this->createResponse($header, $body);
    }

    /**
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     */
    protected function createResponse(Header $header, string $body): Response {

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

    protected function readHeader(Node $node, int $version, bool $waitForResponse): ?Header {

        $headerBytes = $node->read(9, $waitForResponse);
        if ($headerBytes === '') {
            return null;
        }

        $headerVersion = ord($headerBytes[0]);
        $versionIn = $version + 0x80;

        if ($headerVersion !== $versionIn) {
            $nodeConfig = $node->getConfig();

            throw new ConnectionException('Unsupported or mismatched CQL binary protocol version received from server.', ExceptionCode::CONNECTION_PROTOCOL_VERSION_MISMATCH->value, [
                'host' => $nodeConfig->host,
                'port' => $nodeConfig->port,
                'received_version' => $headerVersion,
                'expected_version' => $versionIn,
                'supported_versions' => ReleaseConstants::PHP_CASSANDRA_SUPPORTED_PROTOCOL_VERSIONS,
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

        try {
            $header = new Header(
                version: $version,
                flags: $headerData['flags'],
                stream: $headerData['stream'],
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
