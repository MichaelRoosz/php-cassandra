<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Connection\Node;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;
use Cassandra\TypeNameParser;
use Cassandra\VIntCodec;

final class ProgressiveStreamReader extends StreamReader {
    protected ?Node $source = null;

    public function __construct(string $data = '') {
        $this->data = $data;
        $this->dataLength = strlen($data);
        $this->typeNameParser = new TypeNameParser();
        $this->vIntCodec = new VIntCodec();
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public function getData(bool $includeExtraData = false): string {
        throw new ResponseException(
            message: 'ProgressiveStreamReader does not support random access via getData()',
            code: ExceptionCode::RESPONSE_PSR_GET_DATA_NOT_SUPPORTED->value,
            context: [
                'method' => __METHOD__,
            ]
        );
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\NodeException
     */
    #[\Override]
    public function read(int $length): string {
        if ($length < 1) {
            return '';
        }

        if ($this->source === null) {
            throw new ResponseException(
                message: 'Source not set',
                code: ExceptionCode::RESPONSE_PSR_SOURCE_NOT_SET->value,
                context: [
                    'method' => __METHOD__,
                    'requested_length' => $this->offset + $length,
                    'available' => $this->dataLength,
                ]
            );
        }

        while ($this->dataLength < $this->offset + $length) {
            $this->data .= $received = $this->source->readOnce($this->offset + $length - $this->dataLength);
            $this->dataLength += strlen($received);
        }

        $output = substr($this->data, $this->offset, $length);
        $this->offset += $length;

        return $output;
    }

    public function setSource(Node $source): void {
        $this->source = $source;
    }
}
