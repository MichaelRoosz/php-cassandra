<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Connection\Node;
use Cassandra\ExceptionCode;

final class ProgressiveStreamReader extends StreamReader {
    protected ?Node $source = null;

    public function __construct(string $data = '') {
        $this->data = $data;
        $this->dataLength = strlen($data);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public function getData(bool $includeExtraData = false): string {
        throw new Exception(
            message: 'ProgressiveStreamReader does not support random access via getData()',
            code: ExceptionCode::RESPONSE_PSR_GET_DATA_NOT_SUPPORTED->value,
            context: [
                'method' => __METHOD__,
            ]
        );
    }

    public function setSource(Node $source): void {
        $this->source = $source;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    protected function read(int $length): string {
        if ($length < 1) {
            return '';
        }

        if ($this->source === null) {
            throw new Exception(
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
}
