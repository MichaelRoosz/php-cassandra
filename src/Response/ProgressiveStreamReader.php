<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Connection\Node;

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
    public function getData(): string {
        throw new Exception(
            message: 'ProgressiveStreamReader does not support random access via getData()',
            code: Exception::PSR_GET_DATA_NOT_SUPPORTED,
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

        while ($this->dataLength < $this->offset + $length) {
            if ($this->source === null) {
                throw new Exception(
                    message: 'The response is incomplete, or types expectation mismatch.',
                    code: Exception::PSR_INCOMPLETE_RESPONSE,
                    context: [
                        'method' => __METHOD__,
                        'requested_length' => $this->offset + $length,
                        'available' => $this->dataLength,
                    ]
                );
            }

            $this->data .= $received = $this->source->readOnce($this->offset + $length - $this->dataLength);
            $this->dataLength += strlen($received);
        }

        $output = substr($this->data, $this->offset, $length);
        $this->offset += $length;

        return $output;
    }
}
