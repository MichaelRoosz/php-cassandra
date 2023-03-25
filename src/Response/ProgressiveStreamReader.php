<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Connection\Node;

class ProgressiveStreamReader extends StreamReader {
    protected ?Node $source = null;

    public function __construct(string $data = '') {
        $this->data = $data;
        $this->dataLength = strlen($data);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getData(): string {
        throw new Exception('not supported');
    }

    public function setSource(Node $source): void {
        $this->source = $source;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function read(int $length): string {
        if ($length < 1) {
            return '';
        }

        while ($this->dataLength < $this->offset + $length) {
            if ($this->source === null) {
                throw new Exception('The response is incomplete, or types expectation mismatch.');
            }

            $this->data .= $received = $this->source->readOnce($this->offset + $length - $this->dataLength);
            $this->dataLength += strlen($received);
        }

        $output = substr($this->data, $this->offset, $length);
        $this->offset += $length;

        return $output;
    }
}
