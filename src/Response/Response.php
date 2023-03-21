<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Protocol\Frame;
use Cassandra\Response\StreamReader;
use Stringable;

abstract class Response implements Frame, Stringable
{
    /**
     * @var array{
     *  version: int,
     *  flags: int,
     *  stream: int,
     *  opcode: int,
     * } $_header
     */
    protected array $_header;

    protected ?string $_tracingUuid = null;

    /**
     * @var ?array<string> $_warnings
     */
    protected ?array $_warnings = null;

    /**
     * @var ?array<string,?string> $_payload
     */
    protected ?array $_payload = null;

    protected StreamReader $_stream;

    /**
     * @param array{
     *  version: int,
     *  flags: int,
     *  stream: int,
     *  opcode: int,
     * } $header
     *
     * @throws \Cassandra\Response\Exception
     */
    final public function __construct(array $header, StreamReader $stream)
    {
        $this->_header = $header;

        $this->_stream = $stream;

        $this->readExtraData();
    }

    public function getVersion(): int
    {
        return $this->_header['version'];
    }

    public function getFlags(): int
    {
        return $this->_header['flags'];
    }

    public function getStream(): int
    {
        return $this->_header['stream'];
    }

    public function getOpcode(): int
    {
        return $this->_header['opcode'];
    }

    public function getBody(): string
    {
        return $this->_stream->getData();
    }

    public function getBodyStreamReader(): StreamReader
    {
        return $this->_stream;
    }

    public function getTracingUuid(): ?string
    {
        return $this->_tracingUuid;
    }

    /**
     * @return ?array<string>
     */
    public function getWarnings(): ?array
    {
        return $this->_warnings;
    }

    /**
     * @return ?array<string,?string>
     */
    public function getPayload(): ?array
    {
        return $this->_payload;
    }

    public function __toString(): string
    {
        $body = $this->getBody();
        return pack(
            'CCnCN',
            $this->_header['version'],
            $this->_header['flags'],
            $this->_header['stream'],
            $this->_header['opcode'],
            strlen($body)
        ) . $body;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readExtraData(): void
    {
        $flags = $this->_header['flags'];

        if ($flags & self::FLAG_TRACING) {
            $this->_tracingUuid = $this->_stream->readUuid();
        }

        if ($flags & self::FLAG_WARNING) {
            $this->_warnings = $this->_stream->readStringList();
        }

        if ($flags & self::FLAG_CUSTOM_PAYLOAD) {
            $this->_payload = $this->_stream->readBytesMap();
        }

        $this->_stream->extraDataOffset($this->_stream->pos());
    }
}
