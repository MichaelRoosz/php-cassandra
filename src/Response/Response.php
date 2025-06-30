<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Protocol\Frame;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Opcode;
use Cassandra\Response\StreamReader;
use Stringable;
use TypeError;
use ValueError;

abstract class Response implements Frame, Stringable {
    /**
     * @var array{
     *  version: int,
     *  flags: int,
     *  stream: int,
     *  opcode: int,
     * } $header
     */
    protected array $header;

    /**
     * @var ?array<string,?string> $payload
     */
    protected ?array $payload = null;

    protected StreamReader $stream;

    protected ?string $tracingUuid = null;

    /**
     * @var ?array<string> $warnings
     */
    protected ?array $warnings = null;

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
    final public function __construct(array $header, StreamReader $stream) {
        $this->header = $header;

        $this->stream = $stream;

        $this->readExtraData();
    }

    #[\Override]
    public function __toString(): string {
        $body = $this->getBody();

        return pack(
            'CCnCN',
            $this->header['version'],
            $this->header['flags'],
            $this->header['stream'],
            $this->header['opcode'],
            strlen($body)
        ) . $body;
    }

    #[\Override]
    public function getBody(): string {
        return $this->stream->getData();
    }

    public function getBodyStreamReader(): StreamReader {
        return $this->stream;
    }

    #[\Override]
    public function getFlags(): int {
        return $this->header['flags'];
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public function getOpcode(): Opcode {

        $opcodeInt = $this->header['opcode'];

        try {
            return Opcode::from($opcodeInt);
        } catch (ValueError|TypeError $e) {
            throw new Exception('Invalid opcode: ' . $opcodeInt, 0, [
                'opcode' => $opcodeInt,
            ]);
        }
    }

    /**
     * @return ?array<string,?string>
     */
    public function getPayload(): ?array {
        return $this->payload;
    }

    #[\Override]
    public function getStream(): int {
        return $this->header['stream'];
    }

    public function getTracingUuid(): ?string {
        return $this->tracingUuid;
    }

    #[\Override]
    public function getVersion(): int {
        return $this->header['version'];
    }

    /**
     * @return ?array<string>
     */
    public function getWarnings(): ?array {
        return $this->warnings;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    protected function readExtraData(): void {
        $flags = $this->header['flags'];

        if ($flags & Flag::TRACING->value) {
            $this->tracingUuid = $this->stream->readUuid();
        }

        if ($flags & Flag::WARNING->value) {
            $this->warnings = $this->stream->readStringList();
        }

        if ($flags & Flag::CUSTOM_PAYLOAD->value) {
            $this->payload = $this->stream->readBytesMap();
        }

        $this->stream->extraDataOffset($this->stream->pos());
    }
}
