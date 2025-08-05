<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Protocol\Frame;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Response\StreamReader;
use Stringable;

/**
 * @psalm-consistent-constructor
 */
abstract class Response implements Frame, Stringable {
    public const RESPONSE_CLASS_MAP = [
        Opcode::RESPONSE_ERROR->value => Error::class,
        Opcode::RESPONSE_READY->value => Ready::class,
        Opcode::RESPONSE_AUTHENTICATE->value => Authenticate::class,
        Opcode::RESPONSE_SUPPORTED->value => Supported::class,
        Opcode::RESPONSE_RESULT->value => Result::class,
        Opcode::RESPONSE_EVENT->value => Event::class,
        Opcode::RESPONSE_AUTH_CHALLENGE->value => AuthChallenge::class,
        Opcode::RESPONSE_AUTH_SUCCESS->value => AuthSuccess::class,
    ];

    /**
     * @var ?array<string,?string> $payload
     */
    protected ?array $payload = null;

    protected ?string $tracingUuid = null;

    /**
     * @var ?array<string> $warnings
     */
    protected ?array $warnings = null;

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function __construct(
        protected Header $header,
        protected StreamReader $stream,
    ) {
        $this->readExtraData();
    }

    #[\Override]
    public function __toString(): string {
        $body = $this->getBody();

        return pack(
            'CCnCN',
            $this->header->version,
            $this->header->flags,
            $this->header->stream,
            $this->header->opcode,
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
        return $this->header->flags;
    }

    #[\Override]
    public function getOpcode(): Opcode {

        return $this->header->opcode;
    }

    /**
     * @return ?array<string,?string>
     */
    public function getPayload(): ?array {
        return $this->payload;
    }

    #[\Override]
    public function getStream(): int {
        return $this->header->stream;
    }

    public function getTracingUuid(): ?string {
        return $this->tracingUuid;
    }

    #[\Override]
    public function getVersion(): int {
        return $this->header->version;
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
        $flags = $this->header->flags;

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
