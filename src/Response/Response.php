<?php

declare(strict_types=1);

namespace Cassandra\Response;

use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Protocol\Frame;
use Cassandra\Protocol\Flag;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Stringable;

/**
 * @psalm-consistent-constructor
 */
abstract class Response implements Frame, Stringable {
    /**
     * @var ?array<string,?string> $payload
     */
    private ?array $payload = null;

    private ?string $tracingUuid = null;

    /**
     * @var ?array<string> $warnings
     */
    private ?array $warnings = null;

    /**
     * @throws \Cassandra\Exception\ResponseException
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
            $this->header->opcode->value,
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
    public function getProtocolVersion(): ProtocolVersion {
        return $this->header->version;
    }

    /**
     * @var ?array<int, class-string<\Cassandra\Response\Response>>
     */
    private static ?array $responseClassMap = null;

    /**
     * @return array<int, class-string<\Cassandra\Response\Response>>
     */
    public static function getResponseClassMap(): array {
        // Cached because enum `->value` cannot be used in a constant expression
        // on PHP 8.1; once 8.1 support is dropped this can become a real `const`.
        return self::$responseClassMap ??= [
            Opcode::RESPONSE_ERROR->value => Error::class,
            Opcode::RESPONSE_READY->value => Ready::class,
            Opcode::RESPONSE_AUTHENTICATE->value => Authenticate::class,
            Opcode::RESPONSE_SUPPORTED->value => Supported::class,
            Opcode::RESPONSE_RESULT->value => Result::class,
            Opcode::RESPONSE_EVENT->value => Event::class,
            Opcode::RESPONSE_AUTH_CHALLENGE->value => AuthChallenge::class,
            Opcode::RESPONSE_AUTH_SUCCESS->value => AuthSuccess::class,
        ];
    }

    #[\Override]
    public function getStream(): int {
        return $this->header->stream;
    }

    public function getTracingUuid(): ?string {
        return $this->tracingUuid;
    }

    /**
     * @deprecated Use getProtocolVersion() instead.
     */
    #[\Override]
    public function getVersion(): int {
        return $this->header->version->value;
    }

    /**
     * @return array<string>
     */
    public function getWarnings(): array {
        return $this->warnings ?? [];
    }

    public function hasWarnings(): bool {
        return $this->warnings !== null;
    }

    /**
     * @throws \Cassandra\Exception\ResponseException
     */
    private function readExtraData(): void {
        $flags = $this->header->flags;

        if ($flags & Flag::TRACING) {
            $this->tracingUuid = $this->stream->readUuid();
        }

        if ($flags & Flag::WARNING) {
            $this->warnings = $this->stream->readStringList();
        }

        if ($flags & Flag::CUSTOM_PAYLOAD) {
            $this->payload = $this->stream->readBytesMap();
        }

        $this->stream->extraDataOffset($this->stream->pos());
    }
}
