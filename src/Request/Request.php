<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Protocol\Frame;
use Cassandra\Protocol\Flag;
use Cassandra\ValueFactory;
use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Value\NotSet;
use Cassandra\Value\ValueBase;
use DateTimeInterface;
use Stringable;

abstract class Request implements Frame, Stringable {
    /**
     * Most entries a `[short]`-counted list on the wire can hold.
     *
     * The protocol writes both the number of bound values and the number of
     * statements in a batch as a two-byte count. pack('n', …) takes the low two
     * bytes of whatever it is given without complaint, so a longer list would go
     * out announcing a count of its own length modulo 65536 — the body would
     * then be read as that many entries and whatever followed taken for the
     * fields after them, which is a request the coordinator misparses rather
     * than rejects. Refused here instead, where the caller can still be told
     * which list it was.
     */
    protected const MAX_SHORT_COUNT = 65535;

    private const INT32_MAX = 2147483647;
    private const INT32_MIN = -2147483647 - 1;

    /**
     * Whether the keyspace this request carries was put there by the connection
     * rather than by the caller.
     *
     * A request is addressed on its way to the wire and keeps what it was given,
     * so the option alone cannot say who set it: sent a second time — by hand,
     * or after {@see \Cassandra\Connection::setKeyspace()} — a request that took
     * the connection's default on the first send would look exactly like one the
     * caller addressed themselves, and would go on running against the keyspace
     * the connection has since left. Recorded here so that a default can be
     * replaced by the next default while a keyspace the caller named is still
     * never touched.
     */
    private bool $keyspaceIsConnectionDefault = false;

    /**
     * @param ?array<string,string> $payload
     */
    public function __construct(
        private Opcode $opcode,

        /**
         * The stream id this request is sent on, or null while it has not been
         * assigned one yet.
         */
        private ?int $stream = null,

        private int $flags = 0,
        private ?array $payload = null,
        protected ProtocolVersion $version = ProtocolVersion::V3
    ) {
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function __toString(): string {

        if ($this->stream === null) {
            throw new RequestException(
                'This request has not been assigned a stream id yet, so it cannot be encoded',
                ExceptionCode::REQUEST_STREAM_NOT_ASSIGNED->value,
                [
                    'request_class' => static::class,
                    'opcode' => $this->opcode->name,
                ]
            );
        }

        $body = $this->getBody();

        if ($this->flags & Flag::CUSTOM_PAYLOAD) {
            if ($this->payload === null) {
                $this->flags &= ~Flag::CUSTOM_PAYLOAD;
            } else {
                $payload = $this->payload;
                self::assertShortCount(count($payload), 'custom payload');
                $payloadData = pack('n', count($payload));

                foreach ($payload as $key => $val) {
                    self::assertShortString($key, 'custom payload key');
                    $payloadData .= pack('n', strlen($key)) . $key;
                    $payloadData .= pack('N', strlen($val)) . $val;
                }

                $body = $payloadData . $body;
            }
        }

        return pack(
            'CCnCN',
            $this->version->value,
            $this->flags,
            $this->stream,
            $this->opcode->value,
            strlen($body)
        ) . $body;
    }

    /**
     * Take over another request's record of who the keyspace it carries belongs
     * to, for a request built out of that one's options.
     *
     * The driver builds requests from other requests — the PREPARE an
     * auto-prepared query needs, the PREPARE and EXECUTE a repreparation sends;
     * see {@see \Cassandra\Connection\ResponseDispatcher}. Those are built from
     * the source's options, which carry the keyspace but not the record of who
     * put it there ({@see self::$keyspaceIsConnectionDefault}), so the derived
     * request would start out claiming that the caller named a keyspace this
     * driver applied.
     *
     * That is not a cosmetic difference: a keyspace the caller named is never
     * taken back, so {@see self::clearDefaultKeyspace()} would leave it on a
     * request bound for a connection below v5, where {@see self::getBody()}
     * refuses to encode one at all — a request that worked on v5 failing for
     * good rather than being addressed the way v4 addresses one.
     */
    final public function adoptDefaultKeyspaceMarkerFrom(Request $source): void {

        $this->keyspaceIsConnectionDefault = $source->keyspaceIsConnectionDefault;
    }

    /**
     * Fill in the keyspace the connection is on, for a request that names none
     * of its own.
     *
     * From protocol v5 the keyspace travels with each request rather than being
     * a property of the node's session, so a request that carries none runs
     * against whatever the coordinator defaults to. The connection applies its
     * own here on the way to the wire, at the point where the negotiated version
     * is known; see {@see \Cassandra\Connection\RequestExecutor}.
     *
     * A no-op for the requests that have no keyspace to speak of — STARTUP,
     * OPTIONS, REGISTER, AUTH_RESPONSE — and for one the caller named a keyspace
     * on, which is theirs to be right about.
     *
     * The overrides decide that with {@see self::acceptsDefaultKeyspace()} and
     * then say so with {@see self::markKeyspaceAsConnectionDefault()}, rather
     * than by testing the option for null: a request sent a second time already
     * carries the keyspace of the first send, and telling that apart from one
     * the caller put there is exactly what those two are for.
     *
     * @throws \Cassandra\Exception\RequestException the overrides rebuild their
     * options to put the keyspace in, and building options can refuse what it is
     * given
     */
    public function applyDefaultKeyspace(string $keyspace): void {
    }

    /**
     * Take back a keyspace {@see self::applyDefaultKeyspace()} put on this
     * request, for a send on a protocol version that cannot carry one.
     *
     * The keyspace option only exists from v5. A request is addressed on its way
     * to the wire and keeps what it was given, so one that took the connection's
     * default on a v5 send still carries it when the same object is sent again
     * on a v4 connection — a second {@see \Cassandra\Connection}, or the same one
     * after it renegotiated down on reconnect. Left there it is not merely
     * ignored: {@see self::getBody()} refuses to encode a keyspace the version
     * cannot express, so a request that worked once would fail for good.
     *
     * Only a keyspace this driver put there is taken off, which is the whole
     * point of {@see self::$keyspaceIsConnectionDefault}: one the caller named
     * themselves is theirs, and refusing to encode it is the right answer rather
     * than quietly running the statement somewhere else.
     *
     * @throws \Cassandra\Exception\RequestException the overrides rebuild their
     * options to take the keyspace out, and building options can refuse what it
     * is given
     */
    public function clearDefaultKeyspace(): void {
    }

    public function enableTracing(): void {
        $this->flags |= Flag::TRACING;
    }

    #[\Override]
    public function getBody(): string {
        return '';
    }

    #[\Override]
    public function getFlags(): int {
        return $this->flags;
    }

    #[\Override]
    public function getOpcode(): Opcode {
        return $this->opcode;
    }

    /**
     * @return ?array<string,string>
     */
    public function getPayload(): ?array {
        return $this->payload;
    }

    #[\Override]
    public function getProtocolVersion(): ProtocolVersion {
        return $this->version;
    }

    /**
     * How long the server may take to answer this request, if the request asks
     * for something other than the connection's default, and null otherwise.
     *
     * Requests that carry options override this; the rest — STARTUP, OPTIONS,
     * REGISTER, AUTH_RESPONSE — have nothing to say about it.
     */
    public function getRequestTimeout(): ?float {
        return null;
    }

    /**
     * The stream id this request will be sent on, or null while it has not
     * been assigned one. Encoding the request is what requires an id, so that
     * is where an unassigned one is refused.
     */
    #[\Override]
    public function getStream(): ?int {
        return $this->stream;
    }

    /**
     * @deprecated Use getProtocolVersion() instead.
     */
    #[\Override]
    public function getVersion(): int {
        return $this->version->value;
    }

    public function setFlags(int $flags): void {
        $this->flags = $flags;
    }

    /**
     * @param array<string,string> $payload
     *
     * @throws \Cassandra\Exception\RequestException
     */
    public function setPayload(array $payload): void {
        $this->payload = self::validatePayload($payload);
        $this->flags |= Flag::CUSTOM_PAYLOAD;
    }

    public function setStream(int $stream): void {
        $this->stream = $stream;
    }

    public function setVersion(ProtocolVersion $version): void {
        $this->version = $version;
    }

    /**
     * Whether {@see self::applyDefaultKeyspace()} may write the connection's
     * keyspace onto this request, given the one its options carry now.
     *
     * Yes while it carries none, and yes again for one this connection put there
     * itself — that is a default being replaced by the current default, not the
     * caller being overruled. Only a keyspace the caller named is left alone,
     * for good: they pointed this one statement somewhere, and no later
     * {@see \Cassandra\Connection::setKeyspace()} takes that back.
     */
    final protected function acceptsDefaultKeyspace(?string $currentKeyspace): bool {

        return $currentKeyspace === null || $this->keyspaceIsConnectionDefault;
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    protected static function assertShortCount(int $count, string $field): void {
        if ($count > self::MAX_SHORT_COUNT) {
            throw new RequestException(
                message: ucfirst($field) . ' has too many entries for the protocol short-count encoding',
                code: ExceptionCode::REQUEST_TOO_MANY_MAP_ENTRIES->value,
                context: ['field' => $field, 'count' => $count, 'maximum_count' => self::MAX_SHORT_COUNT]
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    protected static function assertShortString(string $value, string $field): void {
        $length = strlen($value);
        if ($length > self::MAX_SHORT_COUNT) {
            throw new RequestException(
                message: ucfirst($field) . ' is too long for the protocol short-string encoding',
                code: ExceptionCode::REQUEST_FIELD_TOO_LONG->value,
                context: ['field' => $field, 'length' => $length, 'maximum_length' => self::MAX_SHORT_COUNT]
            );
        }
    }

    /**
     * Whether the keyspace this request carries is one the connection put there,
     * see {@see self::$keyspaceIsConnectionDefault}. What
     * {@see self::clearDefaultKeyspace()} is allowed to take back.
     */
    final protected function carriesDefaultKeyspace(): bool {

        return $this->keyspaceIsConnectionDefault;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    protected function encodeQueryParametersAsBinary(
        Consistency $consistency,
        array $values = [],
        QueryOptions $options = new QueryOptions(),
        ProtocolVersion $version = ProtocolVersion::V3,
        bool $namesAreExact = false
    ): string {

        $flags = 0;
        $optional = '';

        if ($values) {
            $flags |= QueryFlag::VALUES;
            $optional .= self::encodeQueryValuesAsBinary($values, $options->namesForValues === true, $namesAreExact);
        }

        if (($options instanceof ExecuteOptions) && $options->skipMetadata) {
            $flags |= QueryFlag::SKIP_METADATA;
        }

        if ($options->pageSize !== null) {
            $flags |= QueryFlag::PAGE_SIZE;
            $optional .= pack('N', max(100, $options->pageSize));
        }

        if ($options->pagingState !== null) {
            $flags |= QueryFlag::WITH_PAGING_STATE;
            $optional .= pack('N', strlen($options->pagingState)) . $options->pagingState;
        }

        if ($options->serialConsistency !== null) {
            $flags |= QueryFlag::WITH_SERIAL_CONSISTENCY;
            $optional .= pack('n', $options->serialConsistency->value);
        }

        if ($options->defaultTimestamp !== null) {
            $flags |= QueryFlag::WITH_DEFAULT_TIMESTAMP;
            $optional .= pack('J', $options->defaultTimestamp);
        }

        if ($options->namesForValues === true) {
            $flags |= QueryFlag::WITH_NAMES_FOR_VALUES;
        }

        if ($options->keyspace !== null) {
            if ($version->value >= ProtocolVersion::V5->value) {
                self::assertShortString($options->keyspace, 'query keyspace');
                $flags |= QueryFlag::WITH_KEYSPACE;
                $optional .= pack('n', strlen($options->keyspace)) . $options->keyspace;
            } else {
                throw new RequestException(
                    message: 'Server protocol version does not support request option "keyspace"',
                    code: ExceptionCode::REQUEST_UNSUPPORTED_OPTION_KEYSPACE->value,
                    context: [
                        'request' => 'QUERY',
                        'option' => 'keyspace',
                        'required_protocol_version' => ProtocolVersion::V5->inOptionFormat(),
                        'actual_protocol_version' => $version->inOptionFormat(),
                        'keyspace' => $options->keyspace,
                    ]
                );
            }
        }

        if ($options->nowInSeconds !== null) {
            if ($version->value >= ProtocolVersion::V5->value) {
                $flags |= QueryFlag::WITH_NOW_IN_SECONDS;
                $optional .= pack('N', $options->nowInSeconds);
            } else {
                throw new RequestException(
                    message: 'Server protocol version does not support request option "now_in_seconds"',
                    code: ExceptionCode::REQUEST_UNSUPPORTED_OPTION_NOW_IN_SECONDS->value,
                    context: [
                        'request' => 'QUERY',
                        'option' => 'now_in_seconds',
                        'required_protocol_version' => ProtocolVersion::V5->inOptionFormat(),
                        'actual_protocol_version' => $version->inOptionFormat(),
                        'now_in_seconds' => $options->nowInSeconds,
                    ]
                );
            }
        }

        if ($version->value < ProtocolVersion::V5->value) {
            return pack('n', $consistency->value) . chr($flags & 0xFF) . $optional;
        } else {
            return pack('n', $consistency->value) . pack('N', $flags) . $optional;
        }
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Exception\RequestException
     */
    protected function encodeQueryValuesAsBinary(array $values, bool $namesForValues = false, bool $namesAreExact = false): string {

        $valueCount = count($values);
        if ($valueCount > self::MAX_SHORT_COUNT) {
            throw new RequestException(
                message: 'Too many bound values for one request; the protocol counts them in two bytes',
                code: ExceptionCode::REQUEST_VALUES_TOO_MANY_VALUES->value,
                context: [
                    'stage' => 'values_encoding',
                    'value_count' => $valueCount,
                    'max_value_count' => self::MAX_SHORT_COUNT,
                ]
            );
        }

        $valuesBinary = pack('n', $valueCount);

        /** @psalm-suppress MixedAssignment */
        foreach ($values as $name => $value) {
            switch (true) {
                case $value instanceof ValueBase:
                    $binary = $value->getBinary();

                    break;

                case $value instanceof NotSet:
                    $binary = $value;

                    break;

                case $value === null:
                    $binary = null;

                    break;

                case is_int($value):
                    // Untyped values carry no type tag, so the server validates
                    // the byte width against the target column: an int column
                    // requires exactly 4 bytes. We therefore encode ints as
                    // int32. A value outside that range would be silently
                    // truncated by pack('N'), so reject it here — the caller must
                    // wrap it (e.g. Cassandra\Value\Bigint) or use a prepared
                    // statement, where the bind-marker type drives the encoding.
                    if ($value < self::INT32_MIN || $value > self::INT32_MAX) {
                        throw new RequestException(
                            message: 'Integer bound value is outside the 32-bit range and would be truncated; wrap it in a typed value (e.g. Cassandra\\Value\\Bigint) or use a prepared statement',
                            code: ExceptionCode::REQUEST_VALUES_INT_OUT_OF_INT32_RANGE->value,
                            context: [
                                'stage' => 'values_encoding',
                                'name' => $name,
                                'value' => $value,
                                'min' => self::INT32_MIN,
                                'max' => self::INT32_MAX,
                            ]
                        );
                    }

                    $binary = pack('N', $value);

                    break;

                case is_string($value):
                    $binary = $value;

                    break;

                case is_bool($value):
                    $binary = $value ? chr(1) : chr(0);

                    break;

                case is_float($value):
                    $binary = pack('E', $value);

                    break;

                case $value instanceof DateTimeInterface:
                    // A DateTime has no unambiguous untyped encoding: a timestamp
                    // column expects an 8-byte long, date/time expect their own
                    // fixed widths, and only a text column would accept a
                    // formatted string. Rather than guess a format that is wrong
                    // for every temporal column, require an explicit typed value
                    // (Cassandra\Value\Timestamp / Date / Time) or a prepared
                    // statement, where the bind-marker type drives the encoding.
                    throw new RequestException(
                        message: 'DateTime bound value has no unambiguous untyped encoding; wrap it in a typed value (e.g. Cassandra\\Value\\Timestamp, Date or Time) or use a prepared statement',
                        code: ExceptionCode::REQUEST_VALUES_AMBIGUOUS_DATETIME->value,
                        context: [
                            'stage' => 'values_encoding',
                            'name' => $name,
                            'php_type' => get_class($value),
                        ]
                    );

                default:
                    throw new RequestException(
                        message: 'Unsupported bound value type',
                        code: ExceptionCode::REQUEST_VALUES_UNSUPPORTED_VALUE_TYPE->value,
                        context: [
                            'stage' => 'values_encoding',
                            'php_type' => gettype($value),
                            'name' => $name,
                        ]
                    );
            }

            if ($namesForValues) {
                if (is_string($name)) {
                    // When the names come straight from the server's bind marker
                    // metadata ($namesAreExact) they are sent unchanged — quoted
                    // identifiers are case-sensitive. User-supplied names are
                    // lowercased, matching how the server stores unquoted
                    // identifiers.
                    $encodedName = $namesAreExact ? $name : strtolower($name);
                    self::assertShortString($encodedName, 'bound value name');
                    $valuesBinary .= pack('n', strlen($encodedName)) . $encodedName;
                } elseif ($namesAreExact) {
                    // The names came from the server's bind marker metadata, so
                    // this is one of them and not a caller's mistake: PHP turns
                    // an array key that is a canonical decimal integer string
                    // into an int, and a quoted numeric column name — `"0"` — is
                    // exactly that. {@see self::encodeQueryValuesForBindMarkerTypes()}
                    // keys its result by marker name, so such a marker arrives
                    // here as an int and would otherwise be refused below,
                    // leaving a statement that can never be bound at all.
                    //
                    // Spelled back out rather than worked around, the coercion
                    // being lossless in exactly the cases it happens in. Only
                    // for names this driver was told; a caller's own are held to
                    // the check below, where an int key really does mean a
                    // sequential array was passed with names_for_values on.
                    $name = (string) $name;
                    self::assertShortString($name, 'bound value name');
                    $valuesBinary .= pack('n', strlen($name)) . $name;
                } else {
                    throw new RequestException(
                        message: 'Invalid values format: sequential array provided while names_for_values=true expects associative array',
                        code: ExceptionCode::REQUEST_VALUES_NAMES_FOR_VALUES_EXPECTS_ASSOCIATIVE->value,
                        context: [
                            'stage' => 'values_encoding',
                            'names_for_values' => true,
                            'provided_key_type' => gettype($name),
                        ]
                    );
                }
            } elseif (is_string($name)) {
                /**
                * @see https://github.com/duoshuo/php-cassandra/issues/29
                */
                throw new RequestException(
                    message: 'Invalid values format: associative array provided while names_for_values=false expects sequential array',
                    code: ExceptionCode::REQUEST_VALUES_NAMES_FOR_VALUES_EXPECTS_SEQUENTIAL->value,
                    context: [
                        'stage' => 'values_encoding',
                        'names_for_values' => false,
                        'provided_key_type' => 'string',
                    ]
                );
            }

            if ($binary === null) {
                $valuesBinary .= "\xff\xff\xff\xff";
            } elseif ($binary instanceof NotSet) {
                $valuesBinary .= "\xff\xff\xff\xfe";
            } else {
                $valuesBinary .= pack('N', strlen($binary)) . $binary;
            }
        }

        return $valuesBinary;
    }

    /**
     * @param array<mixed> $values
     * @param array<\Cassandra\Response\Result\ColumnInfo> $bindMarkers
     * @return array<mixed>
     *
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    protected function encodeQueryValuesForBindMarkerTypes(array $values, array $bindMarkers, bool $namesForValues): array {

        // Named values are matched to the server-reported bind marker names
        // case-insensitively: unquoted identifiers are stored lowercase by the
        // server, so a user-supplied key like "userId" must still bind to the
        // marker "userid".
        $valuesByLowercaseName = [];
        $originalNameByLowercaseName = [];
        if ($namesForValues) {
            /** @psalm-suppress MixedAssignment */
            foreach ($values as $name => $value) {
                if (is_string($name)) {
                    $lowercaseName = strtolower($name);
                    if (isset($originalNameByLowercaseName[$lowercaseName]) && $originalNameByLowercaseName[$lowercaseName] !== $name) {
                        throw new RequestException(
                            message: 'Multiple supplied value names differ only by case and cannot be bound unambiguously',
                            code: ExceptionCode::REQUEST_VALUES_DUPLICATE_BIND_MARKER->value,
                            context: ['stage' => 'values_encoding', 'names' => [$originalNameByLowercaseName[$lowercaseName], $name]]
                        );
                    }
                    $originalNameByLowercaseName[$lowercaseName] = $name;
                    $valuesByLowercaseName[$lowercaseName] = $value;
                }
            }
        }

        $encodedValues = [];
        $usedValueKeys = [];
        foreach ($bindMarkers as $index => $bindMarker) {

            if ($namesForValues) {
                $key = $bindMarker->name;

                // Named encoding keys the resulting value set by marker name, so
                // two markers sharing a name would collapse into one entry and
                // silently send fewer values than the statement has markers.
                // That is ambiguous (which value binds to which marker?), so
                // reject it and point the caller at positional binding.
                if (array_key_exists($key, $encodedValues)) {
                    throw new RequestException(
                        message: 'Duplicate bind marker name "' . $key . '"; named values cannot be bound unambiguously when a marker name repeats. Provide values as a positional (sequential) array instead.',
                        code: ExceptionCode::REQUEST_VALUES_DUPLICATE_BIND_MARKER->value,
                        context: [
                            'stage' => 'values_encoding',
                            'bind_marker' => $key,
                        ]
                    );
                }

                if (array_key_exists($key, $values)) {
                    /** @psalm-suppress MixedAssignment */
                    $value = $values[$key];
                    $usedValueKeys[$key] = true;
                } elseif (array_key_exists(strtolower($key), $valuesByLowercaseName)) {
                    /** @psalm-suppress MixedAssignment */
                    $value = $valuesByLowercaseName[strtolower($key)];
                    $usedValueKeys[$originalNameByLowercaseName[strtolower($key)]] = true;
                } else {
                    throw $this->missingBindValueException($key);
                }
            } else {
                $key = $index;

                if (!array_key_exists($key, $values)) {
                    throw $this->missingBindValueException($key);
                }

                /** @psalm-suppress MixedAssignment */
                $value = $values[$key];
                $usedValueKeys[$key] = true;
            }

            if (
                $value === null
                || ($value instanceof ValueBase)
                || ($value instanceof NotSet)
            ) {
                $encodedValues[$key] = $value;
            } else {
                $encodedValues[$key] = ValueFactory::getValueObjectFromValue($bindMarker->type, $value);
            }
        }

        $extraKeys = array_values(array_diff(array_keys($values), array_keys($usedValueKeys)));
        if ($extraKeys !== []) {
            throw new RequestException(
                message: 'Values were provided that do not match any bind marker',
                code: ExceptionCode::REQUEST_VALUES_EXTRA_BIND_VALUE->value,
                context: ['stage' => 'values_encoding', 'extra_bind_values' => $extraKeys]
            );
        }

        return $encodedValues;
    }

    /**
     * Forget that the keyspace this request carried was the connection's,
     * because it no longer carries one; the counterpart of
     * {@see self::markKeyspaceAsConnectionDefault()}.
     */
    final protected function forgetDefaultKeyspace(): void {

        $this->keyspaceIsConnectionDefault = false;
    }

    /**
     * Record that the keyspace this request now carries is the connection's
     * default, see {@see self::$keyspaceIsConnectionDefault}.
     */
    final protected function markKeyspaceAsConnectionDefault(): void {

        $this->keyspaceIsConnectionDefault = true;
    }

    private function missingBindValueException(int|string $key): RequestException {
        return new RequestException(
            message: 'Missing value for bind marker; provide a value for every bind marker (use null or Cassandra\\Value\\NotSet explicitly if intended)',
            code: ExceptionCode::REQUEST_VALUES_MISSING_BIND_VALUE->value,
            context: [
                'stage' => 'values_encoding',
                'bind_marker' => $key,
            ]
        );
    }

    /**
     * @param array<mixed> $payload
     * @return array<string,string>
     *
     * @throws \Cassandra\Exception\RequestException
     */
    private static function validatePayload(array $payload): array {
        $validatedPayload = [];
        foreach ($payload as $key => $value) {
            if (!is_string($key) || !is_string($value)) {
                throw new RequestException(
                    'Invalid custom payload; every key and value must be a string',
                    ExceptionCode::REQUEST_INVALID_CUSTOM_PAYLOAD->value,
                    [
                        'key_type' => get_debug_type($key),
                        'value_type' => get_debug_type($value),
                    ]
                );
            }

            $validatedPayload[$key] = $value;
        }

        return $validatedPayload;
    }
}
