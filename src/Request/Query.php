<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Opcode;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Consistency;
use Cassandra\Request\Options\RequestOptions;
use Cassandra\Type;

final class Query extends Request {
    /**
     * QUERY
     *
     * Performs a CQL query. The body of the message consists of a CQL query as a [long
     * string] followed by the [consistency] for the operation.
     *
     * Note that the consistency is ignored by some queries (USE, CREATE, ALTER,
     * TRUNCATE, ...).
     *
     * The server will respond to a QUERY message with a RESULT message, the content
     * of which depends on the query.
     *
     * @param array<mixed> $values
     */
    public function __construct(
        protected string $query,
        protected array $values = [],
        protected Consistency $consistency = Consistency::ONE,
        protected QueryOptions $options = new QueryOptions()
    ) {
        parent::__construct(Opcode::REQUEST_QUERY);
    }

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    #[\Override]
    public function getBody(): string {
        $body = pack('N', strlen($this->query)) . $this->query;
        $body .= self::queryParameters($this->consistency, $this->values, $this->options, $this->version);

        return $body;
    }

    /**
     * @param array<mixed> $values
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Request\Exception
     */
    public static function queryParameters(Consistency $consistency, array $values = [], QueryOptions $options = new QueryOptions(), int $version = 3): string {
        $flags = 0;
        $optional = '';

        $opt = $options->toArray();

        if ($values) {
            $flags |= QueryFlag::VALUES->value;
            $optional .= Request::valuesBinary($values, !empty($opt['names_for_values']));
        }

        if (!empty($opt['skip_metadata'])) {
            $flags |= QueryFlag::SKIP_METADATA->value;
        }

        if (isset($opt['page_size'])) {
            $flags |= QueryFlag::PAGE_SIZE->value;
            $optional .= pack('N', $opt['page_size']);
        }

        if (isset($opt['paging_state'])) {
            $flags |= QueryFlag::WITH_PAGING_STATE->value;
            $optional .= pack('N', strlen($opt['paging_state'])) . $opt['paging_state'];
        }

        if (isset($opt['serial_consistency'])) {
            $flags |= QueryFlag::WITH_SERIAL_CONSISTENCY->value;
            $optional .= pack('n', $opt['serial_consistency']);
        }

        if (isset($opt['default_timestamp'])) {
            $flags |= QueryFlag::WITH_DEFAULT_TIMESTAMP->value;
            $optional .= (new Type\Bigint($opt['default_timestamp']))->getBinary();
        }

        if (!empty($opt['names_for_values'])) {
            $flags |= QueryFlag::WITH_NAMES_FOR_VALUES->value;
        }

        if (isset($opt['keyspace'])) {
            if ($version >= 5) {
                $flags |= QueryFlag::WITH_KEYSPACE->value;
                $optional .= pack('n', strlen($opt['keyspace'])) . $opt['keyspace'];
            } else {
                throw new Exception('Option "keyspace" not supported by server');
            }
        }

        if (isset($opt['now_in_seconds'])) {
            if ($version >= 5) {
                $flags |= QueryFlag::WITH_NOW_IN_SECONDS->value;
                $optional .= pack('N', $opt['now_in_seconds']);
            } else {
                throw new Exception('Option "now_in_seconds" not supported by server');
            }
        }

        if ($version < 5) {
            return pack('n', $consistency->value) . chr($flags) . $optional;
        } else {
            return pack('n', $consistency->value) . pack('N', $flags) . $optional;
        }
    }
}
