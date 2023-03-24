<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;

class Prepare extends Request {
    public const FLAG_WITH_KEYSPACE = 0x01;

    protected int $opcode = Frame::OPCODE_PREPARE;

    protected string $_cql;

    /**
     * @var array{
     *  keyspace?: string,
     * } $_options
     */
    protected array $_options;

    /**
     * @param array{
     *  keyspace?: string,
     * } $options
     */
    public function __construct(string $cql, array $options = []) {
        $this->_cql = $cql;
        $this->_options = $options;
    }

    /**
     * @throws \Cassandra\Request\Exception
     */
    public function getBody(): string {
        $flags = 0;
        $optional = '';

        $body = pack('N', strlen($this->_cql)) . $this->_cql;

        if (isset($this->_options['keyspace'])) {
            if ($this->version >= 5) {
                $flags |= Query::FLAG_WITH_KEYSPACE;
                $optional .= pack('n', strlen($this->_options['keyspace'])) . $this->_options['keyspace'];
            } else {
                throw new Exception('Option "keyspace" not supported by server');
            }
        }

        if ($this->version < 5) {
            return $body;
        } else {
            return $body . pack('N', $flags) . $optional;
        }
    }
}
