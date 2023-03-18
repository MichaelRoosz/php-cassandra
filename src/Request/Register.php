<?php

declare(strict_types=1);

namespace Cassandra\Request;

use Cassandra\Protocol\Frame;
use Cassandra\Type;

class Register extends Request
{
    protected int $opcode = Frame::OPCODE_REGISTER;

    /**
     * @var array<mixed> $_events
     */
    protected array $_events;

    /**
     * REGISTER
     *
     * Register this connection to receive some type of events. The body of the
     * message is a [string list] representing the event types to register to. See
     * section 4.2.6 for the list of valid event types.
     *
     * The response to a REGISTER message will be a READY message.
     *
     * Please note that if a client driver maintains multiple connections to a
     * Cassandra node and/or connections to multiple nodes, it is advised to
     * dedicate a handful of connections to receive events, but to *not* register
     * for events on all connections, as this would only result in receiving
     * multiple times the same event messages, wasting bandwidth.
     *
     * @param array<mixed> $events
     */
    public function __construct(array $events)
    {
        $this->_events = $events;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function getBody(): string
    {
        return Type\CollectionList::binary($this->_events, [Type\Base::TEXT]);
    }
}
