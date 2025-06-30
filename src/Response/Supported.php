<?php

declare(strict_types=1);

namespace Cassandra\Response;

final class Supported extends Response {
    /**
     * @return array<string,array<int,string>>
     *
     * @throws \Cassandra\Response\Exception
     */
    public function getData(): array {
        $this->stream->offset(0);

        /**
         * Indicates which startup options are supported by the server. This message
         * comes as a response to an OPTIONS message.
         *
         * The body of a SUPPORTED message is a [string multimap]. This multimap gives
         * for each of the supported STARTUP options, the list of supported values.
         */
        return $this->stream->readStringMultimap();
    }
}
