<?php

declare(strict_types=1);

namespace Cassandra;

class Statement {
    protected Connection $_connection;

    protected int $_streamId;

    protected ?Response\Response $_response = null;

    public function __construct(Connection $connection, int $streamId) {
        $this->_connection = $connection;
        $this->_streamId = $streamId;
    }

    /**
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getResponse(): Response\Response {
        if ($this->_response === null) {
            $this->_response = $this->_connection->getResponse($this->_streamId);
        }

        if ($this->_response instanceof Response\Error) {
            throw $this->_response->getException();
        }

        return $this->_response;
    }

    /**
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getResult(): Response\Result {
        $response = $this->getResponse();

        if (!($response instanceof Response\Result)) {
            throw new Exception('received invalid response');
        }

        return $response;
    }

    public function setResponse(Response\Response $response): void {
        $this->_response = $response;
    }
}
