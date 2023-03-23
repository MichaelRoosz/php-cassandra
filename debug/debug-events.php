<?php

require __DIR__ .  '/../php-cassandra.php';

$nodes =[
    [
        'host' => '127.0.0.1',
        'port' => 8082,
        'username' => 'qr',
        'password' =>'qr',
        'class' => 'Cassandra\Connection\Stream',           //use stream instead of socket, default socket. Stream may not work in some environment
        'connectTimeout' => 10,                             // connection timeout, default 5,  stream transport only
        'timeout' => 3600,                                    // write/recv timeout, default 30, stream transport only
        'persistent' => false,                              // use persistent PHP connection, default false,  stream transport only
    ]
];

$keyspace = '';
$connection = new \Cassandra\Connection($nodes, $keyspace, ['COMPRESSION' => 'lz4']);
$connection->connect();


$connection->addEventListener(new class () implements \Cassandra\EventListener {
    public function onEvent(\Cassandra\Response\Event $event): void
    {
        var_dump($event->getData());
    }
});

$register = new \Cassandra\Request\Register([
    \Cassandra\Response\Event::TOPOLOGY_CHANGE,
    \Cassandra\Response\Event::STATUS_CHANGE,
    \Cassandra\Response\Event::SCHEMA_CHANGE,
]);

$connection->syncRequest($register);

while ($connection->getResponse()) {
    sleep(1);
}
