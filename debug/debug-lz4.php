<?php

require __DIR__ . '/../php-cassandra.php';

$lz4d = new \Cassandra\Compression\Lz4Decompressor(file_get_contents('lz4-1.bin'));

echo $lz4d->decompress(true) . PHP_EOL;
