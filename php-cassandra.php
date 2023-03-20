<?php

declare(strict_types=1);

/* This file can be used instead of install with composer.
 * Just include "require __DIR__  . '/PATH/php-cassandra/php-cassandra.php';" to your code (where PATH is path to php-cassandra folder).
 */
spl_autoload_register(function ($class) {
    if (str_starts_with($class, 'Cassandra\\')) {
        $path = str_replace('\\', '/', substr($class, strlen('Cassandra\\')));
        require  __DIR__ . '/src/' . $path . '.php';
    }
});
