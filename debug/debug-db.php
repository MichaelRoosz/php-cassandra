<?php

declare(strict_types=1);

use Cassandra\Connection;
use Cassandra\Type;

require __DIR__ . '/../php-cassandra.php';

/*
$nodes = [
    [
        'host' => 'localhost',
        'port' => 8082,
        'username' => 'qr',
        'password' => 'qr',
        'class' => 'Cassandra\Connection\Stream',           //use stream instead of socket, default socket. Stream may not work in some environment
        'connectTimeout' => 10,                             // connection timeout, default 5,  stream transport only
        'timeout' => 30,                                    // write/recv timeout, default 30, stream transport only
        //'persistent' => true,                              // use persistent PHP connection, default false,  stream transport only
    ],
];

$keyspace = '';
$connection = new Connection($nodes, $keyspace);
$connection->connect();
*/

// CREATE KEYSPACE IF NOT EXISTS test1 WITH replication = {'class': 'SimpleStrategy'};
// CREATE TABLE test1.test1 (id int, d duration, PRIMARY KEY (id));
// INSERT INTO test1.test1 (id, d) VALUES (1, 10h11m12s);
// INSERT INTO test1.test1 (id, d) VALUES (2, -10h11m12s);
// SELECT * FROM test1.test1;

/*
var_dump((string)new Type\Counter(1234));

var_dump((string)new Type\Duration([
    'months' => 1,
    'days' => 2,
    'nanoseconds' => 3,
]));

var_dump((string)new Type\Duration([
    'months' => -1,
    'days' => -2,
    'nanoseconds' => -3,
]));

var_dump((string)new Type\Duration([
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
]));

var_dump((string)new Type\Duration([
    'months' => 2147483647,
    'days' => 2147483647,
    'nanoseconds' => PHP_INT_MAX,
]));
*/

#var_dump((string)Type\Duration::fromString('-1d2h10m'));

#var_dump((string)Type\Duration::fromString('-768614336404564650y8mo1317624576693539401w1d2562047h47m16s854ms775us808ns'));
#var_dump((string)Type\Duration::fromString('768614336404564650y7mo1317624576693539401w2562047h47m16s854ms775us807ns'));

#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 1, 'days' => 2, 'nanoseconds'=> 3])));
#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 223231, 'days' => 277756, 'nanoseconds'=> 320688000000000])));

#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => 2147483647, 'days' => 2147483647, 'nanoseconds'=> PHP_INT_MAX])));
#var_dump(Type\Duration::parse(Type\Duration::binary(['months' => -2147483648, 'days' => -2147483648, 'nanoseconds'=> PHP_INT_MIN])));

#var_dump(Type\Varint::parse(Type\Varint::binary(PHP_INT_MAX)));
#var_dump(Type\Varint::parse(Type\Varint::binary(-1)));
#var_dump(Type\Varint::parse(Type\Varint::binary(-129)));
#var_dump(Type\Varint::parse(Type\Varint::binary(-5555)));
#var_dump(Type\Varint::parse(Type\Varint::binary(PHP_INT_MIN+1)));
#var_dump(Type\Varint::parse(Type\Varint::binary(PHP_INT_MIN)));
/*
var_dump((string)new Type\Duration([
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
]));

var_dump((string)new Type\Duration([
    'months' => 2147483647,
    'days' => 2147483647,
    'nanoseconds' => PHP_INT_MAX,
]));

var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('-1d2h10m')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));
var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('1d2h10m')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));

var_dump((string)(Type\Duration::fromString('-1d' . substr(PHP_INT_MIN, 1) . 'ns')));
var_dump('+' . (string)(Type\Duration::fromString('1d' . PHP_INT_MAX . 'ns')));

var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('-1d' . substr(PHP_INT_MIN, 1) . 'ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));
var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('1d' . PHP_INT_MAX . 'ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF'));

var_dump(Type\Duration::toDateInterval(Type\Duration::fromString('-1d' . substr((string)PHP_INT_MIN, 1) . 'ns')->getValue()));

var_dump(            '-178956970y8mo306783378w2d2562047h47m16s854ms775us808ns',
(string)new Type\Duration([
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
]));

var_dump(
    '- 184836873Y 5M 19D 23H 47M 16S 854775F',
    Type\Duration::toDateInterval(Type\Duration::fromString('-178956970y8mo306783378w2d2562047h47m16s854ms775us808ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF')
);*/

/*
$minDuration = [
    'months' => -2147483648,
    'days' => -2147483648,
    'nanoseconds' => PHP_INT_MIN,
];

var_dump(
    $minDuration,
    Type\Duration::fromDateInterval(Type\Duration::fromValue($minDuration)->toDateInterval())->getValue()
);
*/
/*
$varInt = (string) PHP_INT_MAX . (string) PHP_INT_MAX;

var_dump(
    #gmp_strval(gmp_com(1)),
    #gmp_strval(gmp_com(-1)),
    #gmp_strval(gmp_com(2)),
    #gmp_strval(gmp_com(-2)),
    #$varInt,
    #(new Type\Varint($varInt))->getValue(),
    #(new Type\Varint($varInt))->getValueAsGmp(),
    #(new Type\Varint($varInt))->getValueAsInt(),
    (string) -1, Type\Varint::fromBinary((new Type\Varint(-1))->getBinary())->getValue(),
    (string) -2, Type\Varint::fromBinary((new Type\Varint(-2))->getBinary())->getValue(),
    (string) PHP_INT_MIN, Type\Varint::fromBinary((new Type\Varint(PHP_INT_MIN))->getBinary())->getValue(),
    (string) PHP_INT_MIN . (string) PHP_INT_MAX, Type\Varint::fromBinary((new Type\Varint((string) PHP_INT_MIN . (string) PHP_INT_MAX))->getBinary())->getValue(),
    (string) PHP_INT_MAX, Type\Varint::fromBinary((new Type\Varint(PHP_INT_MAX))->getBinary())->getValue(),
    (string) PHP_INT_MAX . (string) PHP_INT_MAX, Type\Varint::fromBinary((new Type\Varint((string) PHP_INT_MAX . (string) PHP_INT_MAX))->getBinary())->getValue(),
);
*/

/*
$varInt = (string) PHP_INT_MIN . (string) PHP_INT_MAX;

var_dump(bin2hex((new Type\Varint($varInt))->getBinary()));
var_dump(bin2hex(stringToBinary($varInt)));


$varInt = (string) PHP_INT_MAX . (string) PHP_INT_MAX;

var_dump(bin2hex((new Type\Varint($varInt))->getBinary()));
var_dump(bin2hex(stringToBinary($varInt)));
*/

$varInt = (string) PHP_INT_MIN . (string) PHP_INT_MAX;

var_dump(Type\Varint::fromBinary((new Type\Varint($varInt))->getBinary())->__toString());
#var_dump(binaryToString(stringToBinary($varInt)));

$varInt = (string) PHP_INT_MAX . (string) PHP_INT_MAX;

var_dump(Type\Varint::fromBinary((new Type\Varint($varInt))->getBinary())->__toString());
#var_dump(binaryToString(stringToBinary($varInt)));

/*
        if (self::gmpAvailable()) {
            $this->nativeValue = 0;

            if ($value instanceof GMP) {
                $this->gmpValue = $value;
            } else {
                $this->gmpValue = gmp_init($value);
            }

            return;
        }


        if (is_string($value) && (string) $this->nativeValue !== $value) {
            throw new Exception('Value of Varint is outside of possible range (this system only supports signed ' . (PHP_INT_SIZE*8) . '-bit integers). Install the gmp php extension for support of bigger numbers.');
        }

        public static function fromBinary(string $binary, null|int|array $definition = null): static {
        if (self::gmpAvailable()) {
            $isNegative = (ord($binary[0]) & 0x80) !== 0;

            if ($isNegative) {
                for ($i = 0; $i < strlen($binary); $i++) {
                    $binary[$i] = ~$binary[$i];
                }
            }

            $value = gmp_import($binary, 1, GMP_MSW_FIRST | GMP_LITTLE_ENDIAN);

            if ($isNegative) {
                //$value = gmp_add($value, 1);
                //$value = gmp_neg($value);
                $value = gmp_com($value);
            }

            return new static($value);
        }

        public function getBinary(): string {
        if ($this->gmpValue !== null) {
            $isNegative = gmp_sign($this->gmpValue) === -1;

            if ($isNegative) {
                $value = gmp_add($this->gmpValue, 1);
            } else {
                $value = $this->gmpValue;
            }

            $binary = gmp_export($value, 1, GMP_MSW_FIRST | GMP_LITTLE_ENDIAN);

            if ($isNegative) {
                for ($i = 0; $i < strlen($binary); $i++) {
                    $binary[$i] = ~$binary[$i];
                }
            }

            $length = strlen($binary);

            // Check if the most significant bit is set, which could be interpreted as a negative number
            if (!$isNegative && ($length === 0 || (ord($binary[0]) & 0x80) !== 0)) {
                // Add an extra byte with a 0x00 value to keep the number positive
                $binary = chr(0) . $binary;
            }
            // Check if the most significant bit is not set, which could be interpreted as a positive number
            elseif ($isNegative && ($length === 0 || (ord($binary[0]) & 0x80) === 0)) {
                // Add an extra byte with a 0xFF value to keep the number negative
                $binary = chr(0xFF) . $binary;
            }
            #var_dump(bin2hex($binary));

            return $binary;
        }

        public function getValueAsGmp(): GMP {
            if ($this->gmpValue === null) {
                throw new Exception('The php gmp extension is required for GMP values.');
            }

            return $this->gmpValue;
        }

        protected static function gmpAvailable() : bool {
            if (self::$gmpAvailable === null) {
                self::$gmpAvailable = extension_loaded('gmp');
            }

            return self::$gmpAvailable;
        }
*/
