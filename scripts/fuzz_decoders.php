<?php

declare(strict_types=1);

/**
 * Feeds mutated bytes to every decoder that reads what a peer sends, and fails
 * when anything but a Cassandra\Exception escapes.
 *
 * Usage: php scripts/fuzz_decoders.php [iterations] [seed]
 */

require __DIR__ . '/../vendor/autoload.php';

use Cassandra\Compression\Lz4Decompressor;
use Cassandra\Exception\CassandraException;
use Cassandra\Connection\ResponseReader;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Error;
use Cassandra\Response\Event;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\TypeNameParser;
use Cassandra\Value\ValueComparator;
use Cassandra\Value\ValueEncodeConfig;

ini_set('memory_limit', '1G');
error_reporting(E_ALL);
set_error_handler(static function (int $severity, string $message, string $file, int $line): bool {
    throw new ErrorException($message, 0, $severity, $file, $line);
});

$iterations = (int) ($argv[1] ?? 200000);
$seed = (int) ($argv[2] ?? 20261107);
mt_srand($seed);

function randomBytes(int $length): string {
    $bytes = '';
    for ($i = 0; $i < $length; $i++) {
        $bytes .= chr(mt_rand(0, 255));
    }

    return $bytes;
}

function mutate(string $bytes): string {
    $mutations = mt_rand(0, 3);
    for ($i = 0; $i < $mutations && $bytes !== ''; $i++) {
        $position = mt_rand(0, strlen($bytes) - 1);
        switch (mt_rand(0, 4)) {
            case 0:
                $bytes[$position] = chr(mt_rand(0, 255));

                break;
            case 1:
                $bytes[$position] = "\xFF";

                break;
            case 2:
                $bytes = substr($bytes, 0, $position);

                break;
            case 3:
                $bytes = substr_replace($bytes, randomBytes(mt_rand(1, 8)), $position, 0);

                break;
            default:
                $bytes[$position] = "\x00";
        }
    }

    return $bytes;
}

/**
 * @throws CassandraException
 */
function randomType(int $depth = 0): Cassandra\TypeInfo\TypeInfo {
    $scalars = [
        Type::ASCII, Type::BIGINT, Type::BLOB, Type::BOOLEAN, Type::COUNTER, Type::DATE, Type::DECIMAL,
        Type::DOUBLE, Type::DURATION, Type::FLOAT, Type::INET, Type::INT, Type::SMALLINT, Type::TEXT,
        Type::TIME, Type::TIMESTAMP, Type::TIMEUUID, Type::TINYINT, Type::UUID, Type::VARCHAR, Type::VARINT,
    ];

    $choice = $depth >= 2 ? 0 : mt_rand(0, 10);

    return match (true) {
        $choice === 6 => new ListCollectionInfo(randomType($depth + 1), isFrozen: false),
        $choice === 7 => new SetCollectionInfo(randomType($depth + 1), isFrozen: false),
        $choice === 8 => new MapCollectionInfo(randomType($depth + 1), randomType($depth + 1), isFrozen: false),
        $choice === 9 => new TupleInfo([randomType($depth + 1), randomType($depth + 1)]),
        $choice === 10 => new VectorInfo(randomType($depth + 1), mt_rand(1, 3)),
        default => new SimpleTypeInfo($scalars[array_rand($scalars)]),
    };
}

function cqlString(string $value): string {
    return pack('n', strlen($value)) . $value;
}

function rowsMetadata(): string {
    $columns = [[0x0009, ''], [0x000D, ''], [0x000C, ''], [0x000B, ''], [0x0020, pack('n', 0x0009)], [0x0022, pack('n', 0x000D)], [0x0021, pack('nn', 0x000D, 0x0009)]];
    $metadata = pack('NN', 1, count($columns)) . cqlString('ks') . cqlString('t');
    foreach ($columns as $i => [$type, $extra]) {
        $metadata .= cqlString('c' . $i) . pack('n', $type) . $extra;
    }

    return $metadata;
}

$list = pack('N', 2) . pack('N', 4) . pack('N', 1) . pack('N', 4) . pack('N', 2);
$set = pack('N', 1) . pack('N', 1) . 'a';
$map = pack('N', 1) . pack('N', 1) . 'k' . pack('N', 4) . pack('N', 7);
$row = pack('N', 4) . pack('N', 7) . pack('N', 3) . 'abc' . pack('N', 16) . randomBytes(16) . pack('N', 8) . pack('J', 1700000000000)
    . pack('N', strlen($list)) . $list . pack('N', strlen($set)) . $set . pack('N', strlen($map)) . $map;

$errorContexts = [
    0x1000 => pack('nNN', 1, 1, 1),
    0x1100 => pack('nNN', 1, 0, 1) . cqlString('SIMPLE'),
    0x1200 => pack('nNN', 1, 0, 1) . "\1",
    0x1300 => pack('nNNN', 1, 0, 1, 0) . "\0",
    0x1400 => cqlString('ks') . cqlString('fn') . pack('n', 1) . cqlString('int'),
    0x1500 => pack('nNNN', 1, 0, 1, 0) . cqlString('SIMPLE'),
    0x2400 => cqlString('ks') . cqlString('t'),
    0x2500 => pack('n', 2) . 'id',
];
$frameSeeds = [
    [Opcode::RESPONSE_RESULT, pack('N', 1)],
    [Opcode::RESPONSE_RESULT, pack('N', 2) . rowsMetadata() . pack('N', 2) . $row . $row],
    [Opcode::RESPONSE_RESULT, pack('N', 3) . cqlString('ks')],
    [Opcode::RESPONSE_RESULT, pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 1, 1, 1) . pack('n', 0) . cqlString('ks') . cqlString('t') . cqlString('m') . pack('n', 9) . pack('NN', 4, 0)],
    [Opcode::RESPONSE_RESULT, pack('N', 5) . cqlString('CREATED') . cqlString('TABLE') . cqlString('ks') . cqlString('t')],
    [Opcode::RESPONSE_EVENT, cqlString('STATUS_CHANGE') . cqlString('UP') . "\4\x7F\0\0\1" . pack('N', 9042)],
    [Opcode::RESPONSE_EVENT, cqlString('SCHEMA_CHANGE') . cqlString('DROPPED') . cqlString('KEYSPACE') . cqlString('ks')],
    [Opcode::RESPONSE_SUPPORTED, pack('n', 1) . cqlString('CQL_VERSION') . pack('n', 1) . cqlString('3.0.0')],
    [Opcode::RESPONSE_AUTH_SUCCESS, pack('N', 2) . 'ok'],
    [Opcode::RESPONSE_AUTH_CHALLENGE, pack('N', -1)],
    [Opcode::RESPONSE_AUTHENTICATE, cqlString('PasswordAuthenticator')],
];
foreach ($errorContexts as $code => $context) {
    $frameSeeds[] = [Opcode::RESPONSE_ERROR, pack('N', $code) . cqlString('error') . $context];
}

$createResponse = new ReflectionMethod(ResponseReader::class, 'createResponse');
$responseReader = new ResponseReader();
$versions = [ProtocolVersion::V3, ProtocolVersion::V4, ProtocolVersion::V5];

/**
 * @throws CassandraException
 */
function readWholeFrame(ReflectionMethod $createResponse, ResponseReader $reader, ProtocolVersion $version, int $flags, Opcode $opcode, string $body): void {
    /** @var Cassandra\Response\Response $response */
    $response = $createResponse->invoke($reader, new Header($version, $flags, 1, $opcode, strlen($body)), $body);
    if ($response instanceof RowsResult) {
        $response->fetchAll();
    } elseif ($response instanceof PreparedResult) {
        $response->getPreparedData();
    } elseif ($response instanceof Error) {
        $response->getException();
    } elseif ($response instanceof Event) {
        $response->getData();
    } elseif (method_exists($response, 'getData')) {
        $response->getData();
    }
}

// A seed that does not parse would leave the fuzzing stuck at its first bytes.
foreach ($frameSeeds as $i => [$opcode, $body]) {
    try {
        readWholeFrame($createResponse, $responseReader, ProtocolVersion::V4, 0, $opcode, $body);
    } catch (Throwable $e) {
        fprintf(STDERR, "seed %d (%s) does not parse: %s\n", $i, $opcode->name, $e->getMessage());

        exit(2);
    }
}

$readerCalls = [
    'readString', 'readLongString', 'readBytes', 'readShortBytes', 'readInet', 'readStringList',
    'readStringMap', 'readStringMultimap', 'readBytesMap', 'readTypeInfo',
];

$typeNames = [
    'org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)',
    'org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,3)',
    'org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.TimestampType)',
    'org.apache.cassandra.db.marshal.UserType(ks,7564,61:org.apache.cassandra.db.marshal.Int32Type)',
];

$lz4Inputs = [
    (string) base64_decode('BCJNGGBAgg0AAIBoZWxsbyB3b3JsZAoAAAAA', true),
    "\x11hello" . pack('V', 0),
];

$failures = [];

$record = static function (string $target, Throwable $e, string $input) use (&$failures): void {
    $key = get_class($e) . ' ' . $e->getFile() . ':' . $e->getLine();
    if (!isset($failures[$key])) {
        $failures[$key] = [$target, $e->getMessage(), bin2hex(substr($input, 0, 80)), 0];
    }
    $failures[$key][3]++;
};

$config = new ValueEncodeConfig();

for ($i = 0; $i < $iterations; $i++) {
    // Cells of every type, including wrong widths and nested collections.
    $type = randomType();
    $body = randomBytes(mt_rand(0, 40));
    $input = pack('N', mt_rand(0, 3) === 0 ? mt_rand(-2, 64) : strlen($body)) . mutate($body);

    try {
        (new StreamReader($input))->readValue($type, $config);
    } catch (CassandraException) {
    } catch (Throwable $e) {
        $record('readValue ' . $type->type->name, $e, $input);
    }

    // Wire-format structures.
    $call = $readerCalls[array_rand($readerCalls)];
    $input = mutate(randomBytes(mt_rand(0, 32)));

    try {
        (new StreamReader($input))->{$call}();
    } catch (CassandraException) {
    } catch (Throwable $e) {
        $record($call, $e, $input);
    }

    // Whole response frames, read the way a caller reads them.
    [$opcode, $body] = $frameSeeds[array_rand($frameSeeds)];
    $input = mt_rand(0, 9) === 0 ? mutate($body) : mutate(mutate($body));
    $flags = mt_rand(0, 7) === 0 ? mt_rand(0, 15) : 0;

    try {
        readWholeFrame($createResponse, $responseReader, $versions[array_rand($versions)], $flags, $opcode, $input);
    } catch (CassandraException) {
    } catch (Throwable $e) {
        $record('response ' . $opcode->name, $e, $input);
    }

    // Type names as a node sends them in custom types.
    $input = mutate($typeNames[array_rand($typeNames)]);

    try {
        (new TypeNameParser())->parse($input);
    } catch (CassandraException) {
    } catch (Throwable $e) {
        $record('TypeNameParser', $e, $input);
    }

    // LZ4 blocks and frames, on the pure-PHP path.
    $input = mutate($lz4Inputs[array_rand($lz4Inputs)]);

    try {
        $decompressor = new Lz4Decompressor(preferExtension: false);
        $decompressor->setInput($input);
        mt_rand(0, 1) === 0 ? $decompressor->decompress() : $decompressor->decompressBlock(256);
    } catch (CassandraException) {
    } catch (Throwable $e) {
        $record('Lz4Decompressor', $e, $input);
    }

    // Serialized values compared while sorting frozen collections.
    $left = mutate(randomBytes(mt_rand(0, 24)));
    $right = mutate(randomBytes(mt_rand(0, 24)));

    try {
        ValueComparator::compare($type, $left, $right);
    } catch (CassandraException) {
    } catch (Throwable $e) {
        $record('ValueComparator ' . $type->type->name, $e, $left);
    }
}

printf("%d iterations, seed %d: %d distinct failures\n", $iterations, $seed, count($failures));
foreach ($failures as $where => [$target, $message, $input, $count]) {
    printf("  %s\n    %dx %s: %s\n    input %s\n", $where, $count, $target, $message, $input);
}

exit($failures === [] ? 0 : 1);
