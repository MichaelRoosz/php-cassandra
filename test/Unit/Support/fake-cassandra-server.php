<?php

/**
 * A minimal CQL v4 server, just complete enough to let a real Connection
 * handshake and then behave in the ways timeouts care about: answering slowly,
 * staying silent, or pushing an event.
 *
 * Usage: php fake-cassandra-server.php <mode> [delaySeconds]
 *
 * Listens on a free port of 127.0.0.1 and prints "ready <port>" once it does.
 *
 * PREPARE and EXECUTE are answered in every mode but "deaf", so the modes below
 * only describe what happens to QUERYs. Every PREPARE is also reported on
 * stdout as "prepared <n>", which is how a test can tell whether the client
 * prepared a query it should have had cached.
 *
 * Modes:
 *   idle            handshake, then never send anything unprompted
 *   slow-query      handshake, then answer every QUERY after [delaySeconds]
 *   defer-slow      answer QUERYs mentioning SLOW after [delaySeconds] and all
 *                   others at once, without blocking in between
 *   event           handshake, then push a STATUS_CHANGE event after [delaySeconds]
 *   deaf            handshake, then stop answering anything at all
 *   refuse-startup  answer OPTIONS, then never answer the STARTUP, so that the
 *                   handshake fails part way through
 */

declare(strict_types=1);

const OPCODE_STARTUP = 0x01;
const OPCODE_READY = 0x02;
const OPCODE_OPTIONS = 0x05;
const OPCODE_SUPPORTED = 0x06;
const OPCODE_QUERY = 0x07;
const OPCODE_RESULT = 0x08;
const OPCODE_PREPARE = 0x09;
const OPCODE_EXECUTE = 0x0A;
const OPCODE_REGISTER = 0x0B;
const OPCODE_EVENT = 0x0C;

const PROTOCOL_VERSION = 0x04;

/**
 * @param resource $client
 * @return array{stream: int, opcode: int, body: string}|null
 */
function readFrame($client): ?array {
    $header = '';
    while (strlen($header) < 9) {
        $chunk = fread($client, max(1, 9 - strlen($header)));
        if ($chunk === false || ($chunk === '' && feof($client))) {
            return null;
        }
        $header .= $chunk;
    }

    /** @var array{stream: int, opcode: int, length: int} $parsed */
    $parsed = unpack('Cversion/Cflags/nstream/Copcode/Nlength', $header);

    $body = '';
    while (strlen($body) < $parsed['length']) {
        $chunk = fread($client, max(1, $parsed['length'] - strlen($body)));
        if ($chunk === false || ($chunk === '' && feof($client))) {
            return null;
        }
        $body .= $chunk;
    }

    return ['stream' => $parsed['stream'], 'opcode' => $parsed['opcode'], 'body' => $body];
}

/**
 * @param resource $client
 */
function writeFrame($client, int $stream, int $opcode, string $body): void {
    fwrite($client, pack('CCnCN', PROTOCOL_VERSION | 0x80, 0, $stream, $opcode, strlen($body)) . $body);
    fflush($client);
}

function cqlString(string $value): string {
    return pack('n', strlen($value)) . $value;
}

/** An empty [string multimap], which advertises no options at all. */
function supportedBody(): string {
    return pack('n', 0);
}

/** A void RESULT. */
function voidResultBody(): string {
    return pack('N', 1);
}

/**
 * A PREPARED RESULT for a statement with a single int bind marker named "id".
 *
 * Enough for the client to encode an EXECUTE against it, which is all the
 * auto-prepare paths need; the rows metadata is left out entirely.
 */
function preparedResultBody(): string {
    return pack('N', 4)                                    // kind = PREPARED
        . pack('n', 4) . 'pid1'                            // id [short bytes]
        // prepare metadata: GLOBAL_TABLES_SPEC, one bind marker, no pk index
        . pack('N', 1) . pack('N', 1) . pack('N', 0)
        . cqlString('ks') . cqlString('t')
        . cqlString('id') . pack('n', 0x0009)              // marker "id", type int
        // rows metadata: NO_METADATA, no columns
        . pack('N', 4) . pack('N', 0);
}

/** STATUS_CHANGE / UP for 127.0.0.1:9042. */
function statusChangeEventBody(): string {
    return cqlString('STATUS_CHANGE')
        . cqlString('UP')
        . chr(4) . pack('C4', 127, 0, 0, 1) . pack('N', 9042);
}

$mode = (string) ($argv[1] ?? 'idle');
$delay = (float) ($argv[2] ?? 0.0);

// Port 0 lets the OS pick a free one, which is then reported back on stdout.
// Choosing a port ourselves would risk colliding with a server left over from
// another test and silently talking to the wrong one.
$server = stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
if ($server === false) {
    fwrite(STDERR, "listen failed: $errorMessage\n");

    exit(1);
}

$localName = stream_socket_get_name($server, false);
if ($localName === false) {
    fwrite(STDERR, "could not determine the listening port\n");

    exit(1);
}

fwrite(STDOUT, 'ready ' . substr($localName, (int) strrpos($localName, ':') + 1) . "\n");

$client = stream_socket_accept($server, 20);
if ($client === false) {
    exit(1);
}

$handshakeDone = false;
$eventDueAt = null;
$prepareCount = 0;
$deadline = microtime(true) + 60;

/** @var list<array{dueAt: float, stream: int}> $deferredAnswers */
$deferredAnswers = [];

while (microtime(true) < $deadline) {

    foreach ($deferredAnswers as $index => $deferred) {
        if (microtime(true) >= $deferred['dueAt']) {
            writeFrame($client, $deferred['stream'], OPCODE_RESULT, voidResultBody());
            unset($deferredAnswers[$index]);
        }
    }

    if ($eventDueAt !== null && microtime(true) >= $eventDueAt) {
        // Events are server-initiated and carry stream id -1.
        writeFrame($client, 0xFFFF, OPCODE_EVENT, statusChangeEventBody());
        $eventDueAt = null;
    }

    $read = [$client];
    $write = null;
    $except = null;
    if (stream_select($read, $write, $except, 0, 50000) < 1) {
        continue;
    }

    $frame = readFrame($client);
    if ($frame === null) {
        break;
    }

    // The handshake is always answered, so that the connection under test is
    // fully established before the mode's behaviour kicks in.
    if (!$handshakeDone) {
        if ($frame['opcode'] === OPCODE_OPTIONS) {
            writeFrame($client, $frame['stream'], OPCODE_SUPPORTED, supportedBody());

            continue;
        }

        if ($frame['opcode'] === OPCODE_STARTUP) {
            if ($mode === 'refuse-startup') {
                // Leaves the client waiting out its request timeout with the
                // socket open but the handshake unfinished.
                continue;
            }

            writeFrame($client, $frame['stream'], OPCODE_READY, '');
            $handshakeDone = true;

            if ($mode === 'event') {
                $eventDueAt = microtime(true) + $delay;
            }

            continue;
        }
    }

    if ($mode === 'deaf') {
        continue;
    }

    switch ($frame['opcode']) {
        case OPCODE_OPTIONS:
            // Also the heartbeat: answering it proves the connection is alive.
            writeFrame($client, $frame['stream'], OPCODE_SUPPORTED, supportedBody());

            break;

        case OPCODE_REGISTER:
            writeFrame($client, $frame['stream'], OPCODE_READY, '');

            break;

        case OPCODE_PREPARE:
            // Reported so that a test can tell how often the client prepared,
            // which is what the prepared-result cache is meant to reduce.
            $prepareCount++;
            fwrite(STDOUT, 'prepared ' . $prepareCount . "\n");
            fflush(STDOUT);

            writeFrame($client, $frame['stream'], OPCODE_RESULT, preparedResultBody());

            break;

        case OPCODE_EXECUTE:
            writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

            break;

        case OPCODE_QUERY:
            if ($mode === 'defer-slow') {
                // Answer queries mentioning SLOW only after the delay, without
                // blocking, so that other queries on the same connection are
                // still served in the meantime.
                if (str_contains($frame['body'], 'SLOW')) {
                    $deferredAnswers[] = ['dueAt' => microtime(true) + $delay, 'stream' => $frame['stream']];

                    break;
                }

                writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

                break;
            }

            if ($mode === 'slow-query' && $delay > 0.0) {
                usleep((int) ($delay * 1_000_000));
            }
            writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

            break;
    }
}
