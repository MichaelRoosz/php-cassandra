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
 * PREPARE and EXECUTE are answered in every mode but "deaf" and
 * "always-unprepared", so the modes below only describe what happens to QUERYs
 * unless they say otherwise. Every PREPARE is also reported on stdout as
 * "prepared <n>", which is how a test can tell whether the client prepared a
 * query it should have had cached.
 *
 * Modes:
 *   bad-response-header
 *                   answer the first QUERY with a frame whose header carries the
 *                   wrong protocol version, and every later one normally. The
 *                   nine header bytes are well-formed enough to be read and then
 *                   refused, so the body they announce is left on the wire — a
 *                   client that keeps the connection would read the next
 *                   response at the wrong offset
 *   idle            handshake, then never send anything unprompted
 *   slow-query      handshake, then answer every QUERY after [delaySeconds]
 *   defer-slow      answer QUERYs mentioning SLOW after [delaySeconds] and all
 *                   others at once, without blocking in between
 *   event           handshake, then push a STATUS_CHANGE event after [delaySeconds]
 *   deaf            handshake, then stop answering anything at all
 *   close-on-query  handshake, then hang up on the first QUERY instead of
 *                   answering it, so the client's read fails outright rather
 *                   than going quiet
 *   refuse-startup  answer OPTIONS, then never answer the STARTUP, so that the
 *                   handshake fails part way through
 *   always-unprepared
 *                   prepare happily, but answer every EXECUTE with UNPREPARED,
 *                   as a node that never keeps the prepared statement would
 *   refuse-use      report every QUERY on stdout as "query <cql>", and answer
 *                   the ones that switch keyspace with INVALID, as a node asked
 *                   for a keyspace that does not exist would
 *   event-then-reorder
 *                   on a QUERY mentioning SLOW, push an event at once and answer
 *                   that query after [delaySeconds]; answer every other QUERY
 *                   after twice that. So a client whose event listener issues a
 *                   request of its own is still inside that nested request's
 *                   wait when the first query's answer arrives, and it is the
 *                   nested read that takes it off the wire
 *   trickle-result  answer every QUERY with one large RESULT written in pieces
 *                   over [delaySeconds], as a node streaming a wide page over a
 *                   slow link does. Bytes arrive the whole time but no frame is
 *                   complete until the end, and — being busy writing — the
 *                   server reads nothing meanwhile, so anything sent to it in
 *                   between (a heartbeat OPTIONS above all) is answered only
 *                   afterwards, exactly as a SUPPORTED queued behind a big
 *                   response on the one socket would be
 */

declare(strict_types=1);

const OPCODE_ERROR = 0x00;
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
 * @param ?int $version the version byte to put in the header, for the modes that
 * send one the client is meant to refuse; the response bit (0x80) is set here.
 */
function writeFrame($client, int $stream, int $opcode, string $body, ?int $version = null): void {
    fwrite($client, pack('CCnCN', ($version ?? PROTOCOL_VERSION) | 0x80, 0, $stream, $opcode, strlen($body)) . $body);
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
 * A VOID RESULT padded out to a few megabytes, so that writing it takes long
 * enough to be trickled across a heartbeat timeout.
 *
 * The padding sits past the kind, where a VOID result has nothing more to read,
 * so the client parses it as an ordinary empty result — the point being how long
 * the frame takes to arrive, not what is in it.
 */
function largeVoidResultBody(): string {
    return voidResultBody() . str_repeat("\0", 4 * 1024 * 1024);
}

/**
 * Write one frame in equal pieces spread over $seconds, so that bytes keep
 * arriving for the whole time without a frame ever completing until the end.
 *
 * @param resource $client
 */
function trickleFrame($client, int $stream, int $opcode, string $body, float $seconds): void {
    $frame = pack('CCnCN', PROTOCOL_VERSION | 0x80, 0, $stream, $opcode, strlen($body)) . $body;

    $pieces = 40;
    $pieceLength = (int) ceil(strlen($frame) / $pieces);
    $pauseMicroseconds = (int) ($seconds * 1_000_000 / $pieces);

    for ($offset = 0; $offset < strlen($frame); $offset += $pieceLength) {
        $piece = substr($frame, $offset, $pieceLength);

        $written = 0;
        while ($written < strlen($piece)) {
            $result = fwrite($client, substr($piece, $written));
            if ($result === false) {
                return;
            }
            if ($result === 0) {
                usleep(2000);

                continue;
            }
            $written += $result;
        }

        fflush($client);
        usleep($pauseMicroseconds);
    }
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

/**
 * An UNPREPARED ERROR for the statement id the PREPARED result above hands out.
 *
 * A node that has forgotten a prepared statement answers its EXECUTE with this,
 * and the client is expected to prepare it again and re-execute. A node that
 * answers every re-execution the same way is what the client's repreparation
 * limit exists for.
 */
function unpreparedErrorBody(): string {
    return pack('N', 0x2500)                               // error code = UNPREPARED
        . cqlString('unprepared')                          // message [string]
        . pack('n', 4) . 'pid1';                           // unknown id [short bytes]
}

/**
 * An INVALID ERROR, which is how a node refuses a USE for a keyspace that does
 * not exist.
 */
function invalidErrorBody(string $message): string {
    return pack('N', 0x2200)                               // error code = INVALID
        . cqlString($message);                             // message [string]
}

/**
 * The CQL of a QUERY frame, whose body starts with the [long string] query.
 */
function queryCql(string $body): string {
    /** @var array{length: int} $parsed */
    $parsed = unpack('Nlength', $body);

    return substr($body, 4, $parsed['length']);
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
$badHeaderSent = false;
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
        // The client hung up. Wait for another one rather than exiting, so that
        // a test can disconnect and reconnect against the same server — which
        // is the only way to reach the states that are about one connection
        // having replaced another. Everything that belonged to the old client
        // goes with it: its stream ids mean nothing on the new connection, and
        // the handshake starts over.
        fclose($client);

        $client = stream_socket_accept($server, 20);
        if ($client === false) {
            break;
        }

        $handshakeDone = false;
        $eventDueAt = null;
        $deferredAnswers = [];

        continue;
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
            if ($mode === 'always-unprepared') {
                // Never keeps the prepared statement, so every re-execution is
                // refused the same way and only the client's own limit can end
                // the exchange.
                writeFrame($client, $frame['stream'], OPCODE_ERROR, unpreparedErrorBody());

                break;
            }

            writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

            break;

        case OPCODE_QUERY:
            if ($mode === 'bad-response-header' && !$badHeaderSent) {
                // Deliberately not $badHeaderSent per connection: the client is
                // expected to drop this one, and the next has to be served
                // normally for the recovery to be visible.
                $badHeaderSent = true;

                writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody(), PROTOCOL_VERSION - 1);

                break;
            }

            if ($mode === 'close-on-query') {
                // Hangs up on the query instead of answering it, so the
                // client's read fails as a transport failure rather than as a
                // timeout — which is what puts it down the node-failure path
                // instead of the deadline one.
                fclose($client);

                break 2;
            }

            if ($mode === 'refuse-use') {
                // Reported verbatim so that a test can pin how the client
                // spells the keyspace it is switching to.
                $cql = queryCql($frame['body']);
                fwrite(STDOUT, 'query ' . $cql . "\n");
                fflush(STDOUT);

                if (stripos($cql, 'USE ') === 0) {
                    writeFrame($client, $frame['stream'], OPCODE_ERROR, invalidErrorBody('Keyspace does not exist'));

                    break;
                }

                writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

                break;
            }

            if ($mode === 'event-then-reorder') {
                if (str_contains($frame['body'], 'SLOW')) {
                    // The event goes out first, so the client reads it while
                    // still waiting for this query — and whatever its listener
                    // does then is what the answer below arrives in the middle
                    // of.
                    $eventDueAt = microtime(true);
                    $deferredAnswers[] = ['dueAt' => microtime(true) + $delay, 'stream' => $frame['stream']];

                    break;
                }

                // Twice the delay, so a query the listener sends is answered
                // after the one that is already outstanding rather than before
                // it: the nested wait is still reading when the first answer
                // comes, and so is the one that reads it.
                $deferredAnswers[] = ['dueAt' => microtime(true) + 2 * $delay, 'stream' => $frame['stream']];

                break;
            }

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

            if ($mode === 'trickle-result') {
                trickleFrame($client, $frame['stream'], OPCODE_RESULT, largeVoidResultBody(), $delay);

                break;
            }

            if ($mode === 'slow-query' && $delay > 0.0) {
                usleep((int) ($delay * 1_000_000));
            }
            writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

            break;
    }
}
