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
 *   bad-result-kind answer the first QUERY with a complete RESULT frame whose
 *                   result-kind field is unknown, and every later query
 *                   normally. The bad answer is fully consumed before it is
 *                   rejected, so the connection remains frame-aligned
 *   bad-response-opcode
 *                   answer the first QUERY with a complete frame carrying a
 *                   request-only opcode, and every later query normally. This
 *                   reaches the connection-level malformed-response path
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
 *   authenticate-zero
 *                   require a password handshake and only accept username
 *                   "0" with password "0"
 *   always-unprepared
 *                   prepare happily, but answer every EXECUTE and every BATCH
 *                   with UNPREPARED, as a node that never keeps the prepared
 *                   statement would. A BATCH is refused by naming the first
 *                   prepared statement id it actually carries, which is what a
 *                   real node does and what makes the client's repreparation
 *                   visible: the id changes as the batch is patched
 *   unprepared-batch-once
 *                   hand out a fresh statement id for every PREPARE ("pid1",
 *                   "pid2", ...), refuse the first BATCH with UNPREPARED naming
 *                   the id it carries, and answer every later one normally. So
 *                   a client that recovers has to prepare the statement again
 *                   and put the new id into the batch before re-sending it.
 *                   Every BATCH is reported on stdout as "batch <ids>", which
 *                   is how a test can tell which ids it was sent with
 *   unprepared-batch-unknown-id
 *                   answer every BATCH with UNPREPARED naming a statement id the
 *                   batch does not carry, as a node and a client that disagree
 *                   about what was sent would. There is nothing for the client
 *                   to prepare again, so it has to report that rather than
 *                   guess
 *   unprepared-execute-retyped
 *                   refuse the first EXECUTE with UNPREPARED naming the id it
 *                   carries, and hand out a statement whose bind marker is a
 *                   bigint where the refused one's was an int — as a node whose
 *                   table was altered under a prepared statement would. So a
 *                   client that recovers has to encode the values it was given
 *                   against the marker types it has just been told about, not
 *                   the ones the refused statement carried. Every EXECUTE is
 *                   reported on stdout as "execute <id> <hex values>", which is
 *                   how a test can tell which encoding it was sent with
 *   unprepared-second-page
 *                   answer the first EXECUTE with a page of rows that has more
 *                   pages to come, then refuse the EXECUTE that asks for the
 *                   next page with UNPREPARED naming the id it carries, and
 *                   answer the one after that with the final page — as a node
 *                   that forgets a prepared statement half way through a result
 *                   set does. Each PREPARE hands out a fresh id, so a client
 *                   that recovers has to prepare the statement again and ask
 *                   for the same page with the new id. Every EXECUTE is
 *                   reported on stdout as "execute <id> <paging state>", which
 *                   is how a test can tell which page each one asked for
 *   repeat-paging-state
 *                   answer every QUERY and EXECUTE with a page carrying the same
 *                   non-null paging state. A client that does not detect the cycle
 *                   asks for and retains that page forever
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
 *   event-then-bad-result
 *                   the same ordering, except that the answer to the SLOW query
 *                   is a complete RESULT frame whose result-kind field is
 *                   unknown. So the nested read consumes and then refuses a
 *                   frame belonging to the wait it is nested inside — an id that
 *                   wait still owns, and a failure only that wait can report
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
const OPCODE_AUTHENTICATE = 0x03;
const OPCODE_OPTIONS = 0x05;
const OPCODE_SUPPORTED = 0x06;
const OPCODE_QUERY = 0x07;
const OPCODE_RESULT = 0x08;
const OPCODE_PREPARE = 0x09;
const OPCODE_EXECUTE = 0x0A;
const OPCODE_REGISTER = 0x0B;
const OPCODE_EVENT = 0x0C;
const OPCODE_BATCH = 0x0D;
const OPCODE_AUTH_RESPONSE = 0x0F;
const OPCODE_AUTH_SUCCESS = 0x10;

const PROTOCOL_VERSION = 0x04;

/**
 * How far a [bytes] length read with unpack('N') has to be shifted to bring its
 * sign bit into the sign bit of a PHP int, and back again to extend it. Zero on
 * a 32-bit build, where unpack('N') already hands back the negative.
 */
const SIGNED_INT_SHIFT_BIT_SIZE = (PHP_INT_SIZE * 8) - 32;

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
 * A PREPARED RESULT for a statement with a single bind marker named "id",
 * of type int unless another type id is asked for.
 *
 * Enough for the client to encode an EXECUTE against it, which is all the
 * auto-prepare paths need; the rows metadata is left out entirely.
 *
 * $markerType is what lets a mode hand out a statement whose marker types have
 * moved, which is one of the reasons a real node stops recognising a statement
 * id — an ALTER, or a table dropped and created again. The client has to encode
 * the values against the type it was just told about rather than the one the
 * refused statement carried.
 */
function preparedResultBody(string $id = 'pid1', int $markerType = 0x0009): string {
    return pack('N', 4)                                    // kind = PREPARED
        . pack('n', strlen($id)) . $id                     // id [short bytes]
        // prepare metadata: GLOBAL_TABLES_SPEC, one bind marker, no pk index
        . pack('N', 1) . pack('N', 1) . pack('N', 0)
        . cqlString('ks') . cqlString('t')
        . cqlString('id') . pack('n', $markerType)         // marker "id"
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
function unpreparedErrorBody(string $id = 'pid1'): string {
    return pack('N', 0x2500)                               // error code = UNPREPARED
        . cqlString('unprepared')                          // message [string]
        . pack('n', strlen($id)) . $id;                    // unknown id [short bytes]
}

/**
 * The prepared statement ids a BATCH body carries, in the order they appear.
 *
 * A batch entry is a kind byte followed by either a statement id ([short bytes],
 * kind 1) or a query ([long string], kind 0), and then that entry's values. The
 * values have to be walked even though nothing here looks at them, because they
 * are what separates one entry from the next.
 *
 * Needed because a real node names the statement it did not recognise, and the
 * client is entitled to be told about one the batch actually holds — which,
 * after a repreparation, is no longer the id it was built with.
 *
 * @return list<string>
 */
function batchPreparedIds(string $body): array {

    $ids = [];
    $offset = 1; // batch type

    /** @var array{1: int} $unpacked */
    $unpacked = unpack('n', substr($body, $offset, 2));
    $entryCount = $unpacked[1];
    $offset += 2;

    for ($entry = 0; $entry < $entryCount; $entry++) {
        $kind = ord($body[$offset]);
        $offset += 1;

        if ($kind === 1) {
            /** @var array{1: int} $unpacked */
            $unpacked = unpack('n', substr($body, $offset, 2));
            $offset += 2;
            $ids[] = substr($body, $offset, $unpacked[1]);
            $offset += $unpacked[1];
        } else {
            /** @var array{1: int} $unpacked */
            $unpacked = unpack('N', substr($body, $offset, 4));
            $offset += 4 + $unpacked[1];
        }

        /** @var array{1: int} $unpacked */
        $unpacked = unpack('n', substr($body, $offset, 2));
        $valueCount = $unpacked[1];
        $offset += 2;

        for ($value = 0; $value < $valueCount; $value++) {
            /** @var array{1: int} $unpacked */
            $unpacked = unpack('N', substr($body, $offset, 4));
            $offset += 4;

            // null (-1) and "not set" (-2) are lengths with no bytes behind
            // them, and unpack('N') hands them back unsigned.
            $length = $unpacked[1] << SIGNED_INT_SHIFT_BIT_SIZE >> SIGNED_INT_SHIFT_BIT_SIZE;
            if ($length > 0) {
                $offset += $length;
            }
        }
    }

    return $ids;
}

/**
 * The statement id, the raw bound values and the paging state an EXECUTE frame
 * carries.
 *
 * The body is the statement id as [short bytes], then the query parameters: a
 * [short] consistency, a one-byte flag set, and then the fields the flags
 * announce, in the order the protocol lists them — values, page size, paging
 * state. Everything before a field of interest has to be walked to reach it.
 *
 * Reported so that a test can pin what the client actually put on the wire,
 * which is the whole of what re-encoding after a repreparation changes: the
 * same value bound to an int marker and to a bigint one is four bytes and eight.
 * The paging state is what says which page an EXECUTE asked for, which is how a
 * repreparation half way through a result set can be told from one that started
 * the result set over.
 *
 * @return array{id: string, values: list<string>, pagingState: ?string}
 */
function executeIdAndValues(string $body): array {

    /** @var array{1: int} $unpacked */
    $unpacked = unpack('n', substr($body, 0, 2));
    $id = substr($body, 2, $unpacked[1]);
    $offset = 2 + $unpacked[1];

    $offset += 2; // consistency [short]

    $flags = ord($body[$offset]);
    $offset += 1;

    $values = [];

    if (($flags & 0x01) !== 0) { // VALUES
        /** @var array{1: int} $unpacked */
        $unpacked = unpack('n', substr($body, $offset, 2));
        $valueCount = $unpacked[1];
        $offset += 2;

        for ($value = 0; $value < $valueCount; $value++) {
            /** @var array{1: int} $unpacked */
            $unpacked = unpack('N', substr($body, $offset, 4));
            $offset += 4;

            // null (-1) and "not set" (-2) are lengths with no bytes behind
            // them, and unpack('N') hands them back unsigned.
            $length = $unpacked[1] << SIGNED_INT_SHIFT_BIT_SIZE >> SIGNED_INT_SHIFT_BIT_SIZE;
            if ($length < 0) {
                $values[] = '';

                continue;
            }

            $values[] = substr($body, $offset, $length);
            $offset += $length;
        }
    }

    if (($flags & 0x04) !== 0) { // PAGE_SIZE
        $offset += 4;
    }

    $pagingState = null;

    if (($flags & 0x08) !== 0) { // WITH_PAGING_STATE
        /** @var array{1: int} $unpacked */
        $unpacked = unpack('N', substr($body, $offset, 4));
        $offset += 4;

        $pagingState = substr($body, $offset, $unpacked[1]);
    }

    return ['id' => $id, 'values' => $values, 'pagingState' => $pagingState];
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
 * A ROWS RESULT carrying a single int column "id" and one row.
 *
 * $pagingState marks this as one page of several: a client walking the result
 * set sends it back on the next EXECUTE, which is what makes the second and
 * later pages of a paged statement reachable at all.
 */
function rowsResultBody(int $value, ?string $pagingState = null): string {

    $flags = 0x01;                                         // GLOBAL_TABLES_SPEC
    $pagingStateField = '';

    if ($pagingState !== null) {
        $flags |= 0x02;                                    // HAS_MORE_PAGES
        $pagingStateField = pack('N', strlen($pagingState)) . $pagingState;
    }

    return pack('N', 2)                                    // kind = ROWS
        . pack('N', $flags) . pack('N', 1)                 // flags, one column
        . $pagingStateField                                // paging state [bytes]
        . cqlString('ks') . cqlString('t')                 // global table spec
        . cqlString('id') . pack('n', 0x0009)              // column "id", int
        . pack('N', 1)                                     // one row
        . pack('N', 4) . pack('N', $value);                // its only cell
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
$badResponseSent = false;
$batchRefused = false;
$executeRefused = false;
$deadline = microtime(true) + 60;

/** @var list<array{dueAt: float, stream: int, body?: string}> $deferredAnswers */
$deferredAnswers = [];

while (microtime(true) < $deadline) {

    foreach ($deferredAnswers as $index => $deferred) {
        if (microtime(true) >= $deferred['dueAt']) {
            writeFrame($client, $deferred['stream'], OPCODE_RESULT, $deferred['body'] ?? voidResultBody());
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

            if ($mode === 'authenticate-zero') {
                writeFrame($client, $frame['stream'], OPCODE_AUTHENTICATE, cqlString('PasswordAuthenticator'));

                continue;
            }

            writeFrame($client, $frame['stream'], OPCODE_READY, '');
            $handshakeDone = true;

            if ($mode === 'event') {
                $eventDueAt = microtime(true) + $delay;
            }

            continue;
        }

        if ($frame['opcode'] === OPCODE_AUTH_RESPONSE && $mode === 'authenticate-zero') {
            if ($frame['body'] !== pack('N', 4) . "\0" . '0' . "\0" . '0') {
                fclose($client);

                break;
            }

            writeFrame($client, $frame['stream'], OPCODE_AUTH_SUCCESS, pack('N', 0));
            $handshakeDone = true;

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

            // A fresh id per PREPARE in the modes that refuse a prepared
            // statement, so that a test can tell the statement before a
            // repreparation from the one after it — and so that distinct
            // queries get distinct ids, as a real node gives them. The other
            // modes keep the single id their expectations are written against.
            if ($mode === 'unprepared-execute-retyped') {
                // The statement comes back with a bigint marker where it had an
                // int one, as it would after the table was altered under it.
                writeFrame($client, $frame['stream'], OPCODE_RESULT, preparedResultBody(
                    'pid' . $prepareCount,
                    $prepareCount === 1 ? 0x0009 : 0x0002,
                ));

                break;
            }

            writeFrame($client, $frame['stream'], OPCODE_RESULT, preparedResultBody(
                in_array($mode, ['unprepared-batch-once', 'always-unprepared', 'unprepared-second-page'], true)
                    ? 'pid' . $prepareCount
                    : 'pid1'
            ));

            break;

        case OPCODE_BATCH:
            $batchIds = batchPreparedIds($frame['body']);

            // Reported so that a test can pin which statement ids the batch
            // was sent with, which is the whole of what a repreparation
            // changes about it.
            fwrite(STDOUT, 'batch ' . implode(',', $batchIds) . "\n");
            fflush(STDOUT);

            if ($mode === 'unprepared-batch-unknown-id') {
                writeFrame($client, $frame['stream'], OPCODE_ERROR, unpreparedErrorBody('nosuch'));

                break;
            }

            if ($batchIds !== [] && ($mode === 'always-unprepared' || ($mode === 'unprepared-batch-once' && !$batchRefused))) {
                $batchRefused = true;

                writeFrame($client, $frame['stream'], OPCODE_ERROR, unpreparedErrorBody($batchIds[0]));

                break;
            }

            writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

            break;

        case OPCODE_EXECUTE:
            if ($mode === 'repeat-paging-state') {
                writeFrame($client, $frame['stream'], OPCODE_RESULT, rowsResultBody(1, 'same-page'));

                break;
            }

            if ($mode === 'unprepared-second-page') {
                $execute = executeIdAndValues($frame['body']);

                // Reported so that a test can pin which statement id each page
                // went out with, and which page each one asked for.
                fwrite(STDOUT, 'execute ' . $execute['id'] . ' ' . ($execute['pagingState'] ?? '-') . "\n");
                fflush(STDOUT);

                if ($execute['pagingState'] === null) {
                    // The first page, which every run gets: it is the page
                    // after it that this mode is about.
                    writeFrame($client, $frame['stream'], OPCODE_RESULT, rowsResultBody(1, 'page2'));

                    break;
                }

                if (!$executeRefused) {
                    // The statement was forgotten between two pages, as an
                    // ALTER or a restarted coordinator does it.
                    $executeRefused = true;

                    writeFrame($client, $frame['stream'], OPCODE_ERROR, unpreparedErrorBody($execute['id']));

                    break;
                }

                writeFrame($client, $frame['stream'], OPCODE_RESULT, rowsResultBody(2));

                break;
            }

            if ($mode === 'unprepared-execute-retyped') {
                $execute = executeIdAndValues($frame['body']);

                // Reported so that a test can pin the encoding each EXECUTE
                // went out with, before and after the repreparation.
                fwrite(STDOUT, 'execute ' . $execute['id'] . ' ' . implode(',', array_map('bin2hex', $execute['values'])) . "\n");
                fflush(STDOUT);

                if (!$executeRefused) {
                    $executeRefused = true;

                    writeFrame($client, $frame['stream'], OPCODE_ERROR, unpreparedErrorBody($execute['id']));

                    break;
                }

                writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody());

                break;
            }

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
            if ($mode === 'repeat-paging-state') {
                writeFrame($client, $frame['stream'], OPCODE_RESULT, rowsResultBody(1, 'same-page'));

                break;
            }

            if ($mode === 'bad-response-header' && !$badHeaderSent) {
                // Deliberately not $badHeaderSent per connection: the client is
                // expected to drop this one, and the next has to be served
                // normally for the recovery to be visible.
                $badHeaderSent = true;

                writeFrame($client, $frame['stream'], OPCODE_RESULT, voidResultBody(), PROTOCOL_VERSION - 1);

                break;
            }

            if ($mode === 'bad-result-kind' && !$badResponseSent) {
                $badResponseSent = true;

                writeFrame($client, $frame['stream'], OPCODE_RESULT, pack('N', 0x7FFFFFFF));

                break;
            }

            if ($mode === 'bad-response-opcode' && !$badResponseSent) {
                $badResponseSent = true;

                writeFrame($client, $frame['stream'], OPCODE_REGISTER, '');

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

            if ($mode === 'event-then-bad-result') {
                if (str_contains($frame['body'], 'SLOW')) {
                    // As in event-then-reorder, but the answer this query
                    // eventually gets is one the client cannot decode, so the
                    // nested read is the one that has to refuse a frame
                    // belonging to the wait it is nested inside.
                    $eventDueAt = microtime(true);
                    $deferredAnswers[] = [
                        'dueAt' => microtime(true) + $delay,
                        'stream' => $frame['stream'],
                        'body' => pack('N', 0x7FFFFFFF),
                    ];

                    break;
                }

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
