<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\FrameCodec;
use Cassandra\Connection\Handshake;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\RequestCompressor;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Request;
use Cassandra\Response\StreamReader;
use Cassandra\Response\Supported;

/**
 * Node that does nothing, for the parts of the handshake that decide rather
 * than the parts that talk: {@see Handshake} only reaches a node for its
 * config, and {@see Handshake::wrapNode()} only wraps it.
 */
final class HandshakeTestNode extends NodeImplementation {
    private NodeConfig $config;

    public function __construct() {
        $this->config = new SocketNodeConfig(host: '127.0.0.1', port: 9042);
    }

    #[\Override]
    public function close(): void {
    }

    #[\Override]
    public function getConfig(): NodeConfig {
        return $this->config;
    }

    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        return '';
    }

    #[\Override]
    public function write(string $data): void {
    }

    #[\Override]
    public function writeRequest(Request $request): void {
    }
}

/**
 * What a connection settles with a node before it can carry requests.
 *
 * The compression half of it is what most of this covers: SUPPORTED only says
 * which algorithms the node has, and the spelling it happens to use for one is
 * not something the two sides have agreed anywhere — Cassandra reads the name
 * back with equalsIgnoreCase — so a node that answers "LZ4" is offering the
 * algorithm this driver speaks.
 */
final class HandshakeNegotiationTest extends AbstractUnitTestCase {
    /**
     * The recorded name is this driver's own, whatever the node called it: it
     * is what {@see Handshake::wrapNode()} keys the codec on, and the codecs
     * accept exactly 'lz4'. A node's spelling reaching that far would fail the
     * connection just as refusing it outright did.
     */
    public function testCodecsAreBuiltForANodeThatSpellsTheAlgorithmDifferently(): void {
        $handshake = new Handshake(new ConnectionOptions(enableCompression: true));

        $negotiated = $handshake->negotiate(
            self::supported(['COMPRESSION' => ['LZ4']]),
            new HandshakeTestNode(),
            ProtocolVersion::V5,
        );

        $this->assertInstanceOf(
            FrameCodec::class,
            $handshake->wrapNode(new HandshakeTestNode(), ProtocolVersion::V5, $negotiated['startupOptions']),
            'v5 wraps the transport in the frame codec, which only accepts the lowercase name',
        );

        $this->assertInstanceOf(
            RequestCompressor::class,
            $handshake->wrapNode(new HandshakeTestNode(), ProtocolVersion::V4, $negotiated['startupOptions']),
            'up to v4 the request compressor does, and it only accepts the lowercase name',
        );
    }

    public function testCompressionIsDroppedWhenTheNodeOffersNone(): void {
        $negotiated = self::negotiate([]);

        $this->assertArrayNotHasKey(
            'COMPRESSION',
            $negotiated['startupOptions'],
            'a node whose SUPPORTED names no compression is talked to uncompressed rather than refused',
        );
    }

    /**
     * The regression this file exists for: the algorithms a node offers were
     * matched case-sensitively, so a node advertising anything but the exact
     * lowercase name failed the handshake over an algorithm both sides speak.
     */
    public function testCompressionIsMatchedRegardlessOfHowTheNodeSpellsIt(): void {
        foreach (['lz4', 'LZ4', 'Lz4', 'lZ4'] as $spelling) {
            $negotiated = self::negotiate(['COMPRESSION' => [$spelling]]);

            $this->assertSame(
                'lz4',
                $negotiated['startupOptions']['COMPRESSION'] ?? null,
                'a node offering "' . $spelling . '" offers lz4, and the name sent back is this driver\'s own',
            );
        }
    }

    public function testCompressionIsMatchedWhenTheNodeOffersSeveralAlgorithms(): void {
        $negotiated = self::negotiate(['COMPRESSION' => ['Snappy', 'LZ4']]);

        $this->assertSame('lz4', $negotiated['startupOptions']['COMPRESSION'] ?? null);
    }

    /**
     * The case-insensitive match must not turn into no match at all: a node
     * that really has none of this driver's algorithms is still a configuration
     * to correct.
     */
    public function testCompressionIsStillRefusedWhenTheNodeOffersNoneThisDriverSpeaks(): void {
        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_COMPRESSION_NOT_SUPPORTED->value);

        self::negotiate(['COMPRESSION' => ['snappy', 'DEFLATE']]);
    }

    /**
     * Nothing is asked of the node's compression list where the connection did
     * not ask for compression in the first place.
     */
    public function testNoCompressionIsRequestedWhenItIsNotEnabled(): void {
        $handshake = new Handshake(new ConnectionOptions(enableCompression: false));

        $negotiated = $handshake->negotiate(
            self::supported(['COMPRESSION' => ['LZ4']]),
            new HandshakeTestNode(),
            ProtocolVersion::V4,
        );

        $this->assertArrayNotHasKey('COMPRESSION', $negotiated['startupOptions']);

        $node = new HandshakeTestNode();
        $this->assertSame(
            $node,
            $handshake->wrapNode($node, ProtocolVersion::V4, $negotiated['startupOptions']),
            'an uncompressed v4 connection is not wrapped at all',
        );
    }

    /**
     * @param array<string, list<string>> $serverOptions
     * @return array{version: ProtocolVersion, startupOptions: array<string,string>}
     *
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     */
    private static function negotiate(array $serverOptions, ProtocolVersion $currentVersion = ProtocolVersion::V4): array {
        $handshake = new Handshake(new ConnectionOptions(enableCompression: true));

        return $handshake->negotiate(self::supported($serverOptions), new HandshakeTestNode(), $currentVersion);
    }

    /**
     * A SUPPORTED response carrying $multimap as its [string multimap] body.
     *
     * @param array<string, list<string>> $multimap
     *
     * @throws \Cassandra\Exception\ResponseException
     */
    private static function supported(array $multimap): Supported {
        $body = pack('n', count($multimap));

        foreach ($multimap as $key => $values) {
            $body .= pack('n', strlen($key)) . $key;
            $body .= pack('n', count($values));

            foreach ($values as $value) {
                $body .= pack('n', strlen($value)) . $value;
            }
        }

        return new Supported(
            new Header(
                version: ProtocolVersion::V4,
                flags: 0,
                stream: 0,
                opcode: Opcode::RESPONSE_SUPPORTED,
                length: strlen($body),
            ),
            new StreamReader($body),
        );
    }
}
