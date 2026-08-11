<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Supported;

/**
 * What a connection has to agree with a node before it can carry requests:
 * which protocol version both sides speak, which options the STARTUP frame
 * announces, and which framing the transport is wrapped in once the node has
 * accepted them.
 *
 * The exchange itself — OPTIONS, STARTUP and, where the node asks for it,
 * authentication — is driven by {@see Session::completeHandshake()}, which is
 * the only thing that can send a request on the connection being set up. What
 * is here is the part that decides rather than the part that waits.
 */
final class Handshake {
    private ConnectionOptions $options;

    public function __construct(ConnectionOptions $options) {
        $this->options = $options;
    }

    /**
     * Settle the protocol version and the STARTUP options against what the node
     * says it supports.
     *
     * $currentVersion is what the connection has been speaking so far, and is
     * what the negotiated version falls back to for a node that does not
     * advertise its versions at all.
     *
     * @return array{
     *  version: ProtocolVersion,
     *  startupOptions: array<string,string>
     * }
     *
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     */
    public function negotiate(Supported $supportedResponse, Node $node, ProtocolVersion $currentVersion): array {

        $serverOptions = $supportedResponse->getData();

        // configure protocol version
        if (!isset($serverOptions['PROTOCOL_VERSIONS'])) {
            $versionsSupportedByServer = [$currentVersion];
        } else {
            $versionsSupportedByServer = [];

            foreach ($serverOptions['PROTOCOL_VERSIONS'] as $versionString) {
                $version = ProtocolVersion::fromOptionFormat($versionString);
                if ($version !== null) {
                    $versionsSupportedByServer[] = $version;
                }
            }
        }

        $protocolVersion = ProtocolVersion::getHighestSupportedVersion($versionsSupportedByServer, $this->options->allowedProtocolVersions);
        if ($protocolVersion === null) {

            $versionsSupportedByServerInOptionFormat = array_map(
                fn (ProtocolVersion $v) => $v->inOptionFormat(),
                $versionsSupportedByServer
            );

            $allowedProtocolVersionsInOptionFormat = array_map(
                fn (ProtocolVersion $v) => $v->inOptionFormat(),
                $this->options->allowedProtocolVersions
            );

            throw new ConnectionException('Server does not support a compatible protocol version.', ExceptionCode::CONNECTION_SERVER_PROTOCOL_UNSUPPORTED->value, [
                'protocol_versions_supported_by_server' => $versionsSupportedByServerInOptionFormat,
                'protocol_versions_supported_by_client' => ProtocolVersion::CASES_IN_OPTION_FORMAT,
                'protocol_versions_allowed_by_connection_options' => $allowedProtocolVersionsInOptionFormat,
            ]);
        }

        // configure startup options
        $startupOptions = $this->options->asStartupOptions();

        if (isset($startupOptions['COMPRESSION']) && $startupOptions['COMPRESSION']
            && isset($serverOptions['COMPRESSION']) && $serverOptions['COMPRESSION']
        ) {
            $compressionAlgo = strtolower($startupOptions['COMPRESSION']);

            if (!in_array($compressionAlgo, $serverOptions['COMPRESSION'], true)) {
                $nodeConfig = $node->getConfig();

                throw new ConnectionException('Compression "' . $compressionAlgo . '" not supported by server.', ExceptionCode::CONNECTION_COMPRESSION_NOT_SUPPORTED->value, [
                    'host' => $nodeConfig->host,
                    'port' => $nodeConfig->port,
                    'compression' => $compressionAlgo,
                    'server_supported' => $serverOptions['COMPRESSION'],
                ]);
            }

            $startupOptions['COMPRESSION'] = $compressionAlgo;
        } else {
            unset($startupOptions['COMPRESSION']);
        }

        if ($protocolVersion->value >= ProtocolVersion::V4->value) {
            if ($this->options->throwOnOverload) {
                $startupOptions['THROW_ON_OVERLOAD'] = '1';
            } else {
                $startupOptions['THROW_ON_OVERLOAD'] = '0';
            }
        } else {
            unset($startupOptions['THROW_ON_OVERLOAD']);
        }

        if ($protocolVersion->value < ProtocolVersion::V5->value) {
            unset($startupOptions['DRIVER_NAME']);
            unset($startupOptions['DRIVER_VERSION']);
        }

        return [
            'version' => $protocolVersion,
            'startupOptions' => $startupOptions,
        ];
    }

    /**
     * Put the framing the negotiated options call for around the transport,
     * once the node has accepted the STARTUP.
     *
     * From v5 every frame is wrapped by {@see FrameCodec}, which carries the
     * compression itself; before that, compression is applied to the request
     * body alone.
     *
     * @param array<string,string> $startupOptions
     *
     * @throws \Cassandra\Exception\NodeException
     */
    public function wrapNode(Node $node, ProtocolVersion $version, array $startupOptions): Node {

        if ($version->value >= ProtocolVersion::V5->value) {
            return new FrameCodec($node, $startupOptions['COMPRESSION'] ?? '');
        }

        if (isset($startupOptions['COMPRESSION']) && $startupOptions['COMPRESSION'] !== '') {
            return new RequestCompressor($node, $startupOptions['COMPRESSION']);
        }

        return $node;
    }
}
