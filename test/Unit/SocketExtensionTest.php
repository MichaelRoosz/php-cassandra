<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ExceptionCode;

final class SocketExtensionTest extends AbstractUnitTestCase {
    #[\PHPUnit\Framework\Attributes\RequiresPhpExtension('sockets')]
    public function testTheDefaultTimeoutsAreUnchanged(): void {
        $this->assertSame([
            SO_RCVTIMEO => SocketNodeConfig::DEFAULT_SO_RCVTIMEO,
            SO_SNDTIMEO => SocketNodeConfig::DEFAULT_SO_SNDTIMEO,
        ], (new SocketNodeConfig())->socketOptions);
    }
    public function testWithoutTheExtensionAProjectExceptionIsRaised(): void {
        // Regression: the default argument named SO_RCVTIMEO, so PHP raised an Error of its own.
        $script = 'require ' . var_export(__DIR__ . '/../../vendor/autoload.php', true) . ';'
            . 'try { new Cassandra\Connection\SocketNodeConfig(); echo "constructed"; }'
            . ' catch (Throwable $e) { echo get_class($e), " ", $e->getCode(); }';
        $output = (string) shell_exec(escapeshellarg(PHP_BINARY) . ' -n -r ' . escapeshellarg($script));

        if ($output === 'constructed') {
            $this->markTestSkipped('ext-sockets is compiled into this PHP binary');
        }

        $this->assertSame('Cassandra\Exception\SocketException ' . ExceptionCode::SOCKET_EXTENSION_NOT_LOADED->value, $output);
    }
}
