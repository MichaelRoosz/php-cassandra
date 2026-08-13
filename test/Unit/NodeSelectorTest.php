<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection\RoundRobinSelector;
use Cassandra\Connection\StreamNodeConfig;
use ReflectionProperty;

final class NodeSelectorTest extends AbstractUnitTestCase {
    public function testRoundRobinCounterWrapsWithoutOverflowing(): void {
        $selector = new RoundRobinSelector();
        $nodes = [
            new StreamNodeConfig(host: 'first'),
            new StreamNodeConfig(host: 'second'),
            new StreamNodeConfig(host: 'third'),
        ];

        (new ReflectionProperty(RoundRobinSelector::class, 'counter'))->setValue($selector, PHP_INT_MAX);

        $this->assertSame([$nodes[1], $nodes[2], $nodes[0]], $selector->order($nodes));
        $this->assertSame([$nodes[2], $nodes[0], $nodes[1]], $selector->order($nodes));
    }
}
