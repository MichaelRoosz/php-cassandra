<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Request\Options\QueryOptions;

/**
 * The per-request timeout must survive every way an options object gets copied.
 *
 * The copies are not optional bookkeeping: Connection::query() rewrites the
 * options through withKeyspace() on protocol v5+, and Request\Query rewrites
 * them through withNamesForValues() for associative values. A field dropped in
 * one of those clones would therefore vanish silently, and only on some
 * protocol versions or some call shapes.
 */
final class RequestOptionsTimeoutTest extends AbstractUnitTestCase {
    public function testBatchOptionsKeepTheTimeoutAcrossClones(): void {
        $options = new BatchOptions(requestTimeoutInSeconds: 12.5);

        $this->assertSame(12.5, $options->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withKeyspace('app')->requestTimeoutInSeconds);
    }

    public function testExecuteOptionsKeepTheTimeoutAcrossClones(): void {
        $options = new ExecuteOptions(requestTimeoutInSeconds: 12.5);

        $this->assertSame(12.5, $options->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withKeyspace('app')->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withNamesForValues(true)->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withPagingState('state')->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withSkipMetadata(true)->requestTimeoutInSeconds);
    }

    public function testExecuteOptionsKeepTheTimeoutWhenBuiltFromQueryOptions(): void {
        $queryOptions = new QueryOptions(requestTimeoutInSeconds: 12.5);

        $this->assertSame(12.5, ExecuteOptions::fromQueryOptions($queryOptions)->requestTimeoutInSeconds);
    }

    public function testPrepareOptionsKeepTheTimeoutAcrossClones(): void {
        $options = new PrepareOptions(requestTimeoutInSeconds: 12.5);

        $this->assertSame(12.5, $options->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withKeyspace('app')->requestTimeoutInSeconds);
    }

    public function testQueryOptionsKeepTheTimeoutAcrossClones(): void {
        $options = new QueryOptions(requestTimeoutInSeconds: 12.5);

        $this->assertSame(12.5, $options->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withKeyspace('app')->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withNamesForValues(true)->requestTimeoutInSeconds);
        $this->assertSame(12.5, $options->withPagingState('state')->requestTimeoutInSeconds);
    }

    public function testTimeoutIsNullByDefault(): void {
        $this->assertNull((new QueryOptions())->requestTimeoutInSeconds);
        $this->assertNull((new ExecuteOptions())->requestTimeoutInSeconds);
        $this->assertNull((new BatchOptions())->requestTimeoutInSeconds);
        $this->assertNull((new PrepareOptions())->requestTimeoutInSeconds);
    }
}
