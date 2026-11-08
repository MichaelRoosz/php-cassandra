<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Request\Query;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionMethod;

/**
 * Regression: the keyspace was interpolated into "USE ...;" as given, so a name
 * with a semicolon ran CQL of its own.
 */
final class KeyspaceNameTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{string}>
     */
    public static function invalidNames(): array {
        return [
            'statement after it' => ['ks; DROP KEYSPACE other'],
            'space' => ['my ks'],
            'unbalanced quote' => ['"ks'],
            'unescaped inner quote' => ['"k"s"'],
            'empty quoted' => ['""'],
            'trailing newline' => ["ks\n"],
        ];
    }

    #[DataProvider('invalidNames')]
    public function testANameThatIsNotOneIdentifierIsRefused(string $keyspace): void {
        try {
            self::useKeyspaceQuery($keyspace);
            $this->fail('expected the keyspace name to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_INVALID_KEYSPACE_NAME->value, $e->getCode());
        }
    }

    public function testIdentifiersAreUsedAsGiven(): void {
        $this->assertSame('USE my_ks1;', self::useKeyspaceQuery('my_ks1')->getQuery());
        $this->assertSame('USE MyKs;', self::useKeyspaceQuery('MyKs')->getQuery());
        $this->assertSame('USE "My ""Ks""";', self::useKeyspaceQuery('"My ""Ks"""')->getQuery());
    }

    private static function useKeyspaceQuery(string $keyspace): Query {
        $query = (new ReflectionMethod(Connection::class, 'useKeyspaceQuery'))->invoke(null, $keyspace);
        self::assertInstanceOf(Query::class, $query);

        return $query;
    }
}
