<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Exception\ServerException;
use Cassandra\Response\Result\SchemaChangeResult;
use Cassandra\Type;
use Cassandra\Value;

/**
 * Integration tests for secondary indexes, including the collection-specific
 * index targets and their query predicates.
 *
 * See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useSecondaryIndex.html
 */
final class SecondaryIndexTest extends AbstractIntegrationTestCase {
    public function testCreateAndDropIndex(): void {
        $created = $this->connection->query(
            "CREATE INDEX IF NOT EXISTS idx_temp ON {$this->keyspace}.index_riders (age)"
        );
        $this->assertInstanceOf(SchemaChangeResult::class, $created);

        $dropped = $this->connection->query("DROP INDEX IF EXISTS {$this->keyspace}.idx_temp");
        $this->assertInstanceOf(SchemaChangeResult::class, $dropped);
    }

    public function testIndexOnFullFrozenCollection(): void {
        // A FULL() index matches the collection as a whole, so the bind value is
        // the complete frozen set.
        $ids = $this->queryIds(
            "SELECT id FROM {$this->keyspace}.index_riders WHERE career_teams = ?",
            [Value\SetCollection::fromValue(['Nederland bloeit', 'Rabobank'], Type::VARCHAR, isFrozen: true)]
        );

        $this->assertSame([1], $ids, 'FULL() indexes match on the entire frozen collection');
    }

    public function testIndexOnMapEntries(): void {
        $ids = $this->queryIds(
            "SELECT id FROM {$this->keyspace}.index_riders WHERE teams[?] = ?",
            [2011, 'Nederland bloeit']
        );

        $this->assertSame([1], $ids, 'An ENTRIES() index supports a key/value predicate');
    }

    public function testIndexOnMapKeys(): void {
        $ids = $this->queryIds(
            "SELECT id FROM {$this->keyspace}.index_riders WHERE teams CONTAINS KEY ?",
            [2015]
        );

        $this->assertSame([2], $ids, 'A KEYS() index supports CONTAINS KEY');
    }

    public function testIndexOnRegularColumn(): void {
        $ids = $this->queryIds(
            "SELECT id FROM {$this->keyspace}.index_riders WHERE nationality = ?",
            ['Netherlands']
        );

        $this->assertSame([1, 2], $ids);
    }

    public function testIndexOnSetValues(): void {
        $ids = $this->queryIds(
            "SELECT id FROM {$this->keyspace}.index_riders WHERE tags CONTAINS ?",
            ['sprinter']
        );

        $this->assertSame([2], $ids, 'An index on a set supports CONTAINS');
    }

    public function testSasiIndexWithLikeContains(): void {
        if (!self::isSasiIndexSupported()) {
            $this->markTestSkipped('SASI indexes are not supported (Cassandra 3.4+ only)');
        }

        $ids = $this->queryIds(
            "SELECT id FROM {$this->keyspace}.index_riders WHERE bio LIKE ?",
            ['%climb%']
        );

        $this->assertSame([1, 3], $ids, 'A SASI index in CONTAINS mode matches anywhere in the value');
    }

    public function testSasiIndexWithLikePrefix(): void {
        if (!self::isSasiIndexSupported()) {
            $this->markTestSkipped('SASI indexes are not supported (Cassandra 3.4+ only)');
        }

        $ids = $this->queryIds(
            "SELECT id FROM {$this->keyspace}.index_riders WHERE nickname LIKE ?",
            ['rab%']
        );

        $this->assertSame([1, 2], $ids, 'A SASI index enables LIKE prefix matching');
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS index_riders ('
            . 'id int PRIMARY KEY, '
            . 'nationality varchar, '
            . 'tags set<varchar>, '
            . 'teams map<int, varchar>, '
            . 'career_teams frozen<set<varchar>>, '
            . 'nickname varchar, '
            . 'bio varchar, '
            . 'age int)'
        );

        $conn->query(
            'INSERT INTO index_riders (id, nationality, tags, teams, career_teams, nickname, bio) VALUES '
            . "(1, 'Netherlands', {'climber'}, {2011: 'Nederland bloeit'}, "
            . "{'Rabobank', 'Nederland bloeit'}, 'rabobank-liv', 'a strong climber')"
        );
        $conn->query(
            'INSERT INTO index_riders (id, nationality, tags, teams, career_teams, nickname, bio) VALUES '
            . "(2, 'Netherlands', {'sprinter'}, {2015: 'Rabobank-Liv'}, "
            . "{'Rabobank-Liv'}, 'rabo-sprint', 'a pure sprinter')"
        );
        $conn->query(
            'INSERT INTO index_riders (id, nationality, tags, teams, career_teams, nickname, bio) VALUES '
            . "(3, 'Belgium', {'climber'}, {2016: 'Lotto'}, "
            . "{'Lotto'}, 'lotto-star', 'another climber')"
        );

        $conn->query('CREATE INDEX IF NOT EXISTS idx_nationality ON index_riders (nationality)');
        $conn->query('CREATE INDEX IF NOT EXISTS idx_tags ON index_riders (tags)');
        $conn->query('CREATE INDEX IF NOT EXISTS idx_teams_keys ON index_riders (KEYS(teams))');
        $conn->query('CREATE INDEX IF NOT EXISTS idx_teams_entries ON index_riders (ENTRIES(teams))');
        $conn->query('CREATE INDEX IF NOT EXISTS idx_career_teams ON index_riders (FULL(career_teams))');

        if (self::isSasiIndexSupported()) {
            // SASI is a Cassandra-only index implementation. PREFIX is the default
            // mode; CONTAINS additionally matches in the middle of a value.
            $conn->query(
                'CREATE CUSTOM INDEX IF NOT EXISTS idx_nickname_sasi ON index_riders (nickname) '
                . "USING 'org.apache.cassandra.index.sasi.SASIIndex'"
            );
            $conn->query(
                'CREATE CUSTOM INDEX IF NOT EXISTS idx_bio_sasi ON index_riders (bio) '
                . "USING 'org.apache.cassandra.index.sasi.SASIIndex' "
                . "WITH OPTIONS = {'mode': 'CONTAINS', "
                . "'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', "
                . "'case_sensitive': 'false'}"
            );
        }

        $conn->disconnect();
    }

    /**
     * Index builds complete asynchronously. Until a build finishes the server
     * answers with IndexNotAvailableException (surfaced as a read failure), so
     * retry both on an error and on a still-empty result.
     *
     * @param array<mixed> $values
     *
     * @return list<int>
     */
    private function queryIds(string $query, array $values): array {
        $ids = [];

        for ($attempt = 0; $attempt < 50; $attempt++) {
            try {
                $rows = $this->connection->query($query, $values)->asRowsResult()->fetchAll();

                /** @var list<int> $ids */
                $ids = array_column($rows, 'id');

                if ($ids !== []) {
                    break;
                }
            } catch (ServerException) {
                // index not built yet
            }

            usleep(100000);
        }

        sort($ids);

        return $ids;
    }
}
