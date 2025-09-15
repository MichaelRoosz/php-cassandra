<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\EventType;
use Cassandra\Request\Register;
use Cassandra\Response\Event\SchemaChangeEvent;
use Cassandra\Response\Event\Data\SchemaChangeTarget;
use Cassandra\Response\Event\Data\SchemaChangeType;
use Cassandra\Response\Ready;
use Cassandra\Test\Integration\Data\EventListener;

final class EventsIntegrationTest extends AbstractIntegrationTestCase {
    public function testRegisterAllReturnsReady(): void {

        $conn = $this->connection;

        $resp = $conn->syncRequest(new Register([
            EventType::TOPOLOGY_CHANGE,
            EventType::STATUS_CHANGE,
            EventType::SCHEMA_CHANGE,
        ]));

        $this->assertInstanceOf(Ready::class, $resp);
    }

    public function testSchemaChangeEventsListenerReceivesCreatedAndDropped(): void {

        $conn = $this->connection;

        // Only subscribe to schema events for this test
        $conn->syncRequest(new Register([EventType::SCHEMA_CHANGE]));

        $listener = new EventListener();
        $conn->registerEventListener($listener);

        $table = 'evt_' . substr(bin2hex(random_bytes(6)), 0, 12);
        $fqtn = $this->keyspace . '.' . $table;

        // Create a new table to trigger SCHEMA_CHANGE CREATED
        $conn->query("CREATE TABLE {$fqtn}(id int PRIMARY KEY, v int)");

        $created = $this->waitForSchemaChange($listener, $table, SchemaChangeType::CREATED);
        $this->assertNotNull($created, 'Expected SCHEMA_CHANGE CREATED event for table creation');
        $this->assertSame($this->keyspace, $created->getSchemaChangeData()->keyspace);
        $this->assertSame($table, $created->getSchemaChangeData()->name);
        $this->assertSame(SchemaChangeTarget::TABLE, $created->getSchemaChangeData()->target);

        // Drop the table to trigger SCHEMA_CHANGE DROPPED
        $conn->query("DROP TABLE {$fqtn}");

        $dropped = $this->waitForSchemaChange($listener, $table, SchemaChangeType::DROPPED);
        $this->assertNotNull($dropped, 'Expected SCHEMA_CHANGE DROPPED event for table drop');
        $this->assertSame($this->keyspace, $dropped->getSchemaChangeData()->keyspace);
        $this->assertSame($table, $dropped->getSchemaChangeData()->name);
        $this->assertSame(SchemaChangeTarget::TABLE, $dropped->getSchemaChangeData()->target);

        $conn->unregisterEventListener($listener);
    }

    public function testUnregisterEventListenerPreventsCallbacks(): void {

        $conn = $this->connection;

        $conn->syncRequest(new Register([EventType::SCHEMA_CHANGE]));

        $listener = new EventListener();

        $conn->registerEventListener($listener);

        $table1 = 'evt_' . substr(bin2hex(random_bytes(6)), 0, 12);
        $fqtn1 = $this->keyspace . '.' . $table1;
        $conn->query("CREATE TABLE {$fqtn1}(id int PRIMARY KEY, v int)");
        $this->waitForSchemaChange($listener, $table1, SchemaChangeType::CREATED);

        // Now stop listening and perform another schema change
        $conn->unregisterEventListener($listener);

        $before = count($listener->getEvents());

        $table2 = 'evt_' . substr(bin2hex(random_bytes(6)), 0, 12);
        $fqtn2 = $this->keyspace . '.' . $table2;
        $conn->query("CREATE TABLE {$fqtn2}(id int PRIMARY KEY, v int)");

        // Pump the socket a few times to allow events to be read if any were delivered,
        // but since we unregistered the listener, the callback array must not change.
        $this->pumpFor(1500);

        $after = count($listener->getEvents());
        $this->assertSame($before, $after, 'Listener should not receive events after unregister');

        // Cleanup
        $conn->query("DROP TABLE {$fqtn1}");
        $conn->query("DROP TABLE {$fqtn2}");
    }

    private function pumpFor(int $timeoutMs): void {
        $deadline = microtime(true) + ($timeoutMs / 1000);
        do {
            $this->connection->tryReadNextEvent();
            usleep(100_000);
        } while (microtime(true) < $deadline);
    }

    private function waitForSchemaChange(EventListener $listener, string $table, SchemaChangeType $expectedType, int $timeoutMs = 5000): ?SchemaChangeEvent {

        foreach ($listener->getEvents() as $event) {
            if (!($event instanceof SchemaChangeEvent)) {
                continue;
            }

            $data = $event->getSchemaChangeData();
            if ($data->target === SchemaChangeTarget::TABLE
                && $data->keyspace === $this->keyspace
                && $data->name === $table
                && $data->changeType === $expectedType
            ) {
                return $event;
            }
        }

        $deadline = microtime(true) + ($timeoutMs / 1000);
        do {
            $event = $this->connection->tryReadNextEvent();

            if ($event === null) {
                usleep(100_000); // 100ms

                continue;
            }

            if (!($event instanceof SchemaChangeEvent)) {
                usleep(100_000); // 100ms

                continue;
            }

            $data = $event->getSchemaChangeData();
            if ($data->target === SchemaChangeTarget::TABLE
                && $data->keyspace === $this->keyspace
                && $data->name === $table
                && $data->changeType === $expectedType
            ) {
                return $event;
            }

            usleep(100_000); // 100ms

        } while (microtime(true) < $deadline);

        return null;
    }
}
