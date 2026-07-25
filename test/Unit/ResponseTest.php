<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Response\Event\Data\SchemaChangeTarget;
use Cassandra\Response\Event\Data\SchemaChangeType;
use Cassandra\Response\Event\SchemaChangeEvent;
use Cassandra\Response\Ready;
use Cassandra\Response\Result\SchemaChangeResult;
use Cassandra\Response\StreamReader;

class ResponseTest extends AbstractUnitTestCase {
    public function testSchemaChangeEventWithKeyspaceTarget(): void {
        $body = self::encodeString('SCHEMA_CHANGE')
            . self::encodeString('DROPPED')
            . self::encodeString('KEYSPACE')
            . self::encodeString('myks');

        $header = new Header(
            version: ProtocolVersion::V5,
            flags: 0,
            stream: -1,
            opcode: Opcode::RESPONSE_EVENT,
            length: strlen($body),
        );

        $event = new SchemaChangeEvent($header, new StreamReader($body));
        $data = $event->getSchemaChangeData();

        $this->assertSame(SchemaChangeType::DROPPED, $data->changeType);
        $this->assertSame(SchemaChangeTarget::KEYSPACE, $data->target);
        $this->assertSame('myks', $data->keyspace);
        $this->assertNull($data->name);
    }

    public function testSchemaChangeResultWithKeyspaceTarget(): void {
        $body = pack('N', 5) // result kind: SCHEMA_CHANGE
            . self::encodeString('CREATED')
            . self::encodeString('KEYSPACE')
            . self::encodeString('myks');

        $header = new Header(
            version: ProtocolVersion::V5,
            flags: 0,
            stream: 0,
            opcode: Opcode::RESPONSE_RESULT,
            length: strlen($body),
        );

        $result = new SchemaChangeResult($header, new StreamReader($body));
        $data = $result->getSchemaChangeData();

        $this->assertSame(SchemaChangeType::CREATED, $data->changeType);
        $this->assertSame(SchemaChangeTarget::KEYSPACE, $data->target);
        $this->assertSame('myks', $data->keyspace);
        $this->assertNull($data->name);
    }

    public function testSchemaChangeResultWithTableTarget(): void {
        $body = pack('N', 5)
            . self::encodeString('UPDATED')
            . self::encodeString('TABLE')
            . self::encodeString('myks')
            . self::encodeString('mytable');

        $header = new Header(
            version: ProtocolVersion::V5,
            flags: 0,
            stream: 0,
            opcode: Opcode::RESPONSE_RESULT,
            length: strlen($body),
        );

        $result = new SchemaChangeResult($header, new StreamReader($body));
        $data = $result->getSchemaChangeData();

        $this->assertSame(SchemaChangeType::UPDATED, $data->changeType);
        $this->assertSame(SchemaChangeTarget::TABLE, $data->target);
        $this->assertSame('myks', $data->keyspace);
        $this->assertSame('mytable', $data->name);
    }

    public function testToStringEncodesResponseFrameHeader(): void {
        $header = new Header(
            version: ProtocolVersion::V5,
            flags: 0,
            stream: 3,
            opcode: Opcode::RESPONSE_READY,
            length: 0,
        );

        $frame = (string) (new Ready($header, new StreamReader('')));

        // version byte carries the response direction bit (0x80 | 0x05)
        $this->assertSame("\x85\x00\x00\x03\x02\x00\x00\x00\x00", $frame);
    }

    private static function encodeString(string $value): string {
        return pack('n', strlen($value)) . $value;
    }
}
