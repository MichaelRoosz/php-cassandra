<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayObject;
use IteratorAggregate;
use SplFixedArray;

use Cassandra\Type;

/**
 * @implements IteratorAggregate<ArrayObject<string, mixed>|array<string, mixed>|null>
 */
class Result extends Response implements IteratorAggregate {
    public const VOID = 0x0001;
    public const ROWS = 0x0002;
    public const SET_KEYSPACE = 0x0003;
    public const PREPARED = 0x0004;
    public const SCHEMA_CHANGE = 0x0005;

    public const ROWS_FLAG_GLOBAL_TABLES_SPEC = 0x0001;
    public const ROWS_FLAG_HAS_MORE_PAGES = 0x0002;
    public const ROWS_FLAG_NO_METADATA = 0x0004;
    public const ROWS_METADATA_CHANGED = 0x0008;

    protected ?int $_kind = null;

    /**
     * @var array{
     *  flags: int,
     *  columns_count: int,
     *  page_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * } $_metadata
     */
    protected ?array $_metadata = null;

    /**
     * @var class-string<RowClass> $_rowClass
     */
    protected ?string $_rowClass = null;

    /**
     * @return null|SplFixedArray<mixed>|string|array{
     *   id: string,
     *   result_metadata_id?: string,
     *   metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     page_state?: ?string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     *   result_metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     page_state?: ?string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     * }|array{
     *  change_type: string,
     *  target: string,
     *  keyspace: string,
     *  name?: string,
     *  argument_types?: string[]
     * }
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getData(): null|SplFixedArray|string|array {
        switch($this->getKind()) {
            case self::VOID:
                return $this->getVoidData();

            case self::ROWS:
                return $this->getRowsData();

            case self::SET_KEYSPACE:
                return $this->getSetKeyspaceData();

            case self::PREPARED:
                return $this->getPreparedData();

            case self::SCHEMA_CHANGE:
                return $this->getSchemaChangeData();
        }

        return null;
    }

    /**
     * @return null
     *
     * @throws \Cassandra\Response\Exception
     */
    public function getVoidData(): ?string {
        if ($this->getKind() !== self::VOID) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        $this->_stream->offset(4);

        return null;
    }

    /**
     * @return SplFixedArray<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getRowsData(): SplFixedArray {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        return $this->fetchAll();
    }

    /**
     * @return string
     *
     * @throws \Cassandra\Response\Exception
     */
    public function getSetKeyspaceData(): string {
        if ($this->getKind() !== self::SET_KEYSPACE) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        $this->_stream->offset(4);

        return $this->_stream->readString();
    }

    /**
     * @return array{
     *   id: string,
     *   result_metadata_id?: string,
     *   metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     page_state?: ?string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     *   result_metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     page_state?: ?string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     * }
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getPreparedData(): array {
        if ($this->getKind() !== self::PREPARED) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        $this->_stream->offset(4);

        if ($this->getVersion() >= 5) {
            $data = [
                'id' => $this->_stream->readString(),
                'result_metadata_id' => $this->_stream->readString(),
                'metadata' => $this->_readMetadata(isPrepareMetaData: true),
                'result_metadata' => $this->_readMetadata(isPrepareMetaData: true),
            ];
        } else {
            $data = [
                'id' => $this->_stream->readString(),
                'metadata' => $this->_readMetadata(isPrepareMetaData: true),
                'result_metadata' => $this->_readMetadata(isPrepareMetaData: true),
            ];
        }

        return $data;
    }

    /**
     * @return array{
     *  change_type: string,
     *  target: string,
     *  keyspace: string,
     *  name?: string,
     *  argument_types?: string[]
     * }
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getSchemaChangeData(): array {
        if ($this->getKind() !== self::SCHEMA_CHANGE) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        $this->_stream->offset(4);

        $data = [
            'change_type' => $this->_stream->readString(),
            'target' => $this->_stream->readString(),
            'keyspace' => $this->_stream->readString(),
        ];

        switch ($data['target']) {
            case 'TABLE':
            case 'TYPE':
                $data['name'] = $this->_stream->readString();

                break;

            case 'FUNCTION':
            case 'AGGREGATE':
                $data['name'] = $this->_stream->readString();

                /** @var string[] $argument_types */
                $argument_types = $this->_stream->readList([Type\Base::TEXT]);
                $data['argument_types'] = $argument_types;

                break;
        }

        return $data;
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getKind(): int {
        if ($this->_kind === null) {
            $this->_stream->offset(0);
            $this->_kind = $this->_stream->readInt();
        }

        return $this->_kind;
    }

    /**
     * @param array{
     *  flags: int,
     *  columns_count: int,
     *  page_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * } $metadata
     */
    public function setMetadata(array $metadata): static {
        $this->_metadata = $metadata;

        return $this;
    }

    /**
     * @return array{
     *  flags: int,
     *  columns_count: int,
     *  page_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * }
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getMetadata(bool $isPrepareMetaData = false): array {
        if ($this->_metadata === null) {
            $this->_stream->offset(4);
            $this->_metadata = $this->_readMetadata($isPrepareMetaData);
        }

        return $this->_metadata;
    }

    /**
     * @param class-string<RowClass> $rowClass
     *
     * @throws \Cassandra\Response\Exception
     */
    public function setRowClass(string $rowClass): static {
        if (!is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $this->_rowClass = $rowClass;

        return $this;
    }

    /**
     * @param class-string<RowClass> $rowClass
     * @return SplFixedArray<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchAll(?string $rowClass = null): SplFixedArray {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->_stream->offset(4);
        $this->_metadata = $this->_metadata ? array_merge($this->_metadata, $this->_readMetadata()) : $this->_readMetadata();

        if (!isset($this->_metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->_stream->readInt();
        $rows = new SplFixedArray($rowCount);

        if ($rowClass === null) {
            $rowClass = $this->_rowClass;
        }

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        for ($i = 0; $i < $rowCount; ++$i) {
            $data = [];

            foreach ($this->_metadata['columns'] as $column) {
                /** @psalm-suppress MixedAssignment */
                $data[$column['name']] = $this->_stream->readValue($column['type']);
            }

            if ($rowClass === null) {
                $rows[$i] = $data;
            } else {
                $rows[$i] = new $rowClass($data);
            }
        }

        return $rows;
    }

    /**
     * @return SplFixedArray<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchCol(int $index = 0): SplFixedArray {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->_stream->offset(4);
        $this->_metadata = $this->_metadata ? array_merge($this->_metadata, $this->_readMetadata()) : $this->_readMetadata();

        if (!isset($this->_metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->_stream->readInt();

        $array = new SplFixedArray($rowCount);

        for ($i = 0; $i < $rowCount; ++$i) {
            /** @psalm-suppress MixedAssignment */
            foreach ($this->_metadata['columns'] as $j => $column) {
                $value = $this->_stream->readValue($column['type']);

                if ($j == $index) {
                    $array[$i] = $value;
                }
            }
        }

        return $array;
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchPairs(): array {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->_stream->offset(4);
        $this->_metadata = $this->_metadata ? array_merge($this->_metadata, $this->_readMetadata()) : $this->_readMetadata();

        if (!isset($this->_metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->_stream->readInt();

        $map = [];

        for ($i = 0; $i < $rowCount; ++$i) {
            $key = null;
            /** @psalm-suppress MixedAssignment */
            foreach ($this->_metadata['columns'] as $j => $column) {
                $value = $this->_stream->readValue($column['type']);

                if ($j === 0) {
                    $key = $value;
                    if (!is_int($key) && !is_string($key)) {
                        throw new Exception('Invalid key type');
                    }
                } elseif ($j === 1 && $key !== null) {
                    $map[$key] = $value;
                }
            }
        }

        return $map;
    }

    /**
     * @param class-string<RowClass> $rowClass
     * @return ArrayObject<string, mixed>|array<string, mixed>|null
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchRow(?string $rowClass = null): ArrayObject|array|null {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->_stream->offset(4);
        $this->_metadata = $this->_metadata ? array_merge($this->_metadata, $this->_readMetadata()) : $this->_readMetadata();

        if (!isset($this->_metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->_stream->readInt();

        if ($rowCount === 0) {
            return null;
        }

        if ($rowClass === null) {
            $rowClass = $this->_rowClass;
        }

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $data = [];
        foreach ($this->_metadata['columns'] as $column) {
            /** @psalm-suppress MixedAssignment */
            $data[$column['name']] = $this->_stream->readValue($column['type']);
        }

        if ($rowClass === null) {
            return $data;
        }

        /** @var ArrayObject<string, mixed> $row
         */
        $row = new $rowClass($data);

        return $row;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchOne(): mixed {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->_stream->offset(4);
        $this->_metadata = $this->_metadata ? array_merge($this->_metadata, $this->_readMetadata()) : $this->_readMetadata();

        if (!isset($this->_metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->_stream->readInt();

        if ($rowCount === 0) {
            return null;
        }

        foreach ($this->_metadata['columns'] as $column) {
            return $this->_stream->readValue($column['type']);
        }

        return null;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getIterator(): ResultIterator {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        $metadata = $this->getMetadata();
        if (!isset($metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        return new ResultIterator($this->_stream, $metadata, $this->_rowClass);
    }

    /**
     * @return array{
     *  flags: int,
     *  columns_count: int,
     *  page_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * }
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    protected function _readMetadata(bool $isPrepareMetaData = false): array {
        $metadata = [
            'flags' => $this->_stream->readInt(),
            'columns_count' => $this->_stream->readInt(),
        ];
        $flags = $metadata['flags'];

        if ($flags & self::ROWS_FLAG_HAS_MORE_PAGES) {
            $metadata['page_state'] = $this->_stream->readBytes();
        }

        if ($flags & self::ROWS_METADATA_CHANGED) {
            $metadata['new_metadata_id'] = $this->_stream->readString();
        }

        if ($this->getVersion() >= 4 && $isPrepareMetaData) {
            $metadata['pk_count'] = $this->_stream->readInt();
            $metadata['pk_index'] = [];

            if ($metadata['pk_count'] > 0) {
                for ($i = 0; $i < $metadata['pk_count']; ++$i) {
                    $metadata['pk_index'][] =  $this->_stream->readShort();
                }
            }
        }

        if (!($flags & self::ROWS_FLAG_NO_METADATA)) {
            $metadata['columns'] = [];

            if ($flags & self::ROWS_FLAG_GLOBAL_TABLES_SPEC) {
                $keyspace = $this->_stream->readString();
                $tableName = $this->_stream->readString();

                for ($i = 0; $i < $metadata['columns_count']; ++$i) {
                    $metadata['columns'][] = [
                        'keyspace' => $keyspace,
                        'tableName' => $tableName,
                        'name' => $this->_stream->readString(),
                        'type' => $this->_stream->readType(),
                    ];
                }
            } else {
                for ($i = 0; $i < $metadata['columns_count']; ++$i) {
                    $metadata['columns'][] = [
                        'keyspace' => $this->_stream->readString(),
                        'tableName' => $this->_stream->readString(),
                        'name' => $this->_stream->readString(),
                        'type' => $this->_stream->readType(),
                    ];
                }
            }
        }

        return $metadata;
    }
}
