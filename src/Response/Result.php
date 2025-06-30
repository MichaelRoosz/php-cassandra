<?php

declare(strict_types=1);

namespace Cassandra\Response;

use ArrayObject;
use Cassandra\Request\Request;
use IteratorAggregate;

use Cassandra\Type;

/**
 * @implements IteratorAggregate<ArrayObject<string, mixed>|array<string, mixed>|null>
 */
class Result extends Response implements IteratorAggregate {
    final public const PREPARED = 0x0004;
    final public const ROWS = 0x0002;

    final public const ROWS_FLAG_GLOBAL_TABLES_SPEC = 0x0001;
    final public const ROWS_FLAG_HAS_MORE_PAGES = 0x0002;
    final public const ROWS_FLAG_NO_METADATA = 0x0004;
    final public const ROWS_METADATA_CHANGED = 0x0008;
    final public const SCHEMA_CHANGE = 0x0005;
    final public const SET_KEYSPACE = 0x0003;
    final public const VOID = 0x0001;

    protected ?int $kind = null;

    /**
     * @var array{
     *  flags: int,
     *  columns_count: int,
     *  paging_state?: ?string,
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
    protected ?array $metadata = null;

    /**
     * @var array{
     *  flags: int,
     *  columns_count: int,
     *  paging_state?: ?string,
     *  new_metadata_id?: string,
     *  pk_count?: int,
     *  pk_index?: int[],
     *  columns?: array<array{
     *   keyspace: string,
     *   tableName: string,
     *   name: string,
     *   type: int|array<mixed>,
     *  }>,
     * } $metadataOfPreviousResult
     */
    protected ?array $metadataOfPreviousResult = null;

    /**
     * @var ?array{
     *   id: string,
     *   query_metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     paging_state?: ?string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     *   result_metadata_id?: ?string
     * } $nextExecuteCallInfo
     */
    protected ?array $nextExecuteCallInfo = null;

    protected ?Request $request = null;

    /**
     * @var class-string<RowClass> $rowClass
     */
    protected ?string $rowClass = null;

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function calculateMetadata(): void {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        $this->stream->offset(4);
        $metadata = $this->readMetadata(false);

        if ($this->metadataOfPreviousResult !== null) {
            $this->metadata = array_merge($this->metadataOfPreviousResult, $metadata);
        } else {
            $this->metadata = $metadata;
        }
    }

    /**
     * @param class-string<RowClass> $rowClass
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchAll(?string $rowClass = null): array {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->stream->offset(4);
        $this->calculateMetadata();

        if (!isset($this->metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->stream->readInt();
        $rows = [];

        if ($rowClass === null) {
            $rowClass = $this->rowClass;
        }

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        for ($i = 0; $i < $rowCount; ++$i) {
            $data = [];

            foreach ($this->metadata['columns'] as $column) {
                /** @psalm-suppress MixedAssignment */
                $data[$column['name']] = $this->stream->readValue($column['type']);
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
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchCol(int $index = 0): array {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->stream->offset(4);
        $this->calculateMetadata();

        if (!isset($this->metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->stream->readInt();

        $array = [];

        for ($i = 0; $i < $rowCount; ++$i) {
            /** @psalm-suppress MixedAssignment */
            foreach ($this->metadata['columns'] as $j => $column) {
                $value = $this->stream->readValue($column['type']);

                if ($j == $index) {
                    $array[$i] = $value;
                }
            }
        }

        return $array;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function fetchOne(): mixed {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }
        $this->stream->offset(4);
        $this->calculateMetadata();

        if (!isset($this->metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->stream->readInt();

        if ($rowCount === 0) {
            return null;
        }

        foreach ($this->metadata['columns'] as $column) {
            return $this->stream->readValue($column['type']);
        }

        return null;
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
        $this->stream->offset(4);
        $this->calculateMetadata();

        if (!isset($this->metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->stream->readInt();

        $map = [];

        for ($i = 0; $i < $rowCount; ++$i) {
            $key = null;

            /** @psalm-suppress MixedAssignment */
            foreach ($this->metadata['columns'] as $j => $column) {
                $value = $this->stream->readValue($column['type']);

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
        $this->stream->offset(4);
        $this->calculateMetadata();

        if (!isset($this->metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->stream->readInt();

        if ($rowCount === 0) {
            return null;
        }

        if ($rowClass === null) {
            $rowClass = $this->rowClass;
        }

        if ($rowClass !== null && !is_subclass_of($rowClass, ArrayObject::class)) {
            throw new Exception('row class "' . $rowClass . '" is not a subclass of ArrayObject');
        }

        $data = [];
        foreach ($this->metadata['columns'] as $column) {
            /** @psalm-suppress MixedAssignment */
            $data[$column['name']] = $this->stream->readValue($column['type']);
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
     * @return null|array<mixed>|string|array{
     *   id: string,
     *   result_metadata_id?: string,
     *   metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     paging_state?: ?string,
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
     *     paging_state?: ?string,
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
    public function getData(): null|string|array {
        switch ($this->getKind()) {
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
     * @return array<mixed>|array{
     *   id: string,
     *   result_metadata_id?: string,
     *   metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     paging_state?: ?string,
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
     *     paging_state?: ?string,
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
     * }|array{
     *   keyspace: string
     * }|array{}
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getDataArray(): array {
        switch ($this->getKind()) {
            case self::VOID:
                return [];

            case self::ROWS:
                return $this->getRowsData();

            case self::SET_KEYSPACE:
                return ['keyspace' => $this->getSetKeyspaceData()];

            case self::PREPARED:
                return $this->getPreparedData();

            case self::SCHEMA_CHANGE:
                return $this->getSchemaChangeData();
        }

        return [];
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getIterator(): ResultIterator {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        $metadata = $this->getMetadata();
        if (!isset($metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        return new ResultIterator($this->stream, $metadata, $this->rowClass);
    }

    /**
     * @throws \Cassandra\Response\Exception
     */
    public function getKind(): int {
        if ($this->kind === null) {
            $this->stream->offset(0);
            $this->kind = $this->stream->readInt();
        }

        return $this->kind;
    }

    /**
     * @return array{
     *  flags: int,
     *  columns_count: int,
     *  paging_state?: ?string,
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
    public function getMetadata(): array {
        $this->calculateMetadata();

        if ($this->metadata === null) {
            throw new Exception('Missing Result Metadata');
        }

        return $this->metadata;
    }

    /**
     * @return ?array{
     *   id: string,
     *   query_metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     paging_state?: ?string,
     *     pk_count?: int,
     *     pk_index?: int[],
     *     columns?: array<array{
     *       keyspace: string,
     *       tableName: string,
     *       name: string,
     *       type: int|array<mixed>,
     *     }>,
     *   },
     *   result_metadata_id?: ?string
     * }
     */
    public function getNextExecuteCallInfo(): ?array {
        return $this->nextExecuteCallInfo;
    }

    /**
     * @return array{
     *   id: string,
     *   result_metadata_id?: string,
     *   metadata: array{
     *     flags: int,
     *     columns_count: int,
     *     new_metadata_id?: string,
     *     paging_state?: ?string,
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
     *     paging_state?: ?string,
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

        $this->stream->offset(4);

        if ($this->getVersion() >= 5) {
            $data = [
                'id' => $this->stream->readString(),
                'result_metadata_id' => $this->stream->readString(),
                'metadata' => $this->readMetadata(isPrepareMetaData: true),
                'result_metadata' => $this->readMetadata(isPrepareMetaData: false),
            ];
        } else {
            $data = [
                'id' => $this->stream->readString(),
                'metadata' => $this->readMetadata(isPrepareMetaData: true),
                'result_metadata' => $this->readMetadata(isPrepareMetaData: false),
            ];
        }

        return $data;
    }

    public function getRequest(): ?Request {
        return $this->request;
    }

    /**
     * @return int
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getRowCount(): int {
        if ($this->getKind() !== self::ROWS) {
            return 0;
        }

        $this->stream->offset(4);
        $this->calculateMetadata();

        if (!isset($this->metadata['columns'])) {
            throw new Exception('Missing Result Metadata');
        }

        $rowCount = $this->stream->readInt();

        return $rowCount;
    }

    /**
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function getRowsData(): array {
        if ($this->getKind() !== self::ROWS) {
            throw new Exception('Unexpected Response: ' . $this->getKind());
        }

        return $this->fetchAll();
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

        $this->stream->offset(4);

        $data = [
            'change_type' => $this->stream->readString(),
            'target' => $this->stream->readString(),
            'keyspace' => $this->stream->readString(),
        ];

        switch ($data['target']) {
            case 'TABLE':
            case 'TYPE':
                $data['name'] = $this->stream->readString();

                break;

            case 'FUNCTION':
            case 'AGGREGATE':
                $data['name'] = $this->stream->readString();

                /** @var string[] $argument_types */
                $argument_types = $this->stream->readList([Type::TEXT]);
                $data['argument_types'] = $argument_types;

                break;
        }

        return $data;
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

        $this->stream->offset(4);

        return $this->stream->readString();
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

        $this->stream->offset(4);

        return null;
    }

    /**
     * @param array{
     *  flags: int,
     *  columns_count: int,
     *  paging_state?: ?string,
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
        $this->metadata = $metadata;

        return $this;
    }

    /**
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function setPreviousResult(Result $previousResult): static {
        $previousKind = $previousResult->getKind();

        if ($previousKind === self::PREPARED) {
            $prepareData = $previousResult->getPreparedData();

            $this->metadataOfPreviousResult = $prepareData['result_metadata'];

            $this->nextExecuteCallInfo = [
                'id' => $prepareData['id'],
                'query_metadata' => $prepareData['metadata'],
                'result_metadata_id' => $prepareData['result_metadata_id'] ?? null,
            ];
        } elseif ($previousKind === self::ROWS) {
            $this->metadataOfPreviousResult = $previousResult->getMetadata();

            $lastExecuteCallInfo = $previousResult->getNextExecuteCallInfo();
            if ($lastExecuteCallInfo === null) {
                throw new Exception('prepared statement not found');
            }

            $lastMetadata = $previousResult->getMetadata();

            $resultMetadataId = $lastMetadata['new_metadata_id'] ?? $lastExecuteCallInfo['result_metadata_id'] ?? null;

            $this->nextExecuteCallInfo = [
                'id' => $lastExecuteCallInfo['id'],
                'query_metadata' => $lastExecuteCallInfo['query_metadata'],
                'result_metadata_id' => $resultMetadataId,
            ];
        }

        return $this;
    }

    public function setRequest(Request $request): void {
        $this->request = $request;
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

        $this->rowClass = $rowClass;

        return $this;
    }

    /**
     * @return array{
     *  flags: int,
     *  columns_count: int,
     *  paging_state?: ?string,
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
    protected function readMetadata(bool $isPrepareMetaData = false): array {
        $metadata = [
            'flags' => $this->stream->readInt(),
            'columns_count' => $this->stream->readInt(),
        ];
        $flags = $metadata['flags'];

        if ($flags & self::ROWS_FLAG_HAS_MORE_PAGES) {
            $metadata['paging_state'] = $this->stream->readBytes();
        }

        if ($flags & self::ROWS_METADATA_CHANGED) {
            $metadata['new_metadata_id'] = $this->stream->readString();
        }

        if ($this->getVersion() >= 4 && $isPrepareMetaData) {
            $metadata['pk_count'] = $this->stream->readInt();
            $metadata['pk_index'] = [];

            if ($metadata['pk_count'] > 0) {
                for ($i = 0; $i < $metadata['pk_count']; ++$i) {
                    $metadata['pk_index'][] =  $this->stream->readShort();
                }
            }
        }

        if (!($flags & self::ROWS_FLAG_NO_METADATA)) {
            $metadata['columns'] = [];

            if ($flags & self::ROWS_FLAG_GLOBAL_TABLES_SPEC) {
                $keyspace = $this->stream->readString();
                $tableName = $this->stream->readString();

                for ($i = 0; $i < $metadata['columns_count']; ++$i) {
                    $metadata['columns'][] = [
                        'keyspace' => $keyspace,
                        'tableName' => $tableName,
                        'name' => $this->stream->readString(),
                        'type' => $this->stream->readType(),
                    ];
                }
            } else {
                for ($i = 0; $i < $metadata['columns_count']; ++$i) {
                    $metadata['columns'][] = [
                        'keyspace' => $this->stream->readString(),
                        'tableName' => $this->stream->readString(),
                        'name' => $this->stream->readString(),
                        'type' => $this->stream->readType(),
                    ];
                }
            }
        }

        return $metadata;
    }
}
