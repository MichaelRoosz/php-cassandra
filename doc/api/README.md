# API reference

Generated from the source code with [phpDocumentor](https://www.phpdoc.org/) — run `composer doc:api` to
regenerate. Only the public API is listed; protected and private members are omitted.

For guides and examples, see the [README](../../README.md).

## Namespaces

### \Cassandra

#### Classes

| Class                                                         | Description                                                                                                         |
|---------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| [`Connection`](./classes/Cassandra/Connection.md)             |                                                                                                                     |
| [`ReleaseConstants`](./classes/Cassandra/ReleaseConstants.md) |                                                                                                                     |
| [`Statement`](./classes/Cassandra/Statement.md)               |                                                                                                                     |
| [`StringUtil`](./classes/Cassandra/StringUtil.md)             | ASCII character-class checks used for validating numeric and hex strings.                                           |
| [`TypeNameParser`](./classes/Cassandra/TypeNameParser.md)     | Parser for the Java class name strings Cassandra uses to describe column types
(org.apache.cassandra.db.marshal.*). |
| [`ValueFactory`](./classes/Cassandra/ValueFactory.md)         |                                                                                                                     |
| [`VIntCodec`](./classes/Cassandra/VIntCodec.md)               | Note that a VInt is different from a Varint.                                                                        |

#### Enums

| Enum                                                            | Description                             |
|-----------------------------------------------------------------|-----------------------------------------|
| [`Consistency`](./classes/Cassandra/Consistency.md)             |                                         |
| [`EventType`](./classes/Cassandra/EventType.md)                 |                                         |
| [`SerialConsistency`](./classes/Cassandra/SerialConsistency.md) |                                         |
| [`StatementStatus`](./classes/Cassandra/StatementStatus.md)     | Where an asynchronous statement stands. |
| [`Type`](./classes/Cassandra/Type.md)                           |                                         |
| [`TypeName`](./classes/Cassandra/TypeName.md)                   |                                         |

#### Interfaces

| Interface                                                     | Description |
|---------------------------------------------------------------|-------------|
| [`EventListener`](./classes/Cassandra/EventListener.md)       |             |
| [`WarningsListener`](./classes/Cassandra/WarningsListener.md) |             |

### \Cassandra\Compression

#### Classes

| Class                                                                   | Description                                                                                                                                                                        |
|-------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`Lz4Compressor`](./classes/Cassandra/Compression/Lz4Compressor.md)     | Pure-PHP LZ4 block compressor.                                                                                                                                                     |
| [`Lz4Decompressor`](./classes/Cassandra/Compression/Lz4Decompressor.md) |                                                                                                                                                                                    |
| [`Lz4Extension`](./classes/Cassandra/Compression/Lz4Extension.md)       | Thin adapter over the optional native LZ4 PHP extension (the `lz4` PECL
extension, e.g. kjdev/php-ext-lz4), used to accelerate raw LZ4 block
(de)compression when it is installed. |

### \Cassandra\Connection

#### Classes

| Class                                                                        | Description                                                                                        |
|------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| [`ConnectionOptions`](./classes/Cassandra/Connection/ConnectionOptions.md)   |                                                                                                    |
| [`FrameCodec`](./classes/Cassandra/Connection/FrameCodec.md)                 |                                                                                                    |
| [`NodeConfig`](./classes/Cassandra/Connection/NodeConfig.md)                 |                                                                                                    |
| [`NodeHealth`](./classes/Cassandra/Connection/NodeHealth.md)                 |                                                                                                    |
| [`NodeImplementation`](./classes/Cassandra/Connection/NodeImplementation.md) |                                                                                                    |
| [`RandomSelector`](./classes/Cassandra/Connection/RandomSelector.md)         |                                                                                                    |
| [`RequestCompressor`](./classes/Cassandra/Connection/RequestCompressor.md)   | Node decorator that LZ4-compresses outgoing request frames on the legacy
(protocol v3/v4) framing. |
| [`ResponseReader`](./classes/Cassandra/Connection/ResponseReader.md)         |                                                                                                    |
| [`RoundRobinSelector`](./classes/Cassandra/Connection/RoundRobinSelector.md) |                                                                                                    |
| [`Socket`](./classes/Cassandra/Connection/Socket.md)                         |                                                                                                    |
| [`SocketNodeConfig`](./classes/Cassandra/Connection/SocketNodeConfig.md)     |                                                                                                    |
| [`Stream`](./classes/Cassandra/Connection/Stream.md)                         |                                                                                                    |
| [`StreamNodeConfig`](./classes/Cassandra/Connection/StreamNodeConfig.md)     |                                                                                                    |

#### Enums

| Enum                                                                               | Description |
|------------------------------------------------------------------------------------|-------------|
| [`NodeSelectionStrategy`](./classes/Cassandra/Connection/NodeSelectionStrategy.md) |             |

#### Interfaces

| Interface                                                        | Description |
|------------------------------------------------------------------|-------------|
| [`IoNode`](./classes/Cassandra/Connection/IoNode.md)             |             |
| [`Node`](./classes/Cassandra/Connection/Node.md)                 |             |
| [`NodeSelector`](./classes/Cassandra/Connection/NodeSelector.md) |             |

### \Cassandra\Exception

#### Classes

| Class                                                                                 | Description                                                       |
|---------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| [`CassandraException`](./classes/Cassandra/Exception/CassandraException.md)           |                                                                   |
| [`CompressionException`](./classes/Cassandra/Exception/CompressionException.md)       |                                                                   |
| [`ConnectionException`](./classes/Cassandra/Exception/ConnectionException.md)         |                                                                   |
| [`NodeException`](./classes/Cassandra/Exception/NodeException.md)                     |                                                                   |
| [`RequestException`](./classes/Cassandra/Exception/RequestException.md)               |                                                                   |
| [`RequestTimeoutException`](./classes/Cassandra/Exception/RequestTimeoutException.md) | The server did not answer within the client-side request timeout. |
| [`ResponseException`](./classes/Cassandra/Exception/ResponseException.md)             |                                                                   |
| [`ServerException`](./classes/Cassandra/Exception/ServerException.md)                 |                                                                   |
| [`SocketException`](./classes/Cassandra/Exception/SocketException.md)                 |                                                                   |
| [`StatementException`](./classes/Cassandra/Exception/StatementException.md)           |                                                                   |
| [`StreamException`](./classes/Cassandra/Exception/StreamException.md)                 |                                                                   |
| [`StringMathException`](./classes/Cassandra/Exception/StringMathException.md)         |                                                                   |
| [`TypeInfoException`](./classes/Cassandra/Exception/TypeInfoException.md)             |                                                                   |
| [`TypeNameParserException`](./classes/Cassandra/Exception/TypeNameParserException.md) |                                                                   |
| [`ValueException`](./classes/Cassandra/Exception/ValueException.md)                   |                                                                   |
| [`ValueFactoryException`](./classes/Cassandra/Exception/ValueFactoryException.md)     |                                                                   |
| [`VIntCodecException`](./classes/Cassandra/Exception/VIntCodecException.md)           |                                                                   |

#### Enums

| Enum                                                              | Description                                                                      |
|-------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`ExceptionCode`](./classes/Cassandra/Exception/ExceptionCode.md) | Global enumeration of all exception codes used throughout the Cassandra library. |

### \Cassandra\Exception\ServerException

#### Classes

| Class                                                                                                           | Description |
|-----------------------------------------------------------------------------------------------------------------|-------------|
| [`AlreadyExistsException`](./classes/Cassandra/Exception/ServerException/AlreadyExistsException.md)             |             |
| [`AuthenticationErrorException`](./classes/Cassandra/Exception/ServerException/AuthenticationErrorException.md) |             |
| [`CasWriteUnknownException`](./classes/Cassandra/Exception/ServerException/CasWriteUnknownException.md)         |             |
| [`CdcWriteFailureException`](./classes/Cassandra/Exception/ServerException/CdcWriteFailureException.md)         |             |
| [`ConfigErrorException`](./classes/Cassandra/Exception/ServerException/ConfigErrorException.md)                 |             |
| [`FunctionFailureException`](./classes/Cassandra/Exception/ServerException/FunctionFailureException.md)         |             |
| [`InvalidException`](./classes/Cassandra/Exception/ServerException/InvalidException.md)                         |             |
| [`IsBootstrappingException`](./classes/Cassandra/Exception/ServerException/IsBootstrappingException.md)         |             |
| [`OverloadedException`](./classes/Cassandra/Exception/ServerException/OverloadedException.md)                   |             |
| [`ProtocolErrorException`](./classes/Cassandra/Exception/ServerException/ProtocolErrorException.md)             |             |
| [`ReadFailureException`](./classes/Cassandra/Exception/ServerException/ReadFailureException.md)                 |             |
| [`ReadTimeoutException`](./classes/Cassandra/Exception/ServerException/ReadTimeoutException.md)                 |             |
| [`ServerErrorException`](./classes/Cassandra/Exception/ServerException/ServerErrorException.md)                 |             |
| [`SyntaxErrorException`](./classes/Cassandra/Exception/ServerException/SyntaxErrorException.md)                 |             |
| [`TruncateErrorException`](./classes/Cassandra/Exception/ServerException/TruncateErrorException.md)             |             |
| [`UnauthorizedException`](./classes/Cassandra/Exception/ServerException/UnauthorizedException.md)               |             |
| [`UnavailableException`](./classes/Cassandra/Exception/ServerException/UnavailableException.md)                 |             |
| [`UnpreparedException`](./classes/Cassandra/Exception/ServerException/UnpreparedException.md)                   |             |
| [`WriteFailureException`](./classes/Cassandra/Exception/ServerException/WriteFailureException.md)               |             |
| [`WriteTimeoutException`](./classes/Cassandra/Exception/ServerException/WriteTimeoutException.md)               |             |

### \Cassandra\Protocol

#### Classes

| Class                                              | Description |
|----------------------------------------------------|-------------|
| [`Flag`](./classes/Cassandra/Protocol/Flag.md)     |             |
| [`Header`](./classes/Cassandra/Protocol/Header.md) |             |

#### Enums

| Enum                                                                 | Description |
|----------------------------------------------------------------------|-------------|
| [`Opcode`](./classes/Cassandra/Protocol/Opcode.md)                   |             |
| [`ProtocolVersion`](./classes/Cassandra/Protocol/ProtocolVersion.md) |             |

#### Interfaces

| Interface                                        | Description |
|--------------------------------------------------|-------------|
| [`Frame`](./classes/Cassandra/Protocol/Frame.md) |             |

### \Cassandra\Request

#### Classes

| Class                                                         | Description |
|---------------------------------------------------------------|-------------|
| [`AuthResponse`](./classes/Cassandra/Request/AuthResponse.md) |             |
| [`Batch`](./classes/Cassandra/Request/Batch.md)               |             |
| [`Execute`](./classes/Cassandra/Request/Execute.md)           |             |
| [`Options`](./classes/Cassandra/Request/Options.md)           |             |
| [`Prepare`](./classes/Cassandra/Request/Prepare.md)           |             |
| [`PrepareFlag`](./classes/Cassandra/Request/PrepareFlag.md)   |             |
| [`Query`](./classes/Cassandra/Request/Query.md)               |             |
| [`QueryFlag`](./classes/Cassandra/Request/QueryFlag.md)       |             |
| [`Register`](./classes/Cassandra/Request/Register.md)         |             |
| [`Request`](./classes/Cassandra/Request/Request.md)           |             |
| [`Startup`](./classes/Cassandra/Request/Startup.md)           |             |

#### Enums

| Enum                                                    | Description |
|---------------------------------------------------------|-------------|
| [`BatchType`](./classes/Cassandra/Request/BatchType.md) |             |

### \Cassandra\Request\Options

#### Classes

| Class                                                                     | Description |
|---------------------------------------------------------------------------|-------------|
| [`BatchOptions`](./classes/Cassandra/Request/Options/BatchOptions.md)     |             |
| [`ExecuteOptions`](./classes/Cassandra/Request/Options/ExecuteOptions.md) |             |
| [`PrepareOptions`](./classes/Cassandra/Request/Options/PrepareOptions.md) |             |
| [`QueryOptions`](./classes/Cassandra/Request/Options/QueryOptions.md)     |             |
| [`RequestOptions`](./classes/Cassandra/Request/Options/RequestOptions.md) |             |

### \Cassandra\Response

#### Classes

| Class                                                                                | Description                              |
|--------------------------------------------------------------------------------------|------------------------------------------|
| [`AuthChallenge`](./classes/Cassandra/Response/AuthChallenge.md)                     |                                          |
| [`Authenticate`](./classes/Cassandra/Response/Authenticate.md)                       |                                          |
| [`AuthSuccess`](./classes/Cassandra/Response/AuthSuccess.md)                         |                                          |
| [`Error`](./classes/Cassandra/Response/Error.md)                                     | Indicates an error processing a request. |
| [`Event`](./classes/Cassandra/Response/Event.md)                                     |                                          |
| [`ProgressiveStreamReader`](./classes/Cassandra/Response/ProgressiveStreamReader.md) |                                          |
| [`Ready`](./classes/Cassandra/Response/Ready.md)                                     |                                          |
| [`Response`](./classes/Cassandra/Response/Response.md)                               |                                          |
| [`Result`](./classes/Cassandra/Response/Result.md)                                   |                                          |
| [`ResultFlag`](./classes/Cassandra/Response/ResultFlag.md)                           |                                          |
| [`ResultIterator`](./classes/Cassandra/Response/ResultIterator.md)                   |                                          |
| [`StreamReader`](./classes/Cassandra/Response/StreamReader.md)                       |                                          |
| [`Supported`](./classes/Cassandra/Response/Supported.md)                             |                                          |

#### Enums

| Enum                                                       | Description |
|------------------------------------------------------------|-------------|
| [`ErrorType`](./classes/Cassandra/Response/ErrorType.md)   |             |
| [`ResultKind`](./classes/Cassandra/Response/ResultKind.md) |             |

### \Cassandra\Response\Error

#### Classes

| Class                                                                                          | Description                              |
|------------------------------------------------------------------------------------------------|------------------------------------------|
| [`AlreadyExistsError`](./classes/Cassandra/Response/Error/AlreadyExistsError.md)               | Indicates an error processing a request. |
| [`AuthenticationError`](./classes/Cassandra/Response/Error/AuthenticationError.md)             | Indicates an error processing a request. |
| [`CasWriteUnknownError`](./classes/Cassandra/Response/Error/CasWriteUnknownError.md)           | Indicates an error processing a request. |
| [`CdcWriteFailureError`](./classes/Cassandra/Response/Error/CdcWriteFailureError.md)           | Indicates an error processing a request. |
| [`ConfigError`](./classes/Cassandra/Response/Error/ConfigError.md)                             | Indicates an error processing a request. |
| [`FunctionFailureError`](./classes/Cassandra/Response/Error/FunctionFailureError.md)           | Indicates an error processing a request. |
| [`InvalidError`](./classes/Cassandra/Response/Error/InvalidError.md)                           | Indicates an error processing a request. |
| [`IsBootstrappingError`](./classes/Cassandra/Response/Error/IsBootstrappingError.md)           | Indicates an error processing a request. |
| [`OverloadedError`](./classes/Cassandra/Response/Error/OverloadedError.md)                     | Indicates an error processing a request. |
| [`ProtocolError`](./classes/Cassandra/Response/Error/ProtocolError.md)                         | Indicates an error processing a request. |
| [`ReadFailureError`](./classes/Cassandra/Response/Error/ReadFailureError.md)                   | Indicates an error processing a request. |
| [`ReadTimeoutError`](./classes/Cassandra/Response/Error/ReadTimeoutError.md)                   | Indicates an error processing a request. |
| [`ServerError`](./classes/Cassandra/Response/Error/ServerError.md)                             | Indicates an error processing a request. |
| [`SyntaxError`](./classes/Cassandra/Response/Error/SyntaxError.md)                             | Indicates an error processing a request. |
| [`TruncateError`](./classes/Cassandra/Response/Error/TruncateError.md)                         | Indicates an error processing a request. |
| [`UnauthorizedError`](./classes/Cassandra/Response/Error/UnauthorizedError.md)                 | Indicates an error processing a request. |
| [`UnavailableExceptionError`](./classes/Cassandra/Response/Error/UnavailableExceptionError.md) | Indicates an error processing a request. |
| [`UnpreparedError`](./classes/Cassandra/Response/Error/UnpreparedError.md)                     | Indicates an error processing a request. |
| [`WriteFailureError`](./classes/Cassandra/Response/Error/WriteFailureError.md)                 | Indicates an error processing a request. |
| [`WriteTimeoutError`](./classes/Cassandra/Response/Error/WriteTimeoutError.md)                 | Indicates an error processing a request. |

### \Cassandra\Response\Error\Context

#### Classes

| Class                                                                                                      | Description |
|------------------------------------------------------------------------------------------------------------|-------------|
| [`AlreadyExistsContext`](./classes/Cassandra/Response/Error/Context/AlreadyExistsContext.md)               |             |
| [`CasWriteUnknownContext`](./classes/Cassandra/Response/Error/Context/CasWriteUnknownContext.md)           |             |
| [`ErrorContext`](./classes/Cassandra/Response/Error/Context/ErrorContext.md)                               |             |
| [`FunctionFailureContext`](./classes/Cassandra/Response/Error/Context/FunctionFailureContext.md)           |             |
| [`ReadFailureContext`](./classes/Cassandra/Response/Error/Context/ReadFailureContext.md)                   |             |
| [`ReadTimeoutContext`](./classes/Cassandra/Response/Error/Context/ReadTimeoutContext.md)                   |             |
| [`UnavailableExceptionContext`](./classes/Cassandra/Response/Error/Context/UnavailableExceptionContext.md) |             |
| [`UnpreparedContext`](./classes/Cassandra/Response/Error/Context/UnpreparedContext.md)                     |             |
| [`WriteFailureContext`](./classes/Cassandra/Response/Error/Context/WriteFailureContext.md)                 |             |
| [`WriteTimeoutContext`](./classes/Cassandra/Response/Error/Context/WriteTimeoutContext.md)                 |             |

#### Enums

| Enum                                                                   | Description |
|------------------------------------------------------------------------|-------------|
| [`WriteType`](./classes/Cassandra/Response/Error/Context/WriteType.md) |             |

### \Cassandra\Response\Event

#### Classes

| Class                                                                              | Description |
|------------------------------------------------------------------------------------|-------------|
| [`SchemaChangeEvent`](./classes/Cassandra/Response/Event/SchemaChangeEvent.md)     |             |
| [`StatusChangeEvent`](./classes/Cassandra/Response/Event/StatusChangeEvent.md)     |             |
| [`TopologyChangeEvent`](./classes/Cassandra/Response/Event/TopologyChangeEvent.md) |             |

### \Cassandra\Response\Event\Data

#### Classes

| Class                                                                                 | Description |
|---------------------------------------------------------------------------------------|-------------|
| [`EventData`](./classes/Cassandra/Response/Event/Data/EventData.md)                   |             |
| [`SchemaChangeData`](./classes/Cassandra/Response/Event/Data/SchemaChangeData.md)     |             |
| [`StatusChangeData`](./classes/Cassandra/Response/Event/Data/StatusChangeData.md)     |             |
| [`TopologyChangeData`](./classes/Cassandra/Response/Event/Data/TopologyChangeData.md) |             |

#### Enums

| Enum                                                                                  | Description |
|---------------------------------------------------------------------------------------|-------------|
| [`SchemaChangeTarget`](./classes/Cassandra/Response/Event/Data/SchemaChangeTarget.md) |             |
| [`SchemaChangeType`](./classes/Cassandra/Response/Event/Data/SchemaChangeType.md)     |             |
| [`StatusChangeType`](./classes/Cassandra/Response/Event/Data/StatusChangeType.md)     |             |
| [`TopologyChangeType`](./classes/Cassandra/Response/Event/Data/TopologyChangeType.md) |             |

### \Cassandra\Response\Result

#### Classes

| Class                                                                                 | Description |
|---------------------------------------------------------------------------------------|-------------|
| [`CachedPreparedResult`](./classes/Cassandra/Response/Result/CachedPreparedResult.md) |             |
| [`ColumnInfo`](./classes/Cassandra/Response/Result/ColumnInfo.md)                     |             |
| [`PreparedResult`](./classes/Cassandra/Response/Result/PreparedResult.md)             |             |
| [`PrepareMetadata`](./classes/Cassandra/Response/Result/PrepareMetadata.md)           |             |
| [`RowClass`](./classes/Cassandra/Response/Result/RowClass.md)                         |             |
| [`RowsMetadata`](./classes/Cassandra/Response/Result/RowsMetadata.md)                 |             |
| [`RowsResult`](./classes/Cassandra/Response/Result/RowsResult.md)                     |             |
| [`SchemaChangeResult`](./classes/Cassandra/Response/Result/SchemaChangeResult.md)     |             |
| [`SetKeyspaceResult`](./classes/Cassandra/Response/Result/SetKeyspaceResult.md)       |             |
| [`VoidResult`](./classes/Cassandra/Response/Result/VoidResult.md)                     |             |

#### Enums

| Enum                                                            | Description |
|-----------------------------------------------------------------|-------------|
| [`FetchType`](./classes/Cassandra/Response/Result/FetchType.md) |             |

#### Interfaces

| Interface                                                                       | Description |
|---------------------------------------------------------------------------------|-------------|
| [`RowClassInterface`](./classes/Cassandra/Response/Result/RowClassInterface.md) |             |

### \Cassandra\Response\Result\Data

#### Classes

| Class                                                                              | Description |
|------------------------------------------------------------------------------------|-------------|
| [`PreparedData`](./classes/Cassandra/Response/Result/Data/PreparedData.md)         |             |
| [`ResultData`](./classes/Cassandra/Response/Result/Data/ResultData.md)             |             |
| [`RowsData`](./classes/Cassandra/Response/Result/Data/RowsData.md)                 |             |
| [`SchemaChangeData`](./classes/Cassandra/Response/Result/Data/SchemaChangeData.md) |             |
| [`SetKeyspaceData`](./classes/Cassandra/Response/Result/Data/SetKeyspaceData.md)   |             |
| [`VoidData`](./classes/Cassandra/Response/Result/Data/VoidData.md)                 |             |

### \Cassandra\StringMath

#### Classes

| Class                                                                      | Description |
|----------------------------------------------------------------------------|-------------|
| [`DecimalCalculator`](./classes/Cassandra/StringMath/DecimalCalculator.md) |             |

### \Cassandra\StringMath\DecimalCalculator

#### Classes

| Class                                                                  | Description |
|------------------------------------------------------------------------|-------------|
| [`BCMath`](./classes/Cassandra/StringMath/DecimalCalculator/BCMath.md) |             |
| [`GMP`](./classes/Cassandra/StringMath/DecimalCalculator/GMP.md)       |             |
| [`Native`](./classes/Cassandra/StringMath/DecimalCalculator/Native.md) |             |

### \Cassandra\TypeInfo

#### Classes

| Class                                                                      | Description |
|----------------------------------------------------------------------------|-------------|
| [`CustomInfo`](./classes/Cassandra/TypeInfo/CustomInfo.md)                 |             |
| [`ListCollectionInfo`](./classes/Cassandra/TypeInfo/ListCollectionInfo.md) |             |
| [`MapCollectionInfo`](./classes/Cassandra/TypeInfo/MapCollectionInfo.md)   |             |
| [`SetCollectionInfo`](./classes/Cassandra/TypeInfo/SetCollectionInfo.md)   |             |
| [`SimpleTypeInfo`](./classes/Cassandra/TypeInfo/SimpleTypeInfo.md)         |             |
| [`TupleInfo`](./classes/Cassandra/TypeInfo/TupleInfo.md)                   |             |
| [`TypeInfo`](./classes/Cassandra/TypeInfo/TypeInfo.md)                     |             |
| [`UDTInfo`](./classes/Cassandra/TypeInfo/UDTInfo.md)                       |             |
| [`VectorInfo`](./classes/Cassandra/TypeInfo/VectorInfo.md)                 |             |

### \Cassandra\Value

#### Classes

| Class                                                                                   | Description                                                                                              |
|-----------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| [`Ascii`](./classes/Cassandra/Value/Ascii.md)                                           |                                                                                                          |
| [`Bigint`](./classes/Cassandra/Value/Bigint.md)                                         |                                                                                                          |
| [`Blob`](./classes/Cassandra/Value/Blob.md)                                             |                                                                                                          |
| [`Boolean`](./classes/Cassandra/Value/Boolean.md)                                       |                                                                                                          |
| [`Counter`](./classes/Cassandra/Value/Counter.md)                                       |                                                                                                          |
| [`Custom`](./classes/Cassandra/Value/Custom.md)                                         |                                                                                                          |
| [`Date`](./classes/Cassandra/Value/Date.md)                                             |                                                                                                          |
| [`Decimal`](./classes/Cassandra/Value/Decimal.md)                                       |                                                                                                          |
| [`Double`](./classes/Cassandra/Value/Double.md)                                         | Double-precision floating-point number (same as a PHP "float", 64-bit precision)                         |
| [`Duration`](./classes/Cassandra/Value/Duration.md)                                     |                                                                                                          |
| [`Float32`](./classes/Cassandra/Value/Float32.md)                                       | Single-precision floating-point number (32-bit precision - use the "Double" type for a PHP-like "float") |
| [`Inet`](./classes/Cassandra/Value/Inet.md)                                             |                                                                                                          |
| [`Int32`](./classes/Cassandra/Value/Int32.md)                                           |                                                                                                          |
| [`ListCollection`](./classes/Cassandra/Value/ListCollection.md)                         |                                                                                                          |
| [`MapCollection`](./classes/Cassandra/Value/MapCollection.md)                           |                                                                                                          |
| [`NotSet`](./classes/Cassandra/Value/NotSet.md)                                         |                                                                                                          |
| [`SetCollection`](./classes/Cassandra/Value/SetCollection.md)                           |                                                                                                          |
| [`Smallint`](./classes/Cassandra/Value/Smallint.md)                                     |                                                                                                          |
| [`Time`](./classes/Cassandra/Value/Time.md)                                             |                                                                                                          |
| [`Timestamp`](./classes/Cassandra/Value/Timestamp.md)                                   |                                                                                                          |
| [`Timeuuid`](./classes/Cassandra/Value/Timeuuid.md)                                     |                                                                                                          |
| [`Tinyint`](./classes/Cassandra/Value/Tinyint.md)                                       |                                                                                                          |
| [`Tuple`](./classes/Cassandra/Value/Tuple.md)                                           |                                                                                                          |
| [`UDT`](./classes/Cassandra/Value/UDT.md)                                               |                                                                                                          |
| [`Uuid`](./classes/Cassandra/Value/Uuid.md)                                             |                                                                                                          |
| [`ValueBase`](./classes/Cassandra/Value/ValueBase.md)                                   |                                                                                                          |
| [`ValueEncodeConfig`](./classes/Cassandra/Value/ValueEncodeConfig.md)                   |                                                                                                          |
| [`ValueReadableWithLength`](./classes/Cassandra/Value/ValueReadableWithLength.md)       |                                                                                                          |
| [`ValueReadableWithoutLength`](./classes/Cassandra/Value/ValueReadableWithoutLength.md) |                                                                                                          |
| [`ValueWithFixedLength`](./classes/Cassandra/Value/ValueWithFixedLength.md)             |                                                                                                          |
| [`Varchar`](./classes/Cassandra/Value/Varchar.md)                                       |                                                                                                          |
| [`Varint`](./classes/Cassandra/Value/Varint.md)                                         |                                                                                                          |
| [`Vector`](./classes/Cassandra/Value/Vector.md)                                         |                                                                                                          |

#### Interfaces

| Interface                                                                               | Description |
|-----------------------------------------------------------------------------------------|-------------|
| [`ValueWithMultipleEncodings`](./classes/Cassandra/Value/ValueWithMultipleEncodings.md) |             |

### \Cassandra\Value\EncodeOption

#### Enums

| Enum                                                                                       | Description |
|--------------------------------------------------------------------------------------------|-------------|
| [`DateEncodeOption`](./classes/Cassandra/Value/EncodeOption/DateEncodeOption.md)           |             |
| [`DurationEncodeOption`](./classes/Cassandra/Value/EncodeOption/DurationEncodeOption.md)   |             |
| [`TimeEncodeOption`](./classes/Cassandra/Value/EncodeOption/TimeEncodeOption.md)           |             |
| [`TimestampEncodeOption`](./classes/Cassandra/Value/EncodeOption/TimestampEncodeOption.md) |             |
| [`UuidEncodeOption`](./classes/Cassandra/Value/EncodeOption/UuidEncodeOption.md)           |             |
| [`VarintEncodeOption`](./classes/Cassandra/Value/EncodeOption/VarintEncodeOption.md)       |             |
