<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration\Data\Trait;

trait CompareWithCqlsh {
    /**
     * @param array<mixed> $testValues
     */
    protected function compareWithCqlsh(string $tableName, string $idColumn, string $valueColumn, array $testValues, string $dataType): void {

        $cqlshResults = $this->dumpTableWithCqlsh($this->keyspace, $tableName, $idColumn, $valueColumn);

        foreach ($testValues as $idValue => $phpValue) {
            if (!array_key_exists($idValue, $cqlshResults)) {
                $this->fail("No cqlsh result found for ID {$idValue} in table {$tableName}");
            }

            $this->assertIsArray($cqlshResults[$idValue]);
            if (!array_key_exists($valueColumn, $cqlshResults[$idValue])) {
                $this->fail("Column '{$valueColumn}' not found in cqlsh result for ID {$idValue} in table {$tableName}");
            }

            $cqlshValue = $cqlshResults[$idValue][$valueColumn];

            if ($phpValue === null) {
                $this->assertSame(null, $cqlshValue, 'PHP string value should match cqlsh output');

                continue;
            }

            switch ($dataType) {
                case 'ascii':
                case 'varchar':
                    $this->assertIsString($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->assertSame((string) $phpValue, (string) $cqlshValue, 'PHP string value should match cqlsh output');

                    break;
                case 'bigint':
                case 'integer':
                case 'smallint':
                case 'tinyint':
                    $this->assertIsInt($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->assertSame((string) $phpValue, (string) $cqlshValue, 'PHP integer value should match cqlsh output');

                    break;
                case 'blob':
                    $this->assertIsString($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->assertSame('0x' . bin2hex((string) $phpValue), (string) $cqlshValue, 'PHP blob value should match cqlsh hex output');

                    break;
                case 'boolean':
                    $this->assertIsBool($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->assertSame(((bool) $phpValue) ? 'True' : 'False', (string) $cqlshValue, 'PHP boolean value should match cqlsh output');

                    break;
                case 'uuid':
                    $this->assertIsString($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->assertSame(strtolower((string) $phpValue), strtolower((string) $cqlshValue), 'PHP UUID value should match cqlsh output');

                    break;
                case 'float':
                    $this->assertIsFloat($phpValue);
                    $this->assertIsNumeric($cqlshValue);
                    $phpFloat = (float) $phpValue;
                    $cqlFloat = (float) $cqlshValue;
                    $this->assertEqualsWithDelta($phpFloat, $cqlFloat, max(abs($phpFloat) * 0.01, 0.0001), 'PHP float value should match cqlsh output');

                    break;
                case 'double':
                    $this->assertIsFloat($phpValue);
                    $this->assertIsNumeric($cqlshValue);
                    $phpDouble = (float) $phpValue;
                    $cqlDouble = (float) $cqlshValue;
                    $this->assertEqualsWithDelta($phpDouble, $cqlDouble, max(abs($phpDouble) * 0.01, 0.000001), 'PHP float value should match cqlsh output');

                    break;
                case 'list':
                case 'vector':
                    if ($cqlshValue === null) {
                        $cqlshValue = '';
                    }
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlListMatchesPhpList($phpValue, $cqlshValue);

                    break;
                case 'list_frozen':
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlListMatchesPhpList($phpValue, $cqlshValue, frozen: true);

                    break;
                case 'set':
                    if ($cqlshValue === null) {
                        $cqlshValue = '';
                    }
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlSetMatchesPhpSet($phpValue, $cqlshValue);

                    break;
                case 'set_frozen':
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlSetMatchesPhpSet($phpValue, $cqlshValue, frozen: true);

                    break;
                case 'map':
                    if ($cqlshValue === null) {
                        $cqlshValue = '';
                    }
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlMapMatchesPhpMap($phpValue, $cqlshValue);

                    break;
                case 'map_frozen':
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlMapMatchesPhpMap($phpValue, $cqlshValue, frozen: true);

                    break;
                case 'udt':
                    if ($cqlshValue === null) {
                        $cqlshValue = '';
                    }
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlUdtMatchesPhpUdt($phpValue, $cqlshValue);

                    break;
                case 'udt_frozen':
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlUdtMatchesPhpUdt($phpValue, $cqlshValue, frozen: true);

                    break;
                case 'tuple':
                    if ($cqlshValue === null) {
                        $cqlshValue = '';
                    }
                    $this->assertIsArray($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->verifyCqlTupleMatchesPhpTuple($phpValue, $cqlshValue);

                    break;
                case 'timestamp':
                    $this->assertIsString($cqlshValue);

                    if (is_array($phpValue)) {
                        $this->assertContains($cqlshValue, $phpValue, 'PHP timestamp value should match cqlsh output');
                    } else {
                        $this->assertIsString($phpValue);
                        $this->assertSame($phpValue, $cqlshValue, 'PHP timestamp value should match cqlsh output');
                    }

                    break;
                default:
                    $this->assertIsString($phpValue);
                    $this->assertIsString($cqlshValue);
                    $this->assertSame((string) $phpValue, (string) $cqlshValue, 'PHP value should match cqlsh output');

                    break;
            }
        }
    }

    /**
     * @return array<mixed>
     */
    protected function dumpTableWithCqlsh(string $keyspace, string $tableName, string $idColumn, string $valueColumn): array {

        $dumpFile = './table_dump.csv';

        if (file_exists($dumpFile)) {
            unlink($dumpFile);
        }

        $options = [
            'HEADER' => 'TRUE',
            'DELIMITER' => '|',
            'QUOTE' => '"',
            'ESCAPE' => '?',
            'NULL' => '__NULL__',
            'DATETIMEFORMAT' => '%Y-%m-%d %H:%M:%S.%f%z',
            'DECIMALSEP' => '.',
            'PAGESIZE' => '100',
            'ENCODING' => 'UTF8',
            'NUMPROCESSES' => '1',
        ];

        $optionsString = implode(' AND ', array_map(fn($k, $v) => "{$k} = '{$v}'", array_keys($options), $options));

        $columnList = [$idColumn, $valueColumn];
        $query = "COPY {$keyspace}.\"{$tableName}\" (" . implode(',', $columnList) . ") TO '____DUMP_FILE____' WITH " . $optionsString . ' ;';
        $escapedQuery = escapeshellarg($query);
        $cqlshArguments = "-k {$keyspace} -e {$escapedQuery}";

        $this->runCqlSh($cqlshArguments, $dumpFile);

        $rowData = [];
        $handle = fopen($dumpFile, 'r');
        if ($handle === false) {
            $this->fail('Failed to open CSV file for reading: ' . $dumpFile);
        }

        $headerRaw = fgetcsv(
            stream: $handle,
            length: 0,
            separator: $options['DELIMITER'],
            enclosure: $options['QUOTE'],
            escape: $options['ESCAPE']
        );

        $this->assertIsArray($headerRaw);
        $header = array_map(fn($value) => (string) $value, $headerRaw);

        while (($row = fgetcsv(
            stream: $handle,
            length: 0,
            separator: $options['DELIMITER'],
            enclosure: $options['QUOTE'],
            escape: $options['ESCAPE']
        )) !== false) {

            $row = array_map(fn($value) => str_replace([
                $options['NULL'],
                $options['ESCAPE'] . $options['QUOTE'],
                $options['ESCAPE'] . $options['ESCAPE'],
                '\\\\',
            ], [
                '______NULL______',
                $options['QUOTE'],
                $options['ESCAPE'],
                '______ESCAPE______',
            ], $value ?? ''), $row);

            $row = array_map(fn($value) => str_replace([
                '\\t',
                '\\n',
            ], [
                "\t",
                "\n",
            ], $value), $row);

            $row = array_map(fn($value) => str_replace([
                '______ESCAPE______',
            ], [
                '\\',
            ], $value), $row);

            $row = array_map(function ($value) {
                if ($value === '______NULL______') {
                    return null;
                }

                return str_replace('______NULL______', 'null', $value);
            }, $row);

            $rowData[] = array_combine($header, $row);
        }

        fclose($handle);

        if (file_exists($dumpFile)) {
            unlink($dumpFile);
        }

        $rows = [];
        foreach ($rowData as $row) {
            $rows[$row[$idColumn]] = $row;
        }

        return $rows;
    }

    protected function runCqlSh(string $cqlshArguments, string $dumpFile): void {
        static $useDocker = null;
        if ($useDocker === null) {
            if (shell_exec('which cqlsh') !== null) {
                $useDocker = false;
            } else {
                $useDocker = true;
            }
        }
        if ($useDocker) {
            $this->runCqlShInDocker("cqlsh {$cqlshArguments} localhost 9042 2>&1", $dumpFile);
        } else {
            $port = getenv('APP_CASSANDRA_PORT') ?: '9142';
            $this->runCqlShInCqlSh("cqlsh {$cqlshArguments} localhost {$port} 2>&1", $dumpFile);
        }
    }

    protected function runCqlShInCqlSh(string $command, string $dumpFile): void {

        $command = str_replace('____DUMP_FILE____', $dumpFile, $command);

        $output = shell_exec($command);
        if ($output === null || $output === false) {
            $this->fail("Failed to execute cqlsh command in container: {$command}");
        }

        if (
            str_contains($output, 'Connection error')
            || str_contains($output, 'SyntaxException')
            || str_contains($output, 'InvalidRequest')
            || !str_contains($output, 'rows exported to 1 files')
        ) {
            $this->fail("cqlsh command failed: {$command}\nOutput: {$output}");
        }
    }

    protected function runCqlShInDocker(string $command, string $dumpFile): void {

        $containerName = 'php-cassandra-test-db';

        $containerCheck = shell_exec("docker ps --filter name={$containerName} --format '{{.Names}}' 2>/dev/null");
        $this->assertIsString($containerCheck);

        if (trim($containerCheck) !== $containerName) {
            $this->fail("Cassandra container '{$containerName}' is not running. Please start with 'docker-compose up -d'");
        }

        $command = str_replace('____DUMP_FILE____', '/tmp/table_dump.csv', $command);

        $dockerCommand = "docker exec {$containerName} {$command}";

        $output = shell_exec($dockerCommand);
        if ($output === null || $output === false) {
            $this->fail("Failed to execute cqlsh command in container: {$dockerCommand}");
        }

        if (
            str_contains($output, 'Connection error')
            || str_contains($output, 'SyntaxException')
            || str_contains($output, 'InvalidRequest')
            || !str_contains($output, 'rows exported to 1 files')
        ) {
            $this->fail("cqlsh command failed: {$dockerCommand}\nOutput: {$output}");
        }

        shell_exec("docker cp {$containerName}:/tmp/table_dump.csv {$dumpFile}");
    }

    /**
     * @param array<mixed> $phpValue
     */
    protected function verifyCqlListMatchesPhpList(array $phpValue, string $cqlshValue, bool $frozen = false): void {

        if ($phpValue === []) {
            $this->assertSame($frozen ? '[]' : '', $cqlshValue, 'PHP list value should match cqlsh output');

            return;
        }

        // convert to json
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        sort($phpValue);

        $this->assertIsArray($cqlshArray);
        sort($cqlshArray);

        $this->assertSame($phpValue, $cqlshArray, 'PHP list value should match cqlsh output');
    }

    /**
     * @param array<mixed> $phpValue
     */
    protected function verifyCqlMapMatchesPhpMap(array $phpValue, string $cqlshValue, bool $frozen = false): void {

        if ($phpValue === []) {
            $this->assertSame($frozen ? '{}' : '', $cqlshValue, 'PHP map value should match cqlsh output');

            return;
        }

        // fix keys
        $cqlshValue = preg_replace('/([a-zA-Z0-9-_]+):/', '"$1":', $cqlshValue) ?? '';
        $cqlshValue = str_replace(': ,', ': null,', $cqlshValue);

        // convert to json
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        sort($phpValue);

        $this->assertIsArray($cqlshArray);
        sort($cqlshArray);

        $this->assertSame($phpValue, $cqlshArray, 'PHP map value should match cqlsh output');
    }

    /**
     * @param array<mixed> $phpValue
     */
    protected function verifyCqlSetMatchesPhpSet(array $phpValue, string $cqlshValue, bool $frozen = false): void {

        if ($phpValue === []) {
            $this->assertSame($frozen ? '{}' : '', $cqlshValue, 'PHP set value should match cqlsh output');

            return;
        }

        // convert to json
        $cqlshValue = str_replace(['{', '}'], ['[', ']'], $cqlshValue);
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        sort($phpValue);

        $this->assertIsArray($cqlshArray);
        sort($cqlshArray);

        $this->assertSame($phpValue, $cqlshArray, 'PHP set value should match cqlsh output');
    }

    /**
     * @param array<mixed> $phpValue
     */
    protected function verifyCqlTupleMatchesPhpTuple(array $phpValue, string $cqlshValue): void {

        if ($phpValue === []) {
            $this->assertSame('', $cqlshValue, 'PHP tuple value should match cqlsh output');

            return;
        }

        // convert to json
        $cqlshValue = str_replace(['(', ')'], ['[', ']'], $cqlshValue);
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        sort($phpValue);

        $this->assertIsArray($cqlshArray);
        sort($cqlshArray);

        $this->assertSame($phpValue, $cqlshArray, 'PHP tuple value should match cqlsh output');
    }

    /**
     * @param array<mixed> $phpValue
     */
    protected function verifyCqlUdtMatchesPhpUdt(array $phpValue, string $cqlshValue, bool $frozen = false): void {

        $allFieldsNull = true;
        foreach ($phpValue as $field) {
            if ($field !== null) {
                $allFieldsNull = false;

                break;
            }
        }

        if ($allFieldsNull) {
            $this->assertSame($frozen ? '{}' : '', $cqlshValue, 'PHP UDT value should match cqlsh output');

            return;
        }

        // fix keys
        $cqlshValue = preg_replace('/([a-zA-Z0-9-_]+):/', '"$1":', $cqlshValue) ?? '';
        $cqlshValue = str_replace(': ,', ': null,', $cqlshValue);

        // convert to json
        $cqlshValue = str_replace("'", '"', $cqlshValue);
        $cqlshValue = str_replace(['True', 'False'], ['true', 'false'], $cqlshValue);

        $cqlshArray = json_decode($cqlshValue, true);

        if ($phpValue) {
            sort($phpValue);
        }

        if ($cqlshArray !== null) {
            $this->assertIsArray($cqlshArray);
            sort($cqlshArray);
        }

        $this->assertSame($phpValue, $cqlshArray, 'PHP UDT value should match cqlsh output');
    }
}
