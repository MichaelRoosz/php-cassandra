#!/usr/bin/env php
<?php

declare(strict_types=1);

/*
 * Validate the pure-PHP Cassandra\Compression\Lz4Compressor against a proven,
 * external LZ4 implementation (the reference `lz4` CLI, or the python-lz4
 * library) — NOT against our own decompressor.
 *
 * For every test input the script:
 *   1. compresses it with our Lz4Compressor (raw LZ4 block),
 *   2. wraps the block(s) in a standard LZ4 frame,
 *   3. decompresses that frame with the external tool, and
 *   4. asserts the result is byte-for-byte identical to the input.
 *
 * If the external tool can decode what we produced, our compressor emits valid,
 * standards-compliant LZ4.
 *
 * Test inputs are a battery of generated edge cases plus any files or
 * directories passed as arguments — point it at a real corpus (Canterbury,
 * Silesia, enwik8, ...) for "proven good" real-world data:
 *
 *   php scripts/lz4/validate-compressor.php [--max-bytes=N] [path ...]
 *
 * Exit codes: 0 = all passed, 1 = at least one mismatch, 2 = no external LZ4
 * tool found (cannot validate).
 */

use Cassandra\Compression\Lz4Compressor;

$autoloadCandidates = [
    __DIR__ . '/../../vendor/autoload.php',
    __DIR__ . '/../../../../autoload.php',
];
$autoloadFound = false;
foreach ($autoloadCandidates as $autoload) {
    if (is_file($autoload)) {
        require $autoload;
        $autoloadFound = true;

        break;
    }
}
if (!$autoloadFound) {
    fwrite(STDERR, "Could not locate composer autoloader. Run `composer install` first.\n");
    exit(2);
}

/**
 * Thin wrapper around whichever proven external LZ4 decompressor is available.
 */
final class ExternalLz4Decoder {
    private function __construct(
        private string $name,
        /** @var list<string> $argvPrefix command that reads a .lz4 file arg and writes the decoded bytes to stdout */
        private array $argvPrefix,
    ) {
    }

    /**
     * Decode an LZ4 frame with the external tool. Throws on any tool error.
     *
     * @throws \RuntimeException
     */
    public function decode(string $frame): string {
        $inputPath = tempnam(sys_get_temp_dir(), 'lz4in_');
        if ($inputPath === false) {
            throw new RuntimeException('Unable to create temp file');
        }

        try {
            file_put_contents($inputPath, $frame);

            $argv = $this->argvPrefix;
            $argv[] = $inputPath;

            $descriptors = [
                0 => ['file', '/dev/null', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ];

            $process = proc_open($argv, $descriptors, $pipes);
            if (!is_resource($process)) {
                throw new RuntimeException('Unable to launch external decoder');
            }

            $stdout = stream_get_contents($pipes[1]);
            $stderr = stream_get_contents($pipes[2]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            $exitCode = proc_close($process);

            if ($exitCode !== 0) {
                throw new RuntimeException(sprintf(
                    'External decoder exited with code %d: %s',
                    $exitCode,
                    trim((string) $stderr)
                ));
            }

            return (string) $stdout;
        } finally {
            @unlink($inputPath);
        }
    }

    public static function detect(): ?self {
        // 1) Reference lz4 CLI (Yann Collet's implementation).
        $version = @shell_exec('lz4 --version 2>/dev/null');
        if (is_string($version) && stripos($version, 'lz4') !== false) {
            return new self(
                name: 'lz4 CLI (' . trim($version) . ')',
                argvPrefix: ['lz4', '-d', '-c'],
            );
        }

        // 2) python-lz4 (binds the reference C library).
        $ok = @shell_exec('python3 -c "import lz4.frame" 2>/dev/null; echo $?');
        if (is_string($ok) && trim($ok) === '0') {
            return new self(
                name: 'python-lz4 (lz4.frame)',
                argvPrefix: ['python3', '-c', 'import sys,lz4.frame; sys.stdout.buffer.write(lz4.frame.decompress(open(sys.argv[1],"rb").read()))'],
            );
        }

        return null;
    }

    public function name(): string {
        return $this->name;
    }
}

/**
 * @param list<string> $paths
 * @return array<string, string> map of test-case label => raw bytes
 *
 * @throws \Random\RandomException
 * @throws \UnexpectedValueException
 */
function collectTestInputs(array $paths, int $maxBytes): array {
    $maxBytes = max(0, $maxBytes);

    $inputs = generatedEdgeCases();

    foreach (defaultRealFiles() as $label => $file) {
        if (is_file($file) && is_readable($file)) {
            $inputs['real: ' . $label] = (string) file_get_contents($file, length: $maxBytes);
        }
    }

    foreach ($paths as $path) {
        foreach (expandPath($path) as $file) {
            $size = filesize($file);
            if ($size === false || $size === 0) {
                continue;
            }
            $inputs['file: ' . $file] = (string) file_get_contents($file, length: $maxBytes);
        }
    }

    return $inputs;
}

/**
 * @return array<string, string>
 *
 * @throws \Random\RandomException
 */
function generatedEdgeCases(): array {
    $cases = [
        'empty' => '',
        'one byte' => 'x',
        'below min-match (3)' => 'abc',
        'exactly min-match (4)' => 'abcd',
        'short text' => 'hello lz4 world',
        'all zeros 64k' => str_repeat("\x00", 64 * 1024),
        'repetitive token' => str_repeat('0123456789abcdef', 8192),
        'natural text' => str_repeat('The quick brown fox jumps over the lazy dog. ', 4000),
        'json-ish' => (string) json_encode(array_fill(0, 4000, ['id' => 42, 'name' => 'value', 'ok' => true])),
    ];

    // Sizes around important boundaries: the 64 KiB match window, the Cassandra
    // 128 KiB frame payload, and the 4 MiB LZ4 frame block-max.
    foreach ([65535, 65536, 65537, 131071, 131072, 131073, 4 * 1024 * 1024 + 1] as $size) {
        $cases["compressible {$size}B"] = substr(str_repeat('lz4-boundary-test;', (int) ceil($size / 18)), 0, $size);
        $cases["incompressible {$size}B"] = random_bytes($size);
    }

    return $cases;
}

/**
 * @return array<string, string>
 *
 * @throws \UnexpectedValueException
 */
function defaultRealFiles(): array {
    $root = dirname(__DIR__, 2);

    $files = [
        'composer.lock (json)' => $root . '/composer.lock',
        'README.md' => $root . '/README.md',
        'CHANGELOG.md' => $root . '/CHANGELOG.md',
        'php binary' => PHP_BINARY,
    ];

    // A realistic several-hundred-KB text blob: all of the library's PHP source.
    $concatenated = '';
    $srcDir = $root . '/src';
    if (is_dir($srcDir)) {
        $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($srcDir, FilesystemIterator::SKIP_DOTS));
        foreach ($iterator as $entry) {
            if ($entry instanceof SplFileInfo && $entry->isFile() && $entry->getExtension() === 'php') {
                $concatenated .= (string) file_get_contents($entry->getPathname());
            }
        }
    }
    if ($concatenated !== '') {
        $tmp = tempnam(sys_get_temp_dir(), 'lz4src_');
        if ($tmp !== false) {
            file_put_contents($tmp, $concatenated);
            register_shutdown_function(static fn () => @unlink($tmp));
            $files['concatenated src/*.php'] = $tmp;
        }
    }

    return $files;
}

/**
 * @return list<string>
 *
 * @throws \UnexpectedValueException
 */
function expandPath(string $path): array {
    if (is_file($path)) {
        return [$path];
    }

    if (is_dir($path)) {
        $files = [];
        $iterator = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($path, FilesystemIterator::SKIP_DOTS));
        foreach ($iterator as $entry) {
            if ($entry instanceof SplFileInfo && $entry->isFile()) {
                $files[] = $entry->getPathname();
            }
        }
        sort($files);

        return $files;
    }

    fwrite(STDERR, "warning: path not found, skipping: {$path}\n");

    return [];
}

// --- main -------------------------------------------------------------------

$maxBytes = 16 * 1024 * 1024;
$paths = [];
foreach (array_slice($argv ?? [], 1) as $arg) {
    if (preg_match('/^--max-bytes=(\d+)$/', $arg, $m)) {
        $maxBytes = (int) $m[1];
    } elseif ($arg === '--help' || $arg === '-h') {
        fwrite(STDOUT, "Usage: php scripts/lz4/validate-compressor.php [--max-bytes=N] [path ...]\n");
        exit(0);
    } else {
        $paths[] = $arg;
    }
}

$decoder = ExternalLz4Decoder::detect();
if ($decoder === null) {
    fwrite(STDERR, "No proven external LZ4 decompressor found.\n");
    fwrite(STDERR, "Install one of:\n");
    fwrite(STDERR, "  - the reference lz4 CLI   (Debian/Ubuntu: apt-get install lz4, macOS: brew install lz4)\n");
    fwrite(STDERR, "  - python-lz4              (pip install lz4)\n");
    exit(2);
}

$compressor = new Lz4Compressor(preferExtension: false);

fwrite(STDOUT, "Validating Cassandra\\Compression\\Lz4Compressor against: {$decoder->name()}\n\n");

$inputs = collectTestInputs($paths, $maxBytes);

$passed = 0;
$failed = 0;
$totalIn = 0;
$totalCompressed = 0;

foreach ($inputs as $label => $data) {
    $frame = $compressor->compress($data);

    try {
        $decoded = $decoder->decode($frame);
    } catch (RuntimeException $e) {
        $failed++;
        printf("  FAIL  %-40s decoder error: %s\n", trLeft($label), $e->getMessage());

        continue;
    }

    if ($decoded === $data) {
        $passed++;
        $totalIn += strlen($data);
        $totalCompressed += strlen($frame);
        $ratio = strlen($data) > 0 ? strlen($frame) / strlen($data) : 0.0;
        printf("  ok    %-40s %10d B -> %10d B (%.3f)\n", trLeft($label), strlen($data), strlen($frame), $ratio);
    } else {
        $failed++;
        printf("  FAIL  %-40s decoded %d B, expected %d B\n", trLeft($label), strlen($decoded), strlen($data));
    }
}

function trLeft(string $s, int $max = 40): string {
    return strlen($s) <= $max ? $s : '...' . substr($s, -($max - 3));
}

printf("\n%d passed, %d failed, %d total\n", $passed, $failed, $passed + $failed);
if ($totalIn > 0) {
    printf("aggregate: %d B -> %d B (%.3f)\n", $totalIn, $totalCompressed, $totalCompressed / $totalIn);
}

exit($failed === 0 ? 0 : 1);
