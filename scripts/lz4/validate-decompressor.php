#!/usr/bin/env php
<?php

declare(strict_types=1);

/*
 * Validate the pure-PHP Cassandra\Compression\Lz4Decompressor against a proven,
 * external LZ4 implementation (the reference `lz4` CLI, or the python-lz4
 * library) — NOT against our own compressor.
 *
 * For every test input the script:
 *   1. compresses it with the external tool into a standard LZ4 frame,
 *   2. decodes that whole frame with Lz4Decompressor::decompress(),
 *   3. extracts the raw block(s) from the frame and decodes each with
 *      Lz4Decompressor::decompressBlock() (the exact path the CQL v5 framing and
 *      the v3/v4 response reader use), and
 *   4. asserts both results are byte-for-byte identical to the input.
 *
 * If our decompressor can decode what the reference tool produced, it correctly
 * understands valid, standards-compliant LZ4 rather than merely round-tripping
 * our own compressor's output.
 *
 * Test inputs are a battery of generated edge cases plus any files or
 * directories passed as arguments — point it at a real corpus (Canterbury,
 * Silesia, enwik8, ...) for "proven good" real-world data:
 *
 *   php scripts/lz4/validate-decompressor.php [--max-bytes=N] [path ...]
 *
 * Exit codes: 0 = all passed, 1 = at least one mismatch, 2 = no external LZ4
 * tool found (cannot validate).
 */

use Cassandra\Compression\Lz4Decompressor;

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
 * Thin wrapper around whichever proven external LZ4 encoder is available. It
 * always produces a block-independent frame (magic 0x184D2204) so the frame's
 * blocks can also be decoded individually, and forces 64 KiB blocks so larger
 * inputs exercise the multi-block path.
 */
final class ExternalLz4Encoder {
    private function __construct(
        private string $name,
        /** @var list<string> $argvPrefix command that reads a raw file arg and writes an LZ4 frame to stdout */
        private array $argvPrefix,
    ) {
    }

    public static function detect(): ?self {
        // 1) Reference lz4 CLI (Yann Collet's implementation). Block-independence
        //    is the CLI default; -B4 caps blocks at 64 KiB; -9 for good matches.
        $version = @shell_exec('lz4 --version 2>/dev/null');
        if (is_string($version) && stripos($version, 'lz4') !== false) {
            return new self(
                name: 'lz4 CLI (' . trim($version) . ')',
                argvPrefix: ['lz4', '-9', '-B4', '-c'],
            );
        }

        // 2) python-lz4 (binds the reference C library).
        $ok = @shell_exec('python3 -c "import lz4.frame" 2>/dev/null; echo $?');
        if (is_string($ok) && trim($ok) === '0') {
            return new self(
                name: 'python-lz4 (lz4.frame)',
                argvPrefix: [
                    'python3', '-c',
                    'import sys,lz4.frame as f; sys.stdout.buffer.write('
                        . 'f.compress(open(sys.argv[1],"rb").read(),compression_level=9,'
                        . 'block_linked=False,block_size=f.BLOCKSIZE_MAX64KB))',
                ],
            );
        }

        return null;
    }

    /**
     * Compress raw bytes into an LZ4 frame with the external tool. Throws on any
     * tool error.
     *
     * @throws \RuntimeException
     */
    public function encode(string $data): string {
        $inputPath = tempnam(sys_get_temp_dir(), 'lz4raw_');
        if ($inputPath === false) {
            throw new RuntimeException('Unable to create temp file');
        }

        try {
            file_put_contents($inputPath, $data);

            $argv = $this->argvPrefix;
            $argv[] = $inputPath;

            $descriptors = [
                0 => ['file', '/dev/null', 'r'],
                1 => ['pipe', 'w'],
                2 => ['pipe', 'w'],
            ];

            $process = proc_open($argv, $descriptors, $pipes);
            if (!is_resource($process)) {
                throw new RuntimeException('Unable to launch external encoder');
            }

            $stdout = stream_get_contents($pipes[1]);
            $stderr = stream_get_contents($pipes[2]);
            fclose($pipes[1]);
            fclose($pipes[2]);
            $exitCode = proc_close($process);

            if ($exitCode !== 0) {
                throw new RuntimeException(sprintf(
                    'External encoder exited with code %d: %s',
                    $exitCode,
                    trim((string) $stderr)
                ));
            }

            return (string) $stdout;
        } finally {
            @unlink($inputPath);
        }
    }

    public function name(): string {
        return $this->name;
    }
}

/**
 * Minimal parser for a standard LZ4 frame (magic 0x184D2204) that returns the
 * raw blocks so they can be decoded individually. Only supports the subset the
 * encoders above emit; anything else throws.
 *
 * @return array{independent: bool, blocks: list<array{0: bool, 1: string}>}
 *   `blocks` is a list of [isCompressed, blockBytes].
 *
 * @throws \RuntimeException
 */
function extractFrameBlocks(string $frame): array {
    $length = strlen($frame);
    if ($length < 7) {
        throw new RuntimeException('frame too short');
    }

    /** @var array{1: int}|false $magic */
    $magic = unpack('V', $frame);
    if ($magic === false || $magic[1] !== 0x184D2204) {
        throw new RuntimeException(sprintf('unsupported frame magic 0x%08X', $magic === false ? 0 : $magic[1]));
    }

    $offset = 4;
    $flg = ord($frame[$offset]);
    $offset += 2; // FLG + BD

    $blockIndependent = (bool) ($flg & 0x20);
    $blockChecksum = ($flg & 0x10) ? 4 : 0;
    $contentSize = ($flg & 0x08) ? 8 : 0;
    $dictId = ($flg & 0x01) ? 4 : 0;

    $offset += $contentSize + $dictId + 1; // + 1-byte header checksum

    $blocks = [];
    while ($offset + 4 <= $length) {
        /** @var array{1: int}|false $sizeField */
        $sizeField = unpack('V', substr($frame, $offset, 4));
        if ($sizeField === false) {
            throw new RuntimeException('cannot read block size');
        }
        $offset += 4;

        $raw = $sizeField[1];
        if ($raw === 0) {
            break; // EndMark
        }

        $isCompressed = (($raw >> 31) & 0x1) === 0;
        $blockSize = $raw & 0x7FFFFFFF;

        if ($offset + $blockSize > $length) {
            throw new RuntimeException('block size exceeds frame');
        }

        $blocks[] = [$isCompressed, substr($frame, $offset, $blockSize)];
        $offset += $blockSize + $blockChecksum;
    }

    return ['independent' => $blockIndependent, 'blocks' => $blocks];
}

/**
 * Decode every block of an already-parsed frame with decompressBlock() and
 * concatenate the result — the way the driver decodes protocol payloads.
 *
 * @param list<array{0: bool, 1: string}> $blocks
 *
 * @throws \Cassandra\Exception\CompressionException
 */
function decodeBlocksIndividually(array $blocks): string {
    $out = '';
    foreach ($blocks as [$isCompressed, $blockBytes]) {
        if (!$isCompressed) {
            $out .= $blockBytes;

            continue;
        }

        $decompressor = new Lz4Decompressor();
        $decompressor->setInput($blockBytes);
        $out .= $decompressor->decompressBlock();
    }

    return $out;
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
        // Long overlapping runs of several periods, which stress the decoder's
        // overlapping-match copy (offset < match length).
        'rle run' => str_repeat('A', 200000),
        'period-3 run' => str_repeat('abc', 60000),
        'period-7 run' => str_repeat('abcdefg', 40000),
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

function trLeft(string $s, int $max = 40): string {
    return strlen($s) <= $max ? $s : '...' . substr($s, -($max - 3));
}

// --- main -------------------------------------------------------------------

$maxBytes = 16 * 1024 * 1024;
$paths = [];
foreach (array_slice($argv ?? [], 1) as $arg) {
    if (preg_match('/^--max-bytes=(\d+)$/', $arg, $m)) {
        $maxBytes = (int) $m[1];
    } elseif ($arg === '--help' || $arg === '-h') {
        fwrite(STDOUT, "Usage: php scripts/lz4/validate-decompressor.php [--max-bytes=N] [path ...]\n");
        exit(0);
    } else {
        $paths[] = $arg;
    }
}

$encoder = ExternalLz4Encoder::detect();
if ($encoder === null) {
    fwrite(STDERR, "No proven external LZ4 compressor found.\n");
    fwrite(STDERR, "Install one of:\n");
    fwrite(STDERR, "  - the reference lz4 CLI   (Debian/Ubuntu: apt-get install lz4, macOS: brew install lz4)\n");
    fwrite(STDERR, "  - python-lz4              (pip install lz4)\n");
    exit(2);
}

fwrite(STDOUT, "Validating Cassandra\\Compression\\Lz4Decompressor against: {$encoder->name()}\n\n");

$inputs = collectTestInputs($paths, $maxBytes);

$passed = 0;
$failed = 0;
$skipped = 0;

foreach ($inputs as $label => $data) {
    try {
        $frame = $encoder->encode($data);
    } catch (RuntimeException $e) {
        // The external tool failed to *produce* a frame, so there is nothing to
        // validate our decompressor against — this is not a decompressor bug.
        // (The reference lz4 CLI, for instance, rejects an incompressible input
        // sized exactly at its block boundary.) Skip rather than fail.
        $skipped++;
        $reason = trim(explode("\n", $e->getMessage())[0]);
        printf("  skip  %-40s encoder could not produce a frame: %s\n", trLeft($label), $reason);

        continue;
    }

    // 1) Whole-frame decode.
    try {
        $viaFrame = (new Lz4Decompressor($frame))->decompress();
    } catch (Throwable $e) {
        $failed++;
        printf("  FAIL  %-40s decompress() threw: %s\n", trLeft($label), $e->getMessage());

        continue;
    }

    if ($viaFrame !== $data) {
        $failed++;
        printf("  FAIL  %-40s decompress() gave %d B, expected %d B\n", trLeft($label), strlen($viaFrame), strlen($data));

        continue;
    }

    // 2) Per-block decode (the driver's path). Only meaningful when the frame's
    //    blocks are independent, which our encoder settings guarantee.
    $blockNote = '';

    try {
        $parsed = extractFrameBlocks($frame);
        if ($parsed['independent']) {
            $viaBlocks = decodeBlocksIndividually($parsed['blocks']);
            if ($viaBlocks !== $data) {
                $failed++;
                printf(
                    "  FAIL  %-40s decompressBlock() gave %d B, expected %d B\n",
                    trLeft($label),
                    strlen($viaBlocks),
                    strlen($data)
                );

                continue;
            }
            $blockNote = sprintf(' [%d block(s)]', count($parsed['blocks']));
        } else {
            $blockNote = ' [linked blocks: frame-only]';
        }
    } catch (Throwable $e) {
        $failed++;
        printf("  FAIL  %-40s block extraction/decode failed: %s\n", trLeft($label), $e->getMessage());

        continue;
    }

    $passed++;
    printf("  ok    %-40s %10d B -> %10d B%s\n", trLeft($label), strlen($frame), strlen($data), $blockNote);
}

printf("\n%d passed, %d failed, %d skipped, %d total\n", $passed, $failed, $skipped, $passed + $failed + $skipped);

exit($failed === 0 ? 0 : 1);
