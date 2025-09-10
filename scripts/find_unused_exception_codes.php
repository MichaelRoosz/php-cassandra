<?php

declare(strict_types=1);

// Usage: php scripts/find_unused_exception_codes.php

/**
 * This script scans src/ExceptionCode.php for enum case names and then searches the
 * repository for references to those cases. Any enum case that is never referenced
 * (outside its definition file) is reported as unused.
 */

$root = dirname(__DIR__);
$srcDir = $root . '/src';
$enumFile = $srcDir . '/Exception/ExceptionCode.php';

if (!is_file($enumFile)) {
    fwrite(STDERR, "ExceptionCode.php not found at: {$enumFile}\n");
    exit(1);
}

$enumContents = file_get_contents($enumFile);
if ($enumContents === false) {
    fwrite(STDERR, "Failed to read: {$enumFile}\n");
    exit(1);
}

// Extract enum case names and their numeric values
// Matches lines like: case FOO_BAR = 1234;
preg_match_all('/\bcase\s+([A-Z0-9_]+)\s*=\s*([0-9]+)\s*;/', $enumContents, $matches, PREG_SET_ORDER);

$cases = [];
foreach ($matches as $m) {
    $cases[$m[1]] = (int) $m[2];
}

if (!$cases) {
    fwrite(STDERR, "No enum cases found in ExceptionCode.php\n");
    exit(1);
}

// Recursively collect PHP files to scan (exclude vendor and the enum file itself)
$rii = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS));

$phpFiles = [];
foreach ($rii as $file) {

    if (!($file instanceof SplFileInfo)) {
        continue;
    }

    if (!$file->isFile()) {
        continue;
    }
    $path = $file->getPathname();
    // Exclude vendor
    if (str_contains($path, DIRECTORY_SEPARATOR . 'vendor' . DIRECTORY_SEPARATOR)) {
        continue;
    }
    // Only scan PHP files
    if (strtolower(pathinfo($path, PATHINFO_EXTENSION)) !== 'php') {
        continue;
    }
    // Exclude the enum definition file itself
    if ($path === $enumFile) {
        continue;
    }
    $phpFiles[] = $path;
}

// Prepare search patterns for each case: look for "ExceptionCode::CASE_NAME"
$caseNameToUsed = array_fill_keys(array_keys($cases), false);

foreach ($phpFiles as $path) {
    $contents = file_get_contents($path);
    if ($contents === false) {
        continue;
    }
    foreach ($caseNameToUsed as $caseName => $used) {
        if ($used) {
            continue; // already found
        }
        if (mb_strpos($contents, 'ExceptionCode::' . $caseName) !== false) {
            $caseNameToUsed[$caseName] = true;
        }
    }
}

$unused = [];
foreach ($caseNameToUsed as $caseName => $used) {
    if (!$used) {
        $unused[$caseName] = $cases[$caseName];
    }
}

ksort($unused);

if (!$unused) {
    echo "All ExceptionCode cases are referenced.\n";
    exit(0);
}

echo "Unused ExceptionCode cases (name = value):\n";
foreach ($unused as $name => $value) {
    echo $name . ' = ' . $value . "\n";
}

exit(0);
