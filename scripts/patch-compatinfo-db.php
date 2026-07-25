<?php

declare(strict_types=1);

/**
 * Corrects a known data issue in the phpcompatinfo reference database.
 *
 * PHP 8.2 moved a handful of long-standing functions out of ext-standard into the
 * new (always bundled) ext-random. compatinfo-db records both homes, so those
 * functions appear twice: once under "standard" with their real minimum PHP version,
 * and once under "random" with 8.2. Its resolver (FunctionRepository::getFunctionByName())
 * orders duplicates by php_min descending and keeps the first, so it always reports
 * the 8.2 row — claiming, for example, that a call to random_bytes() requires PHP 8.2
 * even though the function has existed since PHP 7.0 and is available in every build.
 *
 * That single wrong row is enough to push the analysed minimum for this library from
 * 8.1 to 8.2 and fail scripts/check-php-version.sh.
 *
 * The fix is narrow and mechanical: for every function present in *both* ext-random and
 * ext-standard, copy the (lower) php_min from the standard row onto the random row. The
 * extension attribution is left untouched — the report keeps showing the function as
 * belonging to ext-random, which is correct, and only the PHP version requirement is
 * corrected. Functions that genuinely arrived with ext-random in 8.2 have no standard
 * counterpart and are therefore never touched.
 *
 * db:init reloads the reference data from JSON and undoes this, so the patch runs as
 * part of scripts/check-php-version.sh rather than once at setup time. It is idempotent.
 */

$cacheDir = $_SERVER['APP_CACHE_DIR'] ?? $_ENV['APP_CACHE_DIR'] ?? null;

if (!is_string($cacheDir) || $cacheDir === '') {
    $homeDir = $_SERVER['HOME'] ?? $_ENV['HOME'] ?? null;

    if (!is_string($homeDir) || $homeDir === '') {
        fwrite(STDERR, "Cannot locate the compatinfo database: neither APP_CACHE_DIR nor HOME is set.\n");

        exit(1);
    }

    $cacheDir = $homeDir . '/.cache/bartlett';
}

$databaseFile = $cacheDir . '/prod/compatinfo-db.sqlite';

if (!is_file($databaseFile)) {
    fwrite(STDERR, "Compatinfo database not found at {$databaseFile}; run 'vendor/bin/phpcompatinfo db:create' and 'db:init' first.\n");

    exit(1);
}

$database = new SQLite3($databaseFile, SQLITE3_OPEN_READWRITE);
$database->enableExceptions(true);

$database->exec(<<<'SQL'
    UPDATE functions
    SET php_min = (
        SELECT standardFunction.php_min
        FROM functions AS standardFunction
        JOIN extensions AS standardExtension ON standardExtension.id = standardFunction.extension_id
        WHERE standardExtension.name = 'standard'
          AND standardFunction.name = functions.name
          AND COALESCE(standardFunction.declaring_class, '') = ''
    )
    WHERE COALESCE(declaring_class, '') = ''
      AND extension_id = (SELECT id FROM extensions WHERE name = 'random')
      AND EXISTS (
        SELECT 1
        FROM functions AS standardFunction
        JOIN extensions AS standardExtension ON standardExtension.id = standardFunction.extension_id
        WHERE standardExtension.name = 'standard'
          AND standardFunction.name = functions.name
          AND COALESCE(standardFunction.declaring_class, '') = ''
          AND standardFunction.php_min < functions.php_min
      )
    SQL);

$patchedCount = $database->changes();

$database->close();

echo "Patched {$patchedCount} ext-random function(s) with their original ext-standard php_min.\n";
