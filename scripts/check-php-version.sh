#!/bin/bash

# This script verifies that phpcompatinfo correctly identifies PHP 8.1 as the minimum requirement

set -e

# compatinfo-db reports the functions that PHP 8.2 moved from ext-standard to ext-random
# (random_bytes(), mt_rand(), …) as requiring 8.2, although they are available in every
# PHP 7/8 build. See scripts/patch-compatinfo-db.php for details. The correction has to be
# reapplied after every "phpcompatinfo db:init", so it runs here; it is idempotent.
php "$(dirname "$0")/patch-compatinfo-db.php"

echo "Running PHP compatibility analysis..."
OUTPUT=$(vendor/bin/phpcompatinfo analyser:run --no-interaction src)

echo "$OUTPUT"
echo ""

if echo "$OUTPUT" | grep -q "Requires PHP 8.1"; then
    echo "✓ SUCCESS: PHP 8.1 requirement correctly detected"
    exit 0
else
    echo "✗ FAILURE: Expected 'Requires PHP 8.1' in output but it was not found"
    exit 1
fi

