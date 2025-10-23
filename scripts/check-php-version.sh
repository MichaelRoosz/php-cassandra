#!/bin/bash

# This script verifies that phpcompatinfo correctly identifies PHP 8.1 as the minimum requirement

set -e

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

