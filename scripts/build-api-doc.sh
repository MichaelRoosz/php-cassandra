#!/usr/bin/env bash
#
# Generates the API reference in doc/api with phpDocumentor.
#
# phpDocumentor lives in tools/doc rather than in the root composer.json: it pins
# symfony ^6, which cannot be installed next to the symfony ^7 packages this project
# already depends on.
#
# The markdown output uses saggre/phpdocumentor-markdown with a small overlay from
# tools/doc/theme (upstream ships no enum template). Both are combined into
# tools/doc/build/themes/markdown, which is a build artifact and not checked in.

set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tool_dir="$root_dir/tools/doc"
theme_src="$tool_dir/vendor/saggre/phpdocumentor-markdown/themes/markdown"
theme_build="$tool_dir/build/themes/markdown"

if [ ! -f "$tool_dir/vendor/bin/phpdoc" ]; then
    echo "Installing documentation toolchain in tools/doc ..."
    # phpDocumentor caps its supported PHP versions; the tool itself runs fine on newer ones.
    composer install --working-dir="$tool_dir" --no-interaction --ignore-platform-req=php+
fi

echo "Assembling markdown theme ..."
rm -rf "$theme_build"
mkdir -p "$theme_build"
cp -r "$theme_src/." "$theme_build/"
cp "$tool_dir/theme/." "$theme_build/" -r

echo "Generating API reference in doc/api ..."
rm -rf "$root_dir/doc/api"
"$tool_dir/vendor/bin/phpdoc" --config "$root_dir/phpdoc.dist.xml" --no-interaction "$@"

echo "Done: doc/api/README.md"
