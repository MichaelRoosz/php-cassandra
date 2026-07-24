#!/usr/bin/env bash
#
# Validate the pure-PHP Lz4Compressor against a proven external LZ4 tool.
#
# This is a thin convenience wrapper around validate-compressor.php. Its only
# extra feature is `--download-corpus`, which fetches well-known "proven good"
# compression test corpora (the Canterbury corpus) and feeds them to the
# validator as real-world test data.
#
# Usage:
#   scripts/lz4/validate-compressor.sh [--download-corpus] [extra path ...]
#
# Any non-flag arguments are passed straight through to the PHP validator, so
# you can point it at your own files:
#   scripts/lz4/validate-compressor.sh /path/to/silesia
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PHP_BIN="${PHP_BIN:-php}"

download_corpus=0
extra_args=()
for arg in "$@"; do
    case "$arg" in
        --download-corpus) download_corpus=1 ;;
        -h|--help)
            sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) extra_args+=("$arg") ;;
    esac
done

# Warn early (the PHP script also checks and exits 2 if nothing is available).
if ! command -v lz4 >/dev/null 2>&1 \
    && ! python3 -c 'import lz4.frame' >/dev/null 2>&1; then
    echo "note: no 'lz4' CLI or python-lz4 found yet; the validator will explain how to install one." >&2
fi

if [[ "$download_corpus" -eq 1 ]]; then
    corpus_dir="$(mktemp -d "${TMPDIR:-/tmp}/lz4-corpus.XXXXXX")"
    trap 'rm -rf "$corpus_dir"' EXIT

    fetch() {
        local url="$1" out="$2"
        if command -v curl >/dev/null 2>&1; then
            curl -fsSL --connect-timeout 10 --max-time 120 "$url" -o "$out"
        elif command -v wget >/dev/null 2>&1; then
            wget -q --timeout=120 "$url" -O "$out"
        else
            echo "error: need curl or wget to download the corpus." >&2
            exit 1
        fi
    }

    echo "Downloading proven test corpora (Canterbury) ..." >&2
    for name in cantrbry large; do
        url="https://corpus.canterbury.ac.nz/resources/${name}.tar.gz"
        if fetch "$url" "$corpus_dir/${name}.tar.gz"; then
            mkdir -p "$corpus_dir/$name"
            tar -xzf "$corpus_dir/${name}.tar.gz" -C "$corpus_dir/$name"
            echo "  extracted ${name} corpus" >&2
        else
            echo "  warning: could not download ${name} corpus, skipping" >&2
        fi
    done

    extra_args+=("$corpus_dir")
fi

exec "${PHP_BIN}" "${SCRIPT_DIR}/validate-compressor.php" "${extra_args[@]}"
