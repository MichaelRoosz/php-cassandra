# LZ4 validation

These scripts validate the pure-PHP `Cassandra\Compression\Lz4Compressor` and
`Cassandra\Compression\Lz4Decompressor` against a **proven, external** LZ4
implementation — the reference [`lz4` CLI](https://github.com/lz4/lz4) or the
[python-lz4](https://pypi.org/project/lz4/) library (both bind Yann Collet's
reference C implementation). Because each validator crosses the external tool
rather than pairing our compressor with our decompressor, a passing run proves
our code interoperates with standards-compliant LZ4 rather than merely agreeing
with itself.

## Compressor validation

`validate-compressor.php` proves our **compressor** emits valid LZ4. For every
test input it:

1. compresses it with `Lz4Compressor` (producing a raw LZ4 block),
2. wraps the block(s) in a standard LZ4 frame (magic `0x184D2204`),
3. decompresses that frame with the external tool, and
4. asserts the result is byte-for-byte identical to the input.

## Decompressor validation

`validate-decompressor.php` proves our **decompressor** reads valid LZ4. For
every test input it:

1. compresses it with the external tool into a standard LZ4 frame,
2. decodes the whole frame with `Lz4Decompressor::decompress()`,
3. extracts the raw block(s) from the frame and decodes each with
   `Lz4Decompressor::decompressBlock()` — the exact path the CQL v5 framing and
   the v3/v4 response reader use, and
4. asserts both results are byte-for-byte identical to the input.

Inputs the external tool refuses to compress (e.g. the reference `lz4` CLI
rejects an incompressible input sized exactly at its block boundary) are
reported as `skip`, not `FAIL` — there is simply no reference frame to decode.

## Requirements

One of:

- the `lz4` CLI — Debian/Ubuntu `apt-get install lz4`, macOS `brew install lz4`
- python-lz4 — `pip install lz4`

If neither is found the validators print instructions and exit with code `2`.

## Usage

```bash
# Generated edge cases + real files from this repo:
php scripts/lz4/validate-compressor.php
php scripts/lz4/validate-decompressor.php

# Also validate against your own files/directories (recursed):
php scripts/lz4/validate-decompressor.php /path/to/enwik8 /path/to/silesia

# Convenience wrappers that can also fetch proven test corpora (Canterbury):
scripts/lz4/validate-compressor.sh --download-corpus
scripts/lz4/validate-decompressor.sh --download-corpus
```

Exit codes: `0` all passed, `1` at least one mismatch, `2` no external LZ4 tool
available.

## Test data

Both validators always run a battery of generated edge cases (empty input, sizes
around the 64 KiB match window, the 128 KiB Cassandra frame payload and the
4 MiB LZ4 block-max boundary, highly compressible, incompressible, all-zeros,
and long overlapping runs), plus real files from this repository (source, JSON,
the PHP binary).

For "proven good" real-world data, pass a real corpus. `--download-corpus`
fetches the classic [Canterbury corpus](https://corpus.canterbury.ac.nz/)
(including large files such as the E. coli genome and the King James Bible), but
any files you point it at work too (e.g. the
[Silesia corpus](https://sun.aei.polsl.pl//~sdeor/index.php?page=silesia) or
`enwik8`).
