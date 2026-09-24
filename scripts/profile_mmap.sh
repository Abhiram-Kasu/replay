#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE_TARGET_DIR="$(mktemp -d /tmp/replay-mmap-profile-target.XXXXXX)"
TRACE_PATH="${1:-/tmp/replay-mmap-$(date +%Y%m%d-%H%M%S).trace}"

if [[ -e "$TRACE_PATH" ]]; then
    echo "Refusing to overwrite existing trace: $TRACE_PATH" >&2
    exit 1
fi

cd "$REPO_DIR"
echo "Building mmap benchmark with debug symbols..."
CARGO_TARGET_DIR="$PROFILE_TARGET_DIR" RUSTFLAGS="-C debuginfo=2" \
    cargo bench --bench observers --no-run

BENCH_BIN="$(find "$PROFILE_TARGET_DIR/release/deps" -maxdepth 1 -type f -name 'observers-*' -perm -111 -print -quit)"
if [[ -z "$BENCH_BIN" ]]; then
    echo "Could not locate the observers benchmark executable." >&2
    exit 1
fi

echo "Recording Time Profiler trace to: $TRACE_PATH"
xcrun xctrace record \
    --template "Time Profiler" \
    --output "$TRACE_PATH" \
    --launch -- "$BENCH_BIN" --bench memory_mapped_file

echo "Opening trace: $TRACE_PATH"
open "$TRACE_PATH"
