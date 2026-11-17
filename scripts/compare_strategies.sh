#!/usr/bin/env bash
# 压缩策略对照演示：同一份数据分别跑「非压缩回填」与「全量压缩回填」，
# 用 reader.compare_paths 量化读放大与重写成本差异。
set -euo pipefail
export PYTHONPATH="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
BASE="$(pwd)/demo-compare"
rm -rf "$BASE"

run_mode() {
  local name="$1"; shift
  local root="$BASE/$name"
  mkdir -p "$root"
  $PY -m widetable.generator --batches 6 --batch-size 3 --entities 2 --seed 7 \
    | $PY -m widetable.ingestor --root "$root" --batch-rows 12 --batch-seconds 0
  $PY -m widetable.transposer --root "$root" --once
  local n
  n=$(ls "$root/wide/sparse" | wc -l)
  for ((i = 0; i < n; i++)); do
    $PY -m widetable.backfill --root "$root" --once "$@" >/dev/null
  done
}

echo "== 非压缩（逐段密架） =="
run_mode plain
echo "== 全量压缩（单文件重写） =="
run_mode compact --compact

$PY - "$BASE" <<'EOF'
import os, sys
sys.path.insert(0, os.getcwd())
from widetable import reader
for name in ("plain", "compact"):
    r = reader.compare_paths(os.path.join(sys.argv[1], name))
    s, b = r["sparse_stats"], r["backfill_stats"]
    print(f"[{name}] 一致={r['equal']} 行数={r['row_count']}")
    print(f"  稀疏路径: 打开 {s['files_opened']} 文件 / 读 {s['bytes_read']} B")
    print(f"  回填路径: 打开 {b['files_opened']} 文件 / 读 {b['bytes_read']} B"
          f" / 累计重写 {b['rewrites']} 次 {b['rewrite_bytes']} B")
EOF
