#!/usr/bin/env bash
# 一键演示：在独立数据目录里拉起完整四进程 + Web 服务。
# 用法：scripts/run_demo.sh [数据根目录，默认 ./demo-data]
# 停止：Ctrl-C（脚本会回收全部子进程）
set -euo pipefail

ROOT="${1:-$(pwd)/demo-data}"
mkdir -p "$ROOT"
export PYTHONPATH="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"

cleanup() {
  echo ""
  echo "[demo] 停止全部进程 ..."
  kill 0 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[demo] 数据根目录: $ROOT"
echo "[demo] 1/4 启动转置进程（稀疏分段，0.3s 轮询）"
$PY -m widetable.transposer --root "$ROOT" --poll 0.3 &

echo "[demo] 2/4 启动回填进程（非压缩密架，0.5s 轮询）"
$PY -m widetable.backfill --root "$ROOT" --poll 0.5 &

echo "[demo] 3/4 启动 Web 服务 http://127.0.0.1:8000"
$PY -m widetable.server --root "$ROOT" --port 8000 &

sleep 0.8
echo "[demo] 4/4 生成器 → 接入器管道（8 批，每批 12 条落一段，流式延迟 0.4s）"
$PY -m widetable.generator --batches 8 --batch-size 3 --entities 3 \
    --seed 42 --stream-delay 0.4 \
  | $PY -m widetable.ingestor --root "$ROOT" --batch-rows 12 --batch-seconds 0

echo "[demo] 数据已全部投递，进程保持运行；浏览器打开 http://127.0.0.1:8000"
echo "[demo] 目录结构："
find "$ROOT" -maxdepth 3 -type f | grep -v lock | sort | head -20
wait
