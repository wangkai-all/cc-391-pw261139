"""接入器（ingestor）：把 stdin 上的 JSONL 窄表记录按可调批量落盘。

与生成器是两个独立进程，通过管道连接：
    python -m widetable.generator ... | python -m widetable.ingestor --root ...

批量阈值两条，满足其一即落一个窄表分段：
  --batch-rows     缓冲记录数上限（默认 50）
  --batch-seconds  首条记录入队后的最长等待（默认 1.0 秒，模拟持续输入）

分段文件 seg-NNNNNN.json 经临时文件 + rename 原子落盘；
seg id 在状态锁内单调分配，因此下游只按 id 排序即可保证处理顺序。
内存上限就是一个批量的大小，与历史总量无关。
"""
from __future__ import annotations

import argparse
import json
import os
import select
import sys
import time

from . import common


def _next_seg_id(root: str) -> int:
    """锁内分配下一个窄表分段 id。"""
    existing = [sid for sid, _ in common.list_segments(
        os.path.join(root, "narrow"))]
    return (max(existing) + 1) if existing else 0


def flush_segment(root: str, seg_id: int, rows: list[dict]) -> str:
    payload = {"seg": seg_id, "count": len(rows), "rows": rows}
    path = common.seg_path(os.path.join(root, "narrow"), seg_id)
    common.atomic_write_json(path, payload)
    return path


def ingest(root: str, stream, batch_rows: int, batch_seconds: float,
           max_batches: int | None = None) -> list[str]:
    """从文本流逐行读取并按批落盘。返回写出的分段路径列表。"""
    common.ensure_dirs(root)
    written: list[str] = []
    buf: list[dict] = []
    buf_first_ts: float | None = None
    counter = 0

    def flush() -> None:
        nonlocal buf, buf_first_ts
        if not buf:
            return
        with common.state_lock(root):
            seg_id = _next_seg_id(root)
            path = flush_segment(root, seg_id, buf)
        written.append(path)
        buf = []
        buf_first_ts = None

    for line in stream:
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        buf.append(rec)
        if buf_first_ts is None:
            buf_first_ts = time.monotonic()
        if len(buf) >= batch_rows:
            flush()
            counter += 1
            if max_batches is not None and counter >= max_batches:
                break
            continue
        # 时间阈值：<=0 表示禁用，仅按记录数成批；
        # 否则首条入队超过阈值即落盘，避免持续输入时迟迟不刷新
        if (batch_seconds > 0 and buf
                and (time.monotonic() - buf_first_ts) >= batch_seconds):
            flush()
            counter += 1
            if max_batches is not None and counter >= max_batches:
                break

    flush()
    return written


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="窄表接入器：JSONL stdin → 分段文件")
    ap.add_argument("--root", required=True)
    ap.add_argument("--batch-rows", type=int, default=50)
    ap.add_argument("--batch-seconds", type=float, default=1.0)
    args = ap.parse_args(argv)

    # stdin 在管道下按行迭代即可，select 仅用于防止终端模式下的阻塞语义歧义
    if not select.select([sys.stdin], [], [], 0)[0] and sys.stdin.isatty():
        ap.error("stdin 是终端，请通过管道提供 JSONL 数据")

    paths = ingest(args.root, sys.stdin, args.batch_rows, args.batch_seconds)
    for p in paths:
        print(f"ingested {p}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
