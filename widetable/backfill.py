"""后台回填进程：稀疏宽分段 → 列结构一致的密架分段。

两种策略，用 --compact 切换：

  非压缩（默认，稀疏分段的密架化）：
    每个稀疏分段对应一个 backfill/seg-NNNNNN.json，行内显式写出全部已注册
    测量项，缺失位置为 null。读取端顺序流式合并 N 个文件即可，写放大低
    （每段只写一次），读放大 = 全部分段都要打开。

  压缩（--compact）：
    维护单一 backfill/compacted.json，每来一个新分段就把旧压缩件与新分段
    全量读出、合并、整文件重写。读放大降到 1 个文件 + 未回填尾部，
    但重写成本随历史增长线性叠加（第 k 段写入时重写前 k-1 段的数据量），
    bytes_written/rewrites 两个计数器把这笔账直接量出来。

模式号 gen 变化（网页新增测量项）时旧密架结构立刻过时：
  本进程在锁内删除全部回填文件、清空 done_segs、对齐新 gen 后从头回填。
暂停（paused）时不做任何重写，已写文件保留，读取端自动回退到稀疏路径。

修正覆盖层不物化进文件：人工修正保存在 state/corrections.json，
由读取端在两条路径上统一叠加，避免每次修正都触发全量重写。
"""
from __future__ import annotations

import argparse
import os
import time

from . import common, reader, state

COMPACT_NAME = "compacted.json"


def _backfill_dir(root: str) -> str:
    return os.path.join(root, "wide", "backfill")


def _dense_row(row: dict, columns: list[str]) -> dict:
    """单条稀疏行展开为全列密架行，缺失位置 null。"""
    return {"ts": row["ts"], "entity": row["entity"],
            "cells": {m: row["cells"].get(m) for m in columns}}


def _dense_doc(seg_id: int, gen: int, columns: list[str],
               sparse_doc: dict) -> dict:
    """把一个稀疏分段展开为「全列 + null 占位」的密架分段。"""
    return {"seg": seg_id, "gen": gen, "columns": list(columns),
            "rows": [_dense_row(r, columns) for r in sparse_doc["rows"]]}


def _load_compacted(root: str) -> dict | None:
    path = os.path.join(_backfill_dir(root), COMPACT_NAME)
    return common.load_json(path, None)


def _reset_backfill(root: str, bs: dict, new_gen: int) -> None:
    d = _backfill_dir(root)
    if os.path.isdir(d):
        for name in os.listdir(d):
            os.remove(os.path.join(d, name))
    os.makedirs(d, exist_ok=True)
    bs["gen"] = new_gen
    bs["done_segs"] = []


def tick(root: str, compact: bool) -> dict:
    """执行一轮回填，返回本轮统计。全程持状态锁，保证与网页写操作串行。"""
    common.ensure_dirs(root)
    with common.state_lock(root):
        bs = state.load_backfill(root)
        registry = state.load_metrics(root)
        columns = registry["metrics"]
        report = {"paused": bs["paused"], "rewritten": 0,
                  "rejected_late": 0, "reset": False, "bytes": 0}

        if bs["paused"]:
            return report

        # 模式号推进：旧密架结构整体作废，从头回填
        if bs["gen"] != registry["gen"]:
            _reset_backfill(root, bs, registry["gen"])
            report["reset"] = True

        done = set(bs["done_segs"])
        compacted = _load_compacted(root) if compact else None
        if compact and compacted is not None and compacted["gen"] != bs["gen"]:
            compacted = None  # 陈旧压缩件，忽略并重建

        for seg_id, path in common.list_segments(
                os.path.join(root, "wide", "sparse")):
            if seg_id in done:
                continue
            if state.load_backfill(root)["paused"]:
                break  # 运行中被网页暂停：写完当前文件即停

            sparse_doc = common.load_json(path, {"rows": []})
            dense = _dense_doc(seg_id, bs["gen"], columns, sparse_doc)
            d = _backfill_dir(root)

            if compact:
                base = compacted["rows"] if compacted else []
                merge_stats = {"rejected_late": 0}
                merged_rows = reader.merge_dense_rows(
                    root,
                    [(-1, base), (seg_id, dense["rows"])],
                    columns, "backfill-compact", merge_stats)
                compacted = {
                    "gen": bs["gen"], "columns": list(columns),
                    "segs": (compacted["segs"] if compacted else []) + [seg_id],
                    "rows": merged_rows,
                }
                size = common.atomic_write_json(
                    os.path.join(d, COMPACT_NAME), compacted)
                report["rejected_late"] += merge_stats["rejected_late"]
            else:
                size = common.atomic_write_json(
                    os.path.join(d, f"seg-{seg_id:06d}.json"), dense)

            done.add(seg_id)
            bs["done_segs"] = sorted(done)
            bs["bytes_written"] += size
            bs["rewrites"] += 1
            report["rewritten"] += 1
            report["bytes"] += size
            state.save_backfill(root, bs)

        # 非压缩模式下也登记一次状态（含 reset 场景）
        if report["reset"] or report["rewritten"] == 0:
            state.save_backfill(root, bs)
        return report


def run_loop(root: str, poll_seconds: float, compact: bool) -> None:
    while True:
        tick(root, compact)
        time.sleep(poll_seconds)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="后台回填进程（可暂停/可压缩）")
    ap.add_argument("--root", required=True)
    ap.add_argument("--poll", type=float, default=0.5)
    ap.add_argument("--compact", action="store_true",
                    help="维护单一全量压缩文件（高重写成本、低读放大）")
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args(argv)
    if args.once:
        r = tick(args.root, args.compact)
        print(f"backfill {r}", flush=True)
    else:
        run_loop(args.root, args.poll, args.compact)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
