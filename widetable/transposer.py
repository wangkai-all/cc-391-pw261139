"""转置进程：窄表分段 → 稀疏宽表分段（流式，不回看旧文件）。

关键不变量：
  1. 每个窄分段只读一次，处理完立即释放；内存只与「单个分段 + 其出现的
     行/列数」有关，与历史总量无关，满足不把整批数据装内存的要求。
  2. 新测量项在首次出现的那个分段里就地注册（推进模式号 gen），旧分段
     保持原样、永不重写 —— 稀疏方案下旧文件里没有的列读取端按 null 处理，
     绝不补零。
  3. 同分段内同键重复/晚到记录按版本号决胜，被拒事件写冲突日志。
  4. 游标记录最后处理完的窄分段 id，重启后断点续跑。

稀疏宽分段格式（seg-NNNNNN.json）：
  {"seg": id, "gen": 写出时模式号,
   "columns": [本分段实际出现过的测量项, 按注册顺序],
   "rows": [{"ts", "entity", "cells": {metric: cell}}, ...按 (ts,entity) 升序]}
"""
from __future__ import annotations

import argparse
import os
import time

from . import common, state


def transpose_segment(root: str, seg_id: int, narrow_path: str) -> dict:
    """转置单个窄分段，返回统计信息。调用方负责加锁。"""
    payload = common.load_json(narrow_path, {"rows": []})
    rows_in = payload.get("rows", [])

    # 注册顺序 = 测量项在数据流中的首次出现顺序（不用字母序），
    # 这样列顺序可与输入分代计划逐一对账
    seen_metrics = list(dict.fromkeys(r["metric"] for r in rows_in))
    registry, added = state.register_metrics(root, seen_metrics)
    gen_now = registry["gen"]
    ordered = registry["metrics"]

    # (ts, entity) -> metric -> cell，单分段内先做一次合并；
    # 跨分段的同键决胜发生在读取/回填期（稀疏文件不可变，不回看旧段）
    table: dict[tuple[int, str], dict[str, dict]] = {}
    stats = {"in": len(rows_in), "new": 0, "updated": 0,
             "duplicate": 0, "rejected_late": 0}

    for rec in rows_in:
        key = (rec["ts"], rec["entity"])
        row = table.setdefault(key, {})
        cell = {"v": rec["v"], "ver": rec["ver"], "source": rec.get("source", "")}
        merged, status = common.merge_cell(row.get(rec["metric"]), cell)
        row[rec["metric"]] = merged
        stats[status] += 1
        if status in ("rejected_late", "duplicate"):
            state.log_conflict(root, {
                "stage": "transpose", "seg": seg_id,
                "ts": rec["ts"], "entity": rec["entity"],
                "metric": rec["metric"],
                "incoming_ver": rec["ver"],
                "kept_ver": row[rec["metric"]]["ver"],
                "incoming_v": rec["v"],
                "event": ("late_record_rejected" if status == "rejected_late"
                          else "duplicate_record_ignored"),
            })

    # 按 (ts, entity) 排序；cells 内列顺序对齐全局注册顺序（可核对）
    out_rows = []
    for (ts, entity) in sorted(table):
        cells = table[(ts, entity)]
        ordered_cells = {m: cells[m] for m in ordered if m in cells}
        out_rows.append({"ts": ts, "entity": entity, "cells": ordered_cells})

    seg_doc = {
        "seg": seg_id,
        "gen": gen_now,
        "columns": [m for m in ordered if any(m in table[k] for k in table)],
        "rows": out_rows,
    }
    path = common.seg_path(os.path.join(root, "wide", "sparse"), seg_id)
    size = common.atomic_write_json(path, seg_doc)
    stats["seg"] = seg_id
    stats["bytes"] = size
    stats["new_metrics"] = added
    return stats


def run_once(root: str) -> list[dict]:
    """处理游标之后的全部窄分段；锁内串行，避免与回填/注册相互穿插。"""
    common.ensure_dirs(root)
    results: list[dict] = []
    with common.state_lock(root):
        cursor = state.load_cursor(root)
        last = cursor["last_seg"]
        for seg_id, path in common.list_segments(os.path.join(root, "narrow")):
            if seg_id <= last:
                continue
            stats = transpose_segment(root, seg_id, path)
            last = seg_id
            state.save_cursor(root, {"last_seg": last})
            results.append(stats)
    return results


def run_loop(root: str, poll_seconds: float) -> None:
    while True:
        run_once(root)
        time.sleep(poll_seconds)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="窄→宽转置进程（稀疏分段输出）")
    ap.add_argument("--root", required=True)
    ap.add_argument("--poll", type=float, default=0.5,
                    help="无新分段时的轮询间隔秒数")
    ap.add_argument("--once", action="store_true", help="处理完现有分段即退出")
    args = ap.parse_args(argv)
    if args.once:
        for s in run_once(args.root):
            print(f"transpose seg={s['seg']} in={s['in']} "
                  f"rejected_late={s['rejected_late']} "
                  f"new_metrics={s['new_metrics']}", flush=True)
    else:
        run_loop(args.root, args.poll)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
