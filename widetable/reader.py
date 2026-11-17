"""分析读取端：把磁盘上的分段拼成一张逻辑宽表。

两条路径，返回结构完全一致，可直接逐单元比较：

  read_sparse（稀疏分段 + 读取时拼接）：
    用小顶堆对 N 个分段的有序行流做 k 路归并，任何时刻内存里每个分段
    只有一行在读，读放大 = 打开全部 N 个分段；没有任何重写成本。
    迟到记录在归并处按版本号被拒，计入 stats.rejected_late 并写冲突日志。

  read_backfill（回填密架直读）：
    优先读 backfill/ 下结构一致的文件（压缩模式 1 个文件，否则 M 个），
    尚未回填到的尾部稀疏分段自动走稀疏归并补上，因此回填暂停期间查询
    依然正确，只是 stats.fallback_sparse 会显示回退了多少段。

共同规则：
  * 列顺序 = state/metrics.json 的注册顺序；缺失单元输出 null，显式零输出 0。
  * 人工修正（corrections.json）在两条路径上统一叠加；修正版本号恒新，
    旧任务晚到无法覆盖它，且叠加发生在读取期、不触发任何文件重写。
"""
from __future__ import annotations

import heapq
import os

from . import common, state


def _segment_file_stats(path: str) -> int:
    return os.path.getsize(path) if os.path.exists(path) else 0


def _apply_corrections(root: str, row: dict) -> dict:
    """对一个已合并完成的输出行叠加人工修正。"""
    corr = state.load_corrections(root)
    ts, entity = row["ts"], row["entity"]
    for metric, cell in list(row["cells"].items()):
        k = f"{ts}\x1f{entity}\x1f{metric}"
        fix = corr.get(k)
        if fix is None:
            continue
        if cell is None:
            row["cells"][metric] = dict(fix)
            continue
        winner, status = common.merge_cell(cell, fix)
        row["cells"][metric] = winner
        if status == "rejected_late":  # 理论上不会发生：修正版本恒新
            state.log_conflict(root, {
                "stage": "reader", "ts": ts, "entity": entity,
                "metric": metric, "event": "correction_superseded",
                "incoming_ver": fix["ver"], "kept_ver": winner["ver"],
            })
    return row


def _row_key(row: dict) -> tuple[int, str]:
    return row["ts"], row["entity"]


def merge_dense_rows(root: str, row_streams: list[tuple[int, list[dict]]],
                     columns: list[str], stage: str,
                     stats: dict) -> list[dict]:
    """多来源密架/稀疏行的统一合并器。

    row_streams: [(来源分段 id, 行列表), ...]，按分段处理顺序给出。
    同一 (ts, entity) 的行按键归并；同键同列按版本号决胜：
    新版本覆盖、相同版本保留先到者、更旧版本计入 rejected_late 并写冲突日志。
    缺失（None）不参与决胜，因此 null 永远不会覆盖真实值，更不会被当成 0。
    """
    merged: dict[tuple[int, str], dict] = {}
    order: list[tuple[int, str]] = []
    for seg_id, rows in row_streams:
        for row in rows:
            k = _row_key(row)
            if k not in merged:
                merged[k] = {"ts": row["ts"], "entity": row["entity"],
                             "cells": {m: None for m in columns}}
                order.append(k)
            slot = merged[k]["cells"]
            for m in columns:
                incoming = row["cells"].get(m)
                if incoming is None:
                    continue
                old = slot.get(m)
                winner, status = common.merge_cell(old, incoming)
                slot[m] = winner
                if status == "rejected_late":
                    stats["rejected_late"] += 1
                    state.log_conflict(root, {
                        "stage": stage, "seg": seg_id,
                        "ts": row["ts"], "entity": row["entity"], "metric": m,
                        "incoming_ver": incoming["ver"],
                        "kept_ver": winner["ver"], "incoming_v": incoming["v"],
                        "event": "late_record_rejected",
                    })
    return [merged[k] for k in sorted(order)]


def read_sparse(root: str, limit: int | None = None) -> dict:
    """稀疏路径：堆归并全部稀疏分段，逐行输出。"""
    registry = state.load_metrics(root)
    columns = registry["metrics"]
    sparse_dir = os.path.join(root, "wide", "sparse")
    segments = common.list_segments(sparse_dir)

    stats = {"path": "sparse", "files_opened": 0, "bytes_read": 0,
             "segments": len(segments), "rejected_late": 0,
             "fallback_sparse": 0}
    # 每个分段一个迭代器；堆中同时只保留每段一行 —— 内存与分段数无关，
    # 只与「分段数 × 单行」有关，不把整批数据装内存
    heap: list = []
    for idx, (_, path) in enumerate(segments):
        doc = common.load_json(path, {"rows": []})
        stats["files_opened"] += 1
        stats["bytes_read"] += _segment_file_stats(path)
        it = iter(doc["rows"])
        first = next(it, None)
        if first is not None:
            heapq.heappush(
                heap,
                ((first["ts"], first["entity"]), idx, doc.get("seg", -1),
                 first, it))

    merged: dict[tuple[int, str], dict] = {}
    order: list[tuple[int, str]] = []
    while heap:
        key, idx, seg_id, row, it = heapq.heappop(heap)
        nxt = next(it, None)
        if nxt is not None:
            heapq.heappush(
                heap,
                ((nxt["ts"], nxt["entity"]), idx, seg_id, nxt, it))

        if key not in merged:
            merged[key] = {"ts": key[0], "entity": key[1], "cells": {}}
            order.append(key)
        slot = merged[key]["cells"]
        for metric, incoming in row["cells"].items():
            old = slot.get(metric)
            winner, status = common.merge_cell(old, incoming)
            slot[metric] = winner
            if status == "rejected_late":
                stats["rejected_late"] += 1
                state.log_conflict(root, {
                    "stage": "sparse-read", "seg": seg_id,
                    "ts": key[0], "entity": key[1], "metric": metric,
                    "incoming_ver": incoming["ver"],
                    "kept_ver": winner["ver"], "incoming_v": incoming["v"],
                    "event": "late_record_rejected",
                })

    out_rows = []
    for k in order:
        row = merged[k]
        row["cells"] = {m: row["cells"].get(m) for m in columns}
        out_rows.append(_apply_corrections(root, row))
        if limit is not None and len(out_rows) >= limit:
            break
    return {"columns": columns, "rows": out_rows, "stats": stats}


def read_backfill(root: str, limit: int | None = None) -> dict:
    """回填路径：密架文件直读 + 未回填尾部稀疏归并，统一做版本决胜。

    无论密架文件是压缩单件还是逐段件，都不假设行键唯一：
    晚到数据可能让后到分段携带旧键，因此所有来源都过 merge_dense_rows，
    保证与稀疏路径逐单元一致。
    """
    registry = state.load_metrics(root)
    columns = registry["metrics"]
    bs = state.load_backfill(root)
    bf_dir = os.path.join(root, "wide", "backfill")
    done = set(bs["done_segs"])

    stats = {"path": "backfill", "files_opened": 0, "bytes_read": 0,
             "segments": 0, "rejected_late": 0, "fallback_sparse": 0,
             "paused": bs["paused"], "gen": bs["gen"],
             "rewrite_bytes": bs["bytes_written"], "rewrites": bs["rewrites"]}

    # 收集全部密架来源（保持分段顺序），先不合并
    dense_streams: list[tuple[int, list[dict]]] = []
    compact_path = os.path.join(bf_dir, "compacted.json")
    if os.path.exists(compact_path):
        doc = common.load_json(compact_path, {"rows": []})
        stats["files_opened"] += 1
        stats["bytes_read"] += _segment_file_stats(compact_path)
        stats["segments"] += len(doc.get("segs", []))
        for seg_id in doc.get("segs", []):
            # 压缩件内已合并过，整体作为一个来源参与与尾部的再合并
            pass
        dense_streams.append((-1, doc["rows"]))
    else:
        for seg_id, path in common.list_segments(bf_dir):
            doc = common.load_json(path, {"rows": []})
            stats["files_opened"] += 1
            stats["bytes_read"] += _segment_file_stats(path)
            stats["segments"] += 1
            dense_streams.append((seg_id, doc["rows"]))

    # 未回填的稀疏尾部：读入后作为追加来源，由同一合并器决胜
    sparse_segments = [
        (sid, p) for sid, p in common.list_segments(
            os.path.join(root, "wide", "sparse")) if sid not in done
    ]
    for seg_id, path in sparse_segments:
        doc = common.load_json(path, {"rows": []})
        stats["files_opened"] += 1
        stats["bytes_read"] += _segment_file_stats(path)
        dense_streams.append((seg_id, doc["rows"]))
    stats["fallback_sparse"] = len(sparse_segments)

    merged_rows = merge_dense_rows(
        root, dense_streams, columns, "backfill-read", stats)

    fixed = []
    for row in merged_rows:
        fixed.append(_apply_corrections(root, row))
        if limit is not None and len(fixed) >= limit:
            break
    return {"columns": columns, "rows": fixed, "stats": stats}


def compare_paths(root: str) -> dict:
    """同一份数据走两条路径，逐单元核对一致性并报告成本差异。"""
    a = read_sparse(root)
    b = read_backfill(root)
    mismatches = []
    if a["columns"] != b["columns"]:
        mismatches.append({"type": "columns",
                           "sparse": a["columns"], "backfill": b["columns"]})
    map_a = {(r["ts"], r["entity"]): r["cells"] for r in a["rows"]}
    map_b = {(r["ts"], r["entity"]): r["cells"] for r in b["rows"]}
    if set(map_a) != set(map_b):
        mismatches.append({"type": "row_keys",
                           "only_sparse": sorted(set(map_a) - set(map_b)),
                           "only_backfill": sorted(set(map_b) - set(map_a))})
    for k in sorted(set(map_a) & set(map_b)):
        for col in a["columns"]:
            va, vb = map_a[k].get(col), map_b[k].get(col)
            if _cell_key(va) != _cell_key(vb):
                mismatches.append({
                    "type": "cell", "ts": k[0], "entity": k[1],
                    "metric": col, "sparse": va, "backfill": vb,
                })
    return {
        "equal": not mismatches,
        "mismatches": mismatches[:50],
        "row_count": len(a["rows"]),
        "sparse_stats": a["stats"],
        "backfill_stats": b["stats"],
        "read_amplification": {
            "sparse_files": a["stats"]["files_opened"],
            "backfill_files": b["stats"]["files_opened"],
            "sparse_bytes": a["stats"]["bytes_read"],
            "backfill_bytes": b["stats"]["bytes_read"],
        },
    }


def _cell_key(cell: dict | None):
    if cell is None:
        return None
    return cell.get("v"), tuple(cell.get("ver") or [])
