"""状态层：测量项注册表、修正覆盖层、回填状态、冲突日志。

所有写操作都应在 common.state_lock(root) 之内进行。
状态文件本身是普通 JSON，通过原子写替换；锁负责多进程互斥。
"""
from __future__ import annotations

import os

from . import common

# 状态文件名
F_METRICS = "metrics.json"
F_CORRECTIONS = "corrections.json"
F_BACKFILL = "backfill.json"
F_CURSOR = "transpose-cursor.json"
F_CONFLICTS = "conflicts.jsonl"

CORRECTION_STATUS = ("accepted", "rejected_late", "duplicate")


def state_path(root: str, name: str) -> str:
    return os.path.join(root, "state", name)


# ---------------------------------------------------------------- 测量项注册表

def load_metrics(root: str) -> dict:
    """{"metrics": [按注册顺序排列的测量项], "gen": 当前模式号}"""
    return common.load_json(state_path(root, F_METRICS),
                            {"metrics": [], "gen": 0})


def save_metrics(root: str, data: dict) -> None:
    common.atomic_write_json(state_path(root, F_METRICS), data)


def register_metrics(root: str, new_names: list[str]) -> tuple[dict, list[str]]:
    """注册新测量项；出现新名字即推进模式号 gen。返回 (注册表, 本次新增)。"""
    data = load_metrics(root)
    known = set(data["metrics"])
    added = []
    for name in new_names:
        if name not in known:
            data["metrics"].append(name)
            known.add(name)
            added.append(name)
    if added:
        data["gen"] += 1
        save_metrics(root, data)
    return data, added


# ---------------------------------------------------------------- 修正覆盖层

def _key(ts: int, entity: str, metric: str) -> str:
    return f"{ts}\x1f{entity}\x1f{metric}"


def load_corrections(root: str) -> dict:
    """{key: {"v", "ver", "source", "at", "note"}}"""
    return common.load_json(state_path(root, F_CORRECTIONS), {})


def save_corrections(root: str, data: dict) -> None:
    common.atomic_write_json(state_path(root, F_CORRECTIONS), data)


def apply_correction(root: str, ts: int, entity: str, metric: str,
                     value: float, note: str = "") -> tuple[dict, str]:
    """写入一条人工修正，与已有覆盖层按版本号决胜。

    修正版本 = [CORRECTION_BASE + 序号, 0, 0]，序号单调递增，
    所以新修正总是胜过旧修正，且任何自动数据都无法覆盖它。
    """
    data = load_corrections(root)
    seq = max((c["ver"][0] - common.CORRECTION_BASE
               for c in data.values()), default=-1) + 1
    cell = {
        "v": value,
        "ver": [common.CORRECTION_BASE + seq, 0, 0],
        "source": "web-correction",
        "at": common.now_wall(),
        "note": note,
    }
    k = _key(ts, entity, metric)
    merged, status = common.merge_cell(data.get(k), cell)
    if status in ("new", "updated"):
        data[k] = merged
        save_corrections(root, data)
        status = "accepted" if status == "new" else "accepted"
    return cell, status


def correction_cell(root: str, ts: int, entity: str, metric: str) -> dict | None:
    return load_corrections(root).get(_key(ts, entity, metric))


# ---------------------------------------------------------------- 回填状态

def default_backfill_state() -> dict:
    return {
        "paused": False,
        "gen": 0,                 # 已回填到的模式号
        "done_segs": [],          # 已按当前 gen 处理完的宽表分段 id
        "bytes_written": 0,       # 累计重写字节（重写成本）
        "rewrites": 0,            # 累计重写次数
        "last_error": None,
        "updated_at": None,
    }


def load_backfill(root: str) -> dict:
    data = common.load_json(state_path(root, F_BACKFILL), None)
    if data is None:
        data = default_backfill_state()
        save_backfill(root, data)
    return data


def save_backfill(root: str, data: dict) -> None:
    data["updated_at"] = common.now_wall()
    common.atomic_write_json(state_path(root, F_BACKFILL), data)


# ---------------------------------------------------------------- 转置游标

def load_cursor(root: str) -> dict:
    return common.load_json(state_path(root, F_CURSOR),
                            {"last_seg": -1})


def save_cursor(root: str, data: dict) -> None:
    common.atomic_write_json(state_path(root, F_CURSOR), data)


# ---------------------------------------------------------------- 冲突日志

def log_conflict(root: str, entry: dict) -> None:
    """追加一条迟到/重复事件（人工修正被拒、自动数据被拒均记这里）。"""
    entry = dict(entry)
    entry["at"] = common.now_wall()
    common.write_jsonl_line(state_path(root, F_CONFLICTS), entry)


def read_conflicts(root: str, limit: int = 200) -> list[dict]:
    rows = common.read_jsonl(state_path(root, F_CONFLICTS))
    return rows[-limit:]
