"""公共约定：目录布局、原子写、分段命名、版本号与缺失语义。

窄表记录（narrow record）：
    {"ts": int, "entity": str, "metric": str, "v": float,
     "source": str, "ver": [gen:int, batch:int, seq:int]}

宽表单元（cell）：
    {"v": float, "ver": [int, int, int], "source": str}

语义约定：
  * 缺失 == 该 (ts, entity, metric) 不存在任何记录，读取端输出 null，
    绝不与数值 0 混淆；0 是一条真实记录。
  * 版本号 ver 为三等分整数数组，按字典序比较；迟到记录版本更旧则丢弃。
  * 人工修正版本基号 CORRECTION_BASE 远大于任何自动生成版本，
    因此旧任务晚到永远无法覆盖人工修正。
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager
from typing import Any, Iterable

CORRECTION_BASE = 1_000_000_000  # 人工修正 ver[0] 的基号


# ---------------------------------------------------------------- 目录布局

def ensure_dirs(root: str) -> dict[str, str]:
    """初始化并返回全部工作目录。"""
    dirs = {
        "root": root,
        "narrow": os.path.join(root, "narrow"),
        "sparse": os.path.join(root, "wide", "sparse"),
        "backfill": os.path.join(root, "wide", "backfill"),
        "state": os.path.join(root, "state"),
        "web": os.path.join(os.path.dirname(os.path.abspath(__file__)), "web"),
    }
    for name, path in dirs.items():
        if name != "web":
            os.makedirs(path, exist_ok=True)
    return dirs


def seg_path(directory: str, seg_id: int) -> str:
    return os.path.join(directory, f"seg-{seg_id:06d}.json")


def list_segments(directory: str) -> list[tuple[int, str]]:
    """返回 [(seg_id, path)]，按 id 升序；目录不存在则空表。"""
    if not os.path.isdir(directory):
        return []
    out = []
    for name in os.listdir(directory):
        if name.startswith("seg-") and name.endswith(".json"):
            try:
                seg_id = int(name[4:-5])
            except ValueError:
                continue
            out.append((seg_id, os.path.join(directory, name)))
    out.sort()
    return out


# ---------------------------------------------------------------- 原子 IO

def atomic_write_json(path: str, obj: Any) -> int:
    """临时文件 + fsync + rename，保证读者永远看不到半个文件。返回字节数。"""
    tmp = f"{path}.tmp.{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return os.path.getsize(path)


def write_jsonl_line(path: str, obj: Any) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False))
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------- 锁

@contextmanager
def state_lock(root: str):
    """全局状态互斥锁（flock，进程级，自动释放）。"""
    lock_path = os.path.join(root, "state", "lock")
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    fh = open(lock_path, "a+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


# ---------------------------------------------------------------- 版本语义

def cmp_version(a: list[int] | None, b: list[int] | None) -> int:
    """字典序版本比较，返回 -1/0/1；None 视为最旧。"""
    if a is None:
        return -1 if b is not None else 0
    if b is None:
        return 1
    if a < b:
        return -1
    if a > b:
        return 1
    return 0


def merge_cell(old: dict | None, new: dict) -> tuple[dict, str]:
    """两个单元按版本号决胜。

    返回 (胜出单元, 状态)，状态取值：
      new            旧位置为空
      updated        新版本覆盖旧值（正常更新）
      duplicate      版本相同，保留旧值
      rejected_late  新版本更旧，迟到数据被丢弃
    """
    if old is None:
        return new, "new"
    c = cmp_version(new["ver"], old["ver"])
    if c > 0:
        return new, "updated"
    if c == 0:
        return old, "duplicate"
    return old, "rejected_late"


def now_wall() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def iter_batched(items: list, size: int) -> Iterable[list]:
    for i in range(0, len(items), size):
        yield items[i:i + size]
