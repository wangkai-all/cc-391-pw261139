"""虚构窄表数据生成器。

特性：
  * 批量可调（--batches / --batch-size / --entities），确定性随机（--seed）。
  * 测量项分代出现：首批只有 m_temp/m_pressure，第 2 批起出现 m_vibration，
    第 4 批起出现 m_humidity —— 用于验证「迟到列不改旧文件、不被当成 0」。
  * 缺失语义可核对：约 12% 的单元直接不产生记录（读取端应为 null）；
    另有约 6% 的单元显式产生数值 0（读取端应为 0，与 null 严格区分）。
  * 晚到/重复：从第 2 批起，每批重投一条首批旧单元（旧版本号 + 不同值），
    并在第 3 批重投一条完全相同版本号的记录，供冲突链路核对。

输出为 stdout 上的 JSONL，每行一条窄表记录；
由 ingestor 经管道消费，两个进程各自独立。
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time
from typing import Iterator

# 测量项分代计划：{批次下标: 新增测量项}
METRIC_PLAN: dict[int, list[str]] = {
    0: ["m_temp", "m_pressure"],
    2: ["m_vibration"],
    4: ["m_humidity"],
}


def metrics_at_batch(batch_idx: int) -> list[str]:
    active: list[str] = []
    for start, names in sorted(METRIC_PLAN.items()):
        if batch_idx >= start:
            active.extend(names)
    return active


def generation_of_batch(batch_idx: int) -> int:
    """批次对应的模式号（与 state.register_metrics 的 gen 对齐，从 1 起）。"""
    return sum(1 for start in METRIC_PLAN if batch_idx >= start)


def emit_batches(batches: int, batch_size: int, entities: int,
                 seed: int) -> Iterator[tuple[int, list[dict]]]:
    """逐批产出 (批次号, 窄表记录列表)。

    同一批内时间戳连续：ts = batch * batch_size * entities + 行内序号。
    版本号 ver=[模式号, 批次号, 批内序号]，由数据源（本生成器）赋予。
    """
    rng = random.Random(seed)
    first_cells: list[dict] = []          # 首批单元，供之后晚到重投
    duplicate_used = False

    for b in range(batches):
        gen = generation_of_batch(b)
        metrics = metrics_at_batch(b)
        rows: list[dict] = []
        seq = 0

        def next_ver() -> list[int]:
            return [gen, b, seq]

        for i in range(batch_size):
            for entity_idx in range(entities):
                ts = b * batch_size * entities + i * entities + entity_idx
                entity = f"E{entity_idx:03d}"
                for metric in metrics:
                    roll = rng.random()
                    if roll < 0.12:
                        continue  # 真实缺失：不产生任何记录
                    if roll < 0.18:
                        value = 0.0  # 显式零：与缺失严格区分
                    else:
                        value = round(rng.uniform(0, 100), 4)
                    rec = {
                        "ts": ts,
                        "entity": entity,
                        "metric": metric,
                        "v": value,
                        "source": "synthetic",
                        "ver": next_ver(),
                    }
                    seq += 1
                    rows.append(rec)
                    if b == 0:
                        first_cells.append(rec)

        # 晚到重投：版本号明确更旧（旧任务延迟送达）、值被篡改，必须被拒。
        # 注意它与下面的「完全重复」区分：版本更旧 = rejected_late，
        # 版本相同 = duplicate，两条冲突路径都要可核对。
        if b >= 2 and first_cells:
            victim = first_cells[b % len(first_cells)]
            old_ver = list(victim["ver"])
            # 模式号减一保证严格更旧（即便原记录批内序号为 0 也成立）
            old_ver[0] = max(0, old_ver[0] - 1)
            rows.append({
                "ts": victim["ts"],
                "entity": victim["entity"],
                "metric": victim["metric"],
                "v": round(victim["v"] + 999.0, 4),  # 明显错误的值
                "source": "synthetic-late",
                "ver": old_ver,
            })

        # 完全重复（同版本号）重投一次
        if b == 3 and first_cells and not duplicate_used:
            victim = first_cells[0]
            rows.append(dict(victim))
            duplicate_used = True

        yield b, rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="虚构窄表数据生成器（JSONL → stdout）")
    ap.add_argument("--batches", type=int, default=8)
    ap.add_argument("--batch-size", type=int, default=4,
                    help="每批每个实体的时间点数；批量越大单批内存占用越高")
    ap.add_argument("--entities", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--stream-delay", type=float, default=0.0,
                    help="每批之间暂停秒数，模拟持续输入")
    args = ap.parse_args(argv)

    for _, rows in emit_batches(args.batches, args.batch_size,
                                args.entities, args.seed):
        for rec in rows:
            sys.stdout.write(json.dumps(rec, ensure_ascii=False) + "\n")
        sys.stdout.flush()
        if args.stream_delay:
            time.sleep(args.stream_delay)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
