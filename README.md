# 流式窄表转宽表（分段稀疏 + 后台回填）

持续输入的窄表记录 `(ts, entity, metric, v)` 转成按 `(ts, entity)` 展开、
测量项为列的宽表。新测量项可能在写完前几个文件后才出现；接入、转置、
回填、分析读取各自是独立进程，通过分段文件与状态目录协作。

零第三方依赖，Python 3.11 标准库实现。

## 快速开始

```bash
# 完整四进程 + 网页（数据目录默认 ./demo-data，已被 gitignore）
bash scripts/run_demo.sh

# 仅跑测试（12 项端到端用例，含真实多进程管道）
python3 -m unittest discover -s tests -v

# 两种回填策略的读放大 / 重写成本对照
bash scripts/compare_strategies.sh
```

网页打开 <http://127.0.0.1:8000>：注册测量项、修正单元、暂停/恢复回填、
切换两条读取路径并逐单元比较。

## 进程与数据流

```
generator ──JSONL/stdout──▶ ingestor ──narrow/seg-N.json──▶ transposer
 (虚构数据,可调批量)        (按批原子落盘)    │             (逐段转置,游标续跑)
                                              ▼
                                   wide/sparse/seg-N.json  (稀疏,不可变)
                                              │
                    ┌─────────────────────────┴────────────────────────┐
                    ▼                                                  ▼
          backfill 进程(可暂停)                              reader 稀疏直读路径
          wide/backfill/ (密架)                               (堆归并,读取时拼接)
                    └─────────────────────────┬────────────────────────┘
                                              ▼
                                   server 进程 / 分析读取端 + 网页
```

各进程独立运行、无共享内存：`generator | ingestor` 是真管道；
`transposer`、`backfill` 按 `--poll` 轮询新分段；`server` 只读磁盘状态。
互斥由 `state/lock` 上的 `flock` 保证，文件写入全部走
临时文件 + fsync + rename，读者永远看不到半个文件。

## 关键语义（可逐条核对）

1. **缺失 ≠ 零**。生成器约 12% 的单元不产生记录（读取端输出 `null`），
   约 6% 显式产生数值 `0.0`（读取端输出 `0`）。宽表任何路径都不会把
   缺失补成 0；`test_02` 重放 RNG 逐单元核对这两类语义。
2. **迟到列不回改旧文件**。测量项在首次出现的分段里注册并推进模式号
   `gen`，旧稀疏分段保持字节不变（`test_03` 同时校验列集合与 mtime）。
   旧分段没有的列，读取端按 `null` 处理。
3. **版本号决胜，旧任务晚到不覆盖新值**。每条记录带
   `ver=[模式号, 批次, 批内序号]`，按字典序比较：新版本覆盖、相同版本
   保留先到者、更旧版本记 `rejected_late` 并写 `state/conflicts.jsonl`。
   跨分段的晚到在读取/回填合并期拦截（稀疏文件不可变，转置只做段内合并）。
4. **人工修正恒新**。修正版本基号 `CORRECTION_BASE=1_000_000_000`，
   任何自动数据都无法覆盖；修正只进 `state/corrections.json` 覆盖层，
   读取期在两条路径上统一叠加，不触发分段重写。`test_09` 用篡改值的
   旧版本记录冲击已修正单元，确认原值不变。
5. **列顺序 = 注册顺序**。`state/metrics.json` 即唯一列序来源，
   分段内 `cells` 键序与读取端列序都对齐它（`test_10`）。
6. **不全量装内存**。接入器只缓冲一个批量；转置器一次处理一个分段；
   稀疏读取用小顶堆做 k 路归并，任一时刻每个分段只有一行在内存。

## 两种物化策略的成本权衡

| | 稀疏直读（保留分段 + 读取时拼接） | 后台回填（密架一致结构） |
|---|---|---|
| 新测量项到达 | 零重写，旧文件不动 | gen 变化即整体作废、从头重写 |
| 读放大 | 打开全部 N 个分段 | 非压缩 M 个文件；`--compact` 仅 1 个 |
| 写/重写成本 | 0 | 非压缩每段写一次；压缩模式第 k 段重写前 k-1 段历史，累计写字节随 N² 量级增长 |
| 暂停影响 | 无 | 暂停期查询自动回退稀疏尾部，结果仍正确 |
| 人工修正 | 读取期叠加，零重写 | 同样只进覆盖层，零重写 |

`compare_strategies.sh` 的实测输出（8 个分段）：压缩模式读取只开
1 个文件，但累计重写 38.5KB；非压缩与稀疏各写/读约 8–9.5KB。
回填状态里的 `bytes_written` / `rewrites` 把重写成本持续量化，
网页顶栏直接展示。

**选择建议**：测量项稳定、查询频繁且想限制打开文件数时用回填（优先
非压缩，仅在分段极多时考虑压缩）；测量项仍在演化、写入持续不断时
保留稀疏分段，把合并成本推迟到读取期，避免反复重写历史。

## 目录布局

```
<root>/
  narrow/seg-NNNNNN.json      接入器落盘的窄表分段
  wide/sparse/seg-NNNNNN.json 转置输出的稀疏宽分段（不可变）
  wide/backfill/              回填密架分段（或 compacted.json）
  state/
    metrics.json              测量项注册表 + 模式号 gen（列序权威）
    transpose-cursor.json     转置游标，断点续跑
    backfill.json             回填暂停位、gen、done_segs、重写成本计数
    corrections.json          人工修正覆盖层
    conflicts.jsonl           晚到/重复/修正事件流水
    lock                      flock 互斥锁
```

## 命令清单

```bash
export PYTHONPATH=$PWD   # 仓库根目录

# 单独运行各进程（均可独立起停）
python -m widetable.generator  --batches 8 --batch-size 4 --entities 3 --seed 42
python -m widetable.ingestor   --root $ROOT --batch-rows 50 --batch-seconds 1
python -m widetable.transposer --root $ROOT --poll 0.5 [--once]
python -m widetable.backfill   --root $ROOT --poll 0.5 [--compact] [--once]
python -m widetable.server     --root $ROOT --port 8000
```

## API 摘要

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/api/state` | 注册表、gen、回填状态与成本、分段计数、最近冲突 |
| GET | `/api/read?path=sparse\|backfill&limit=N` | 两条读取路径之一 |
| GET | `/api/compare` | 两路径逐单元对比 + 读放大/重写成本对照 |
| POST | `/api/metrics` | `{"metric":"m_x"}` 注册测量项，推进 gen |
| POST | `/api/corrections` | `{"ts","entity","metric","v","note?"}` 人工修正 |
| POST | `/api/backfill/pause` `/resume` | 暂停/恢复回填 |
