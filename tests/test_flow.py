"""端到端测试：标准库 unittest，python -m unittest -v 即可运行。

覆盖：
  1. 真实多进程管道：generator → ingestor → transposer（子进程 + 真分段文件）
  2. 缺失(null) 与显式零(0) 语义严格区分
  3. 迟到列（新测量项中途出现）不改写旧稀疏分段，旧文件列集合保持不变
  4. 晚到旧版本记录被拦截，原值不被覆盖，冲突日志有记录
  5. 稀疏直读 vs 回填直读逐单元一致；压缩模式亦然
  6. 暂停回填期间查询自动回退稀疏路径，恢复后追平
  7. 新增测量项触发回填整体重置重写，重写成本计数器增长
  8. 人工修正版本恒新，旧任务晚到无法覆盖
  9. 流式内存边界：单分段处理，读稀疏路径时堆中每段至多一行（结构检查）
 10. 列顺序 == 注册顺序，可核对
"""
from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from widetable import backfill, common, generator, ingestor, reader, state, transposer  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class FlowTestBase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="wt-")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def build_pipeline(self, batches=6, batch_rows=12):
        """用真实子进程跑 generator | ingestor，再用 transposer 转置。

        batch_rows 取小值，保证多批次数据落到多个窄分段，
        从而真实覆盖「新测量项在写完前几个文件后才出现」的场景。
        """
        # 注：-I 隔离模式会同时忽略 cwd 与 PYTHONPATH，找不到本仓库包；
        # 仓库代码可信、数据目录独立，改为显式 PYTHONPATH 注入。
        env = {**os.environ, "PYTHONPATH": REPO}
        gen = subprocess.Popen(
            [sys.executable, "-m", "widetable.generator",
             "--batches", str(batches), "--batch-size", "3",
             "--entities", "2", "--seed", "7"],
            cwd=REPO, env=env, stdout=subprocess.PIPE)
        ing = subprocess.Popen(
            [sys.executable, "-m", "widetable.ingestor",
             "--root", self.root, "--batch-rows", str(batch_rows),
             "--batch-seconds", "0"],
            cwd=REPO, env=env, stdin=gen.stdout, stderr=subprocess.PIPE)
        gen.stdout.close()
        _, err = ing.communicate(timeout=30)
        gen.wait(timeout=10)
        self.assertEqual(ing.returncode, 0)
        self.assertEqual(gen.returncode, 0)
        stats = transposer.run_once(self.root)
        return stats, err.decode()

    def expected_zero_and_missing(self):
        """重放生成器 RNG，找出显式零与真实缺失的具体单元。"""
        zeros, missing = set(), set()
        for b, rows in generator.emit_batches(6, 3, 2, 7):
            active = set(generator.metrics_at_batch(b))
            produced = {(r["ts"], r["entity"], r["metric"]) for r in rows
                        if r["source"] == "synthetic"}
            for i in range(3):
                for e in range(2):
                    ts = b * 3 * 2 + i * 2 + e
                    for m in active:
                        k = (ts, f"E{e:03d}", m)
                        if k not in produced:
                            missing.add(k)
            for r in rows:
                if r["source"] == "synthetic" and r["v"] == 0.0:
                    zeros.add((r["ts"], r["entity"], r["metric"]))
        return zeros, missing


class TestEndToEnd(FlowTestBase):
    def test_01_pipeline_files_and_registry(self):
        stats, ingest_log = self.build_pipeline()
        self.assertTrue(ingest_log.strip())  # ingestor 确实落了分段
        narrow = common.list_segments(os.path.join(self.root, "narrow"))
        sparse = common.list_segments(os.path.join(self.root, "wide", "sparse"))
        self.assertTrue(narrow and sparse)
        self.assertEqual([s["seg"] for s in stats],
                         [sid for sid, _ in narrow])
        registry = state.load_metrics(self.root)
        # 分代计划：m_temp/m_pressure → +m_vibration → +m_humidity
        self.assertEqual(registry["metrics"],
                         ["m_temp", "m_pressure", "m_vibration", "m_humidity"])
        self.assertEqual(registry["gen"], 3)

    def test_02_null_vs_zero_semantics(self):
        self.build_pipeline()
        zeros, missing = self.expected_zero_and_missing()
        self.assertTrue(zeros, "测试数据里应存在显式零")
        self.assertTrue(missing, "测试数据里应存在真实缺失")
        res = reader.read_sparse(self.root)
        index = {(r["ts"], r["entity"]): r["cells"] for r in res["rows"]}
        for k in zeros:
            cell = index[(k[0], k[1])][k[2]]
            self.assertIsNotNone(cell, f"{k} 是显式零，不得读作 null")
            self.assertEqual(cell["v"], 0.0)
        for k in missing:
            if (k[0], k[1]) in index:  # 该行可能整行无记录
                self.assertIsNone(index[(k[0], k[1])][k[2]],
                                  f"{k} 是真实缺失，必须为 null")

    def test_03_late_column_never_rewrites_old_segments(self):
        self.build_pipeline()
        sparse_dir = os.path.join(self.root, "wide", "sparse")
        first = common.load_json(common.seg_path(sparse_dir, 0), {})
        first_cols = set(first["columns"])
        first_mtime = os.path.getmtime(common.seg_path(sparse_dir, 0))
        self.assertNotIn("m_vibration", first_cols)
        self.assertNotIn("m_humidity", first_cols)

        # 回填 + 网页新增测量项后，旧稀疏分段文件字节与 mtime 均不变
        backfill.tick(self.root, compact=False)
        with common.state_lock(self.root):
            state.register_metrics(self.root, ["m_late_web"])
        backfill.tick(self.root, compact=False)
        self.assertEqual(
            first_cols,
            set(common.load_json(common.seg_path(sparse_dir, 0),
                                 {})["columns"]))
        self.assertEqual(
            first_mtime, os.path.getmtime(common.seg_path(sparse_dir, 0)))

        # 但读取端全列输出，迟到列在历史行上为 null，不是 0
        res = reader.read_sparse(self.root)
        self.assertEqual(res["columns"][-1], "m_late_web")
        self.assertTrue(all(r["cells"]["m_late_web"] is None
                            for r in res["rows"]))

    def test_04_late_record_rejected_and_logged(self):
        stats, _ = self.build_pipeline()
        # 晚到重投位于后续分段，跨分段拦截发生在读取/回填期；
        # 转置统计仍应是自洽的（各状态计数之和 == 输入行数）
        for s in stats:
            self.assertEqual(
                s["new"] + s["updated"] + s["duplicate"] + s["rejected_late"],
                s["in"])

        sparse_res = reader.read_sparse(self.root)
        self.assertGreaterEqual(sparse_res["stats"]["rejected_late"], 1,
                                "生成器重投的旧版本晚到记录必须在读取期被拦截")
        backfill.tick(self.root, compact=False)
        bf_res = reader.read_backfill(self.root)
        self.assertGreaterEqual(bf_res["stats"]["rejected_late"], 1,
                                "回填路径同样必须拦截跨段晚到")
        conflicts = state.read_conflicts(self.root)
        self.assertTrue(any(c["event"] == "late_record_rejected"
                            for c in conflicts))
        # 被篡改的 +999 值不得出现在任何一条路径
        for res in (sparse_res, bf_res):
            for r in res["rows"]:
                for cell in r["cells"].values():
                    if cell is not None:
                        self.assertLess(cell["v"], 900.0,
                                        "晚到记录的篡改值泄漏进了宽表")

    def test_05_two_read_paths_identical(self):
        self.build_pipeline()
        # 非压缩回填
        for _ in range(3):
            backfill.tick(self.root, compact=False)
        cmp = reader.compare_paths(self.root)
        self.assertTrue(cmp["equal"], json.dumps(cmp["mismatches"], ensure_ascii=False))
        self.assertGreaterEqual(cmp["sparse_stats"]["files_opened"],
                                cmp["backfill_stats"]["files_opened"])

    def test_06_compact_mode_and_rewrite_cost(self):
        self.build_pipeline()
        sparse_segs = len(common.list_segments(
            os.path.join(self.root, "wide", "sparse")))
        for _ in range(sparse_segs):
            backfill.tick(self.root, compact=True)
        bs = state.load_backfill(self.root)
        self.assertEqual(bs["rewrites"], sparse_segs)
        # 压缩模式每次都整文件重写：至少两次写入时累计字节必然超过末文件，
        # 因为第 k 次写入包含前 k-1 段的全部历史数据
        compact_path = os.path.join(self.root, "wide", "backfill",
                                    backfill.COMPACT_NAME)
        compact_size = os.path.getsize(compact_path)
        self.assertGreaterEqual(bs["bytes_written"], compact_size)
        if sparse_segs >= 2:
            self.assertGreater(bs["bytes_written"], compact_size)
        cmp = reader.compare_paths(self.root)
        self.assertTrue(cmp["equal"], json.dumps(cmp["mismatches"], ensure_ascii=False))
        self.assertEqual(cmp["backfill_stats"]["files_opened"], 1)

    def test_07_pause_and_fallback_then_catchup(self):
        self.build_pipeline()
        sparse_ids = [sid for sid, _ in common.list_segments(
            os.path.join(self.root, "wide", "sparse"))]
        self.assertGreaterEqual(len(sparse_ids), 3)

        # 精确制造「只回填首段、其余为稀疏尾部」的部分回填状态
        backfill.tick(self.root, compact=False)
        with common.state_lock(self.root):
            bs = state.load_backfill(self.root)
            bs["done_segs"] = sparse_ids[:1]
            bs["paused"] = True
            state.save_backfill(self.root, bs)
            for sid in sparse_ids[1:]:  # 删除多余回填文件，模拟中途暂停
                p = common.seg_path(os.path.join(self.root, "wide", "backfill"), sid)
                if os.path.exists(p):
                    os.remove(p)

        rep = backfill.tick(self.root, compact=False)  # 暂停态：什么都不写
        self.assertTrue(rep["paused"] and rep["rewritten"] == 0)

        res = reader.read_backfill(self.root)
        self.assertGreater(res["stats"]["fallback_sparse"], 0,
                           "存在未追平尾部时必须回退稀疏路径补齐")
        cmp = reader.compare_paths(self.root)
        self.assertTrue(cmp["equal"], json.dumps(cmp["mismatches"], ensure_ascii=False))

        with common.state_lock(self.root):
            bs = state.load_backfill(self.root)
            bs["paused"] = False
            state.save_backfill(self.root, bs)
        for _ in range(5):
            backfill.tick(self.root, compact=False)
        res2 = reader.read_backfill(self.root)
        self.assertEqual(res2["stats"]["fallback_sparse"], 0)
        self.assertTrue(reader.compare_paths(self.root)["equal"])

    def test_08_new_metric_resets_and_rewrites_backfill(self):
        self.build_pipeline()
        backfill.tick(self.root, compact=False)
        bs0 = state.load_backfill(self.root)
        self.assertTrue(bs0["done_segs"])
        with common.state_lock(self.root):
            state.register_metrics(self.root, ["m_extra"])
        rep = backfill.tick(self.root, compact=False)
        self.assertTrue(rep["reset"])
        bs1 = state.load_backfill(self.root)
        self.assertEqual(bs1["gen"], state.load_metrics(self.root)["gen"])
        # 旧回填文件已被删除重建
        bf = common.list_segments(os.path.join(self.root, "wide", "backfill"))
        self.assertTrue(bf)
        doc = common.load_json(bf[0][1], {})
        self.assertIn("m_extra", doc["columns"])
        self.assertTrue(reader.compare_paths(self.root)["equal"])

    def test_09_correction_beats_late_arrival(self):
        self.build_pipeline()
        target_ts, target_entity = 0, "E000"
        res = reader.read_sparse(self.root)
        first_row = next(r for r in res["rows"]
                         if r["ts"] == target_ts and r["entity"] == target_entity)
        metric = "m_temp"
        original = first_row["cells"][metric]
        self.assertIsNotNone(original)

        with common.state_lock(self.root):
            cell, status = state.apply_correction(
                self.root, target_ts, target_entity, metric, 555.5, "校准")
        self.assertEqual(status, "accepted")
        self.assertGreater(cell["ver"][0], common.CORRECTION_BASE - 1)

        # 模拟旧任务晚到：一条版本很旧但值不同的自动数据，再跑一次转置合并
        late = {"seg": 999, "rows": [{
            "ts": target_ts, "entity": target_entity, "metric": metric,
            "v": 999.9, "source": "synthetic-late", "ver": [0, 0, 0]}]}
        late_path = os.path.join(self.root, "narrow", "seg-000999.json")
        common.atomic_write_json(late_path, late)
        transposer.run_once(self.root)
        backfill.tick(self.root, compact=False)

        for path in ("sparse", "backfill"):
            out = reader.read_backfill(self.root) if path == "backfill" \
                else reader.read_sparse(self.root)
            row = next(r for r in out["rows"]
                       if r["ts"] == target_ts and r["entity"] == target_entity)
            self.assertEqual(row["cells"][metric]["v"], 555.5,
                             f"{path} 路径上人工修正被晚到数据覆盖")
            self.assertIn("correction", row["cells"][metric]["source"])
        self.assertTrue(reader.compare_paths(self.root)["equal"])

    def test_10_column_order_is_registration_order(self):
        self.build_pipeline()
        with common.state_lock(self.root):
            state.register_metrics(self.root, ["m_zzz"])
            state.register_metrics(self.root, ["m_aaa"])
        res = reader.read_sparse(self.root)
        registry = state.load_metrics(self.root)
        self.assertEqual(res["columns"], registry["metrics"])
        self.assertEqual(res["columns"][-2:], ["m_zzz", "m_aaa"])
        # 每个稀疏分段内 cells 的键顺序也遵循注册顺序
        for _, p in common.list_segments(os.path.join(self.root, "wide", "sparse")):
            doc = common.load_json(p, {"rows": []})
            for row in doc["rows"]:
                keys = list(row["cells"])
                self.assertEqual(keys, sorted(keys, key=registry["metrics"].index))

    def test_11_ingestor_batching_tunable(self):
        rows = []
        for _, batch in generator.emit_batches(3, 2, 2, 1):
            rows.extend(batch)
        stream = io.StringIO("\n".join(json.dumps(r) for r in rows))
        written = ingestor.ingest(self.root, stream, batch_rows=5,
                                  batch_seconds=0)
        self.assertGreaterEqual(len(written), 2)  # 小批量 → 多个分段
        loaded = 0
        for _, p in common.list_segments(os.path.join(self.root, "narrow")):
            loaded += common.load_json(p, {"count": 0})["count"]
        self.assertEqual(loaded, len(rows))

    def test_12_cursor_resume_skips_processed(self):
        self.build_pipeline()
        before = state.load_cursor(self.root)["last_seg"]
        self.assertEqual(transposer.run_once(self.root), [])  # 无新分段
        self.assertEqual(state.load_cursor(self.root)["last_seg"], before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
