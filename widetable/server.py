"""分析/管理 Web 服务（仅标准库 http.server）。

API：
  GET  /api/state                 注册表、回填状态、分段计数、最近冲突
  POST /api/metrics               {"metric": "m_x"} 新增测量项（推进 gen）
  POST /api/corrections           {"ts","entity","metric","v","note?"} 修正单元
  POST /api/backfill/pause        暂停回填
  POST /api/backfill/resume       恢复回填
  GET  /api/read?path=sparse|backfill&limit=N
  GET  /api/compare               两条读取路径逐单元对比 + 成本对照
  GET  /                           单页网页（web/index.html）
"""
from __future__ import annotations

import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import common, reader, state

ROOT: str = ""  # main() 注入


class Handler(BaseHTTPRequestHandler):
    server_version = "widetable/1.0"

    # ------------------------------------------------------------ 工具
    def _send_json(self, obj, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def log_message(self, fmt, *args):  # 安静一点
        pass

    # ------------------------------------------------------------ GET
    def do_GET(self) -> None:
        url = urlparse(self.path)
        if url.path == "/api/state":
            return self._send_json(self._state())
        if url.path == "/api/read":
            qs = parse_qs(url.query)
            path = qs.get("path", ["sparse"])[0]
            limit = qs.get("limit", [None])[0]
            limit = int(limit) if limit else None
            if path == "backfill":
                return self._send_json(reader.read_backfill(ROOT, limit))
            return self._send_json(reader.read_sparse(ROOT, limit))
        if url.path == "/api/compare":
            return self._send_json(reader.compare_paths(ROOT))
        if url.path in ("/", "/index.html"):
            return self._serve_static("index.html", "text/html; charset=utf-8")
        if url.path.startswith("/static/"):
            name = os.path.basename(url.path[len("/static/"):])
            ctype = ("text/css; charset=utf-8" if name.endswith(".css")
                     else "application/javascript; charset=utf-8")
            return self._serve_static(name, ctype)
        self._send_json({"error": "not found"}, 404)

    def _serve_static(self, name: str, ctype: str) -> None:
        web_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
        path = os.path.join(web_dir, name)
        if not os.path.isfile(path):
            return self._send_json({"error": "not found"}, 404)
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _state(self) -> dict:
        registry = state.load_metrics(ROOT)
        bs = state.load_backfill(ROOT)
        cursor = state.load_cursor(ROOT)
        return {
            "metrics": registry["metrics"],
            "gen": registry["gen"],
            "backfill": bs,
            "cursor": cursor,
            "narrow_segments": len(common.list_segments(
                os.path.join(ROOT, "narrow"))),
            "sparse_segments": len(common.list_segments(
                os.path.join(ROOT, "wide", "sparse"))),
            "backfill_segments": len(common.list_segments(
                os.path.join(ROOT, "wide", "backfill"))),
            "corrections": state.load_corrections(ROOT),
            "recent_conflicts": state.read_conflicts(ROOT, 30),
        }

    # ------------------------------------------------------------ POST
    def do_POST(self) -> None:
        url = urlparse(self.path)
        try:
            body = self._read_body()
        except (json.JSONDecodeError, ValueError) as exc:
            return self._send_json({"error": f"bad json: {exc}"}, 400)

        if url.path == "/api/metrics":
            return self._add_metric(body)
        if url.path == "/api/corrections":
            return self._add_correction(body)
        if url.path == "/api/backfill/pause":
            return self._set_pause(True)
        if url.path == "/api/backfill/resume":
            return self._set_pause(False)
        self._send_json({"error": "not found"}, 404)

    def _add_metric(self, body: dict) -> None:
        name = (body.get("metric") or "").strip()
        if not name or not name.replace("_", "").isalnum():
            return self._send_json(
                {"error": "metric 需为字母数字/下划线组合"}, 400)
        with common.state_lock(ROOT):
            registry, added = state.register_metrics(ROOT, [name])
        # 回填进程下一 tick 发现 gen 不一致会自动整体重置重写
        return self._send_json({"added": added, "metrics": registry["metrics"],
                                "gen": registry["gen"]})

    def _add_correction(self, body: dict) -> None:
        try:
            ts = int(body["ts"])
            entity = str(body["entity"])
            metric = str(body["metric"])
            value = float(body["v"])
        except (KeyError, TypeError, ValueError):
            return self._send_json(
                {"error": "需要 ts:int, entity:str, metric:str, v:float"}, 400)
        with common.state_lock(ROOT):
            cell, status = state.apply_correction(
                ROOT, ts, entity, metric, value, str(body.get("note", "")))
            state.log_conflict(ROOT, {
                "stage": "web", "ts": ts, "entity": entity, "metric": metric,
                "event": f"correction_{status}", "applied_ver": cell["ver"],
                "value": value,
            })
        self._send_json({"status": status, "cell": cell})

    def _set_pause(self, paused: bool) -> None:
        with common.state_lock(ROOT):
            bs = state.load_backfill(ROOT)
            bs["paused"] = paused
            state.save_backfill(ROOT, bs)
        self._send_json({"paused": paused, "backfill": bs})


def main(argv: list[str] | None = None) -> int:
    global ROOT
    ap = argparse.ArgumentParser(description="宽表分析/管理 Web 服务")
    ap.add_argument("--root", required=True)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args(argv)
    ROOT = args.root
    common.ensure_dirs(ROOT)
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"widetable server on http://{args.host}:{args.port}  root={ROOT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
