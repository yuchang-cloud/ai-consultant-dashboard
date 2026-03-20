#!/Users/yuchang/Documents/AI_consultant_data/.venv/bin/python3
"""
AI顾问数据看板 本地服务器
────────────────────────────────────────────────────────
访问地址：http://localhost:8765
提供看板页面和手动刷新接口。

POST /api/refresh  → 触发数据更新
GET  /api/status   → 查询当前更新状态
GET  /             → 看板页面
────────────────────────────────────────────────────────
"""

import subprocess
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, send_file
from flask_cors import CORS  # type: ignore

BASE_DIR   = Path(__file__).parent
SCRIPT     = BASE_DIR / "update_dashboard.py"
DASHBOARD  = BASE_DIR / "dashboard.html"
PYTHON     = BASE_DIR / ".venv/bin/python3"
PORT       = 8765

app = Flask(__name__)
CORS(app)  # 允许 GitHub Pages 的页面调用本地 API

_lock = threading.Lock()
_state: dict = {
    "running":     False,
    "last_run_ts": None,   # Unix timestamp
    "last_result": None,   # "success" | "error: ..."
}


def _run_update() -> None:
    with _lock:
        if _state["running"]:
            return
        _state["running"]     = True
        _state["last_result"] = None

    try:
        proc = subprocess.run(
            [str(PYTHON), str(SCRIPT)],
            capture_output=True, text=True,
            cwd=str(BASE_DIR),
        )
        result = "success" if proc.returncode == 0 else f"error: {(proc.stdout + proc.stderr)[-800:].strip()}"
    except Exception as exc:
        result = f"error: {exc}"

    with _lock:
        _state["running"]     = False
        _state["last_run_ts"] = time.time()
        _state["last_result"] = result


@app.route("/")
def index():
    return send_file(DASHBOARD)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    with _lock:
        already_running = _state["running"]

    if already_running:
        return jsonify({"started": False, "message": "数据更新已在进行中，请稍候..."})

    thread = threading.Thread(target=_run_update, daemon=True)
    thread.start()
    return jsonify({"started": True, "message": "数据更新已启动，约需 2-3 分钟..."})


@app.route("/api/status")
def api_status():
    with _lock:
        snap = dict(_state)
    return jsonify(snap)


if __name__ == "__main__":
    print(f"✅ 看板服务已启动：http://localhost:{PORT}")
    print(f"   按 Ctrl+C 停止")
    app.run(host="127.0.0.1", port=PORT, debug=False)
