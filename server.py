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

from flask import Flask, jsonify, request, send_file

BASE_DIR   = Path(__file__).parent
SCRIPT     = BASE_DIR / "update_dashboard.py"
DASHBOARD  = BASE_DIR / "dashboard.html"
PYTHON     = BASE_DIR / ".venv/bin/python3"
PORT       = 8765

ALLOWED_ORIGINS = {
    "https://yuchang-cloud.github.io",
    "http://localhost:8765",
    "http://127.0.0.1:8765",
    "null",  # 本地直接打开 HTML 文件时
}

app = Flask(__name__)

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


def _cors_headers(response):
    """添加 CORS 和 Private Network Access 头，让 GitHub Pages (HTTPS) 可以调用 localhost。"""
    origin = request.headers.get("Origin", "")
    if origin in ALLOWED_ORIGINS or not origin:
        response.headers["Access-Control-Allow-Origin"]          = origin or "*"
        response.headers["Access-Control-Allow-Methods"]         = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"]         = "Content-Type"
        response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response


@app.after_request
def after_request(response):
    return _cors_headers(response)


@app.route("/api/refresh", methods=["OPTIONS", "POST"])
def api_refresh():
    if request.method == "OPTIONS":
        return _cors_headers(app.make_default_options_response())
    with _lock:
        already_running = _state["running"]
    if already_running:
        return jsonify({"started": False, "message": "数据更新已在进行中，请稍候..."})
    thread = threading.Thread(target=_run_update, daemon=True)
    thread.start()
    return jsonify({"started": True, "message": "数据更新已启动，约需 2-3 分钟..."})


@app.route("/api/status", methods=["GET", "OPTIONS"])
def api_status():
    if request.method == "OPTIONS":
        return _cors_headers(app.make_default_options_response())
    with _lock:
        snap = dict(_state)
    return jsonify(snap)


@app.route("/")
def index():
    return send_file(DASHBOARD)


if __name__ == "__main__":
    print(f"✅ 看板服务已启动：http://localhost:{PORT}")
    print(f"   GitHub Pages 上的刷新按钮也会连接此服务")
    print(f"   按 Ctrl+C 停止")
    app.run(host="127.0.0.1", port=PORT, debug=False)
