#!/Users/yuchang/Documents/AI_consultant_data/.venv/bin/python3
"""
AI顾问数据看板 自动刷新脚本
────────────────────────────────────────────────────────
运行时机：每天 12:00（由 macOS LaunchAgent 调度）
数据范围：约课日期(study_date) 从 DATA_START 到昨天
数据来源：Mario SQL模板 #24773（bigdata MCP HTTP API）

完整数据逻辑说明见 dashboard.html 中的注释块。
────────────────────────────────────────────────────────
"""

import csv
import io
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import requests

# ═══════════════════════════════ 配置 ═══════════════════════════════

MCP_URL     = "https://bigdata-mcp.zhenguanyu.com/mcp"
# 优先读取环境变量（GitHub Actions 中由 Secret 注入），回退到本地默认值
MCP_TOKEN   = os.environ.get("MCP_TOKEN", "qcugTRqOjTfQslFzLsUkqmXobeQLfbNj")
TEMPLATE_ID = 24773
# 使用脚本所在目录的相对路径，本地和 GitHub Actions 均适用
BASE_DIR    = Path(__file__).parent
DASHBOARD   = BASE_DIR / "dashboard.html"
LOG_FILE    = BASE_DIR / "dashboard_update.log"

# 数据起始约课日期（固定，看板从这一天开始统计）
DATA_START  = "2025-12-22"

# ── 顾问分类 ─────────────────────────────────────────────────────────
AI_LDAPS      = {"zhongdong", "zhangyuqingbj03", "zhanghuijiebj"}
# wuqiongzz02 和空白均不纳入任何统计（既不是AI也不是真人）
EXCLUDE_LDAPS = {"wuqiongzz02", ""}

# ── 渠道 ID 列表（与 dashboard.html 中的 CHANNELS 保持一致）──────────
CHANNEL_IDS = ["zebra-paid", "points-mall", "oral-app", "xiaohongshu", "weibo"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("dashboard_updater")


# ═══════════════════════════════ MCP 客户端 ═══════════════════════════

class MCPClient:
    """
    轻量级 MCP Streamable HTTP 客户端。
    支持 JSON 和 SSE 两种响应格式。
    """

    def __init__(self, url: str, token: str):
        self.url        = url
        self.token      = token
        self.session_id = None
        self._req_id    = 0

    def _next_id(self) -> int:
        self._req_id += 1
        return self._req_id

    def _headers(self) -> dict:
        h = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json, text/event-stream",
        }
        if self.session_id:
            h["Mcp-Session-Id"] = self.session_id
        return h

    def _post(self, payload: dict, timeout: int = 120) -> dict:
        """
        发送 JSON-RPC 请求，自动处理 JSON 和 SSE 两种响应格式。
        """
        resp = requests.post(
            self.url, json=payload, headers=self._headers(),
            timeout=timeout, stream=True,
        )
        resp.raise_for_status()

        sid = resp.headers.get("Mcp-Session-Id")
        if sid:
            self.session_id = sid

        ct = resp.headers.get("Content-Type", "")
        if "text/event-stream" in ct:
            # 解析 SSE 流，找到与请求 id 对应的 data 事件
            target_id = payload.get("id")
            for raw_line in resp.iter_lines():
                line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
                if line.startswith("data: ") and line[6:].strip():
                    msg = json.loads(line[6:])
                    if target_id is None or msg.get("id") == target_id:
                        return msg
            return {}
        else:
            return resp.json()

    def initialize(self):
        """执行 MCP 握手初始化。"""
        rsp = self._post({
            "jsonrpc": "2.0", "id": self._next_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "dashboard-updater", "version": "1.0"},
            },
        })
        # 发送 initialized 通知（不需要等响应）
        requests.post(
            self.url,
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            headers=self._headers(), timeout=15,
        )
        return rsp

    def tool_call(self, name: str, arguments: dict) -> dict:
        """
        调用指定 MCP 工具，返回解析后的 API 结果。
        工具响应格式：result.content[].text（JSON字符串）
        """
        rsp = self._post({
            "jsonrpc": "2.0", "id": self._next_id(),
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        }, timeout=180)

        if rsp and "result" in rsp:
            for c in rsp["result"].get("content", []):
                if c.get("type") == "text":
                    return json.loads(c["text"])

        if rsp and "error" in rsp:
            raise RuntimeError(f"MCP error ({name}): {rsp['error']}")

        raise RuntimeError(f"Unexpected MCP response from {name}: {rsp}")


# ═══════════════════════════════ 数据查询 ═══════════════════════════

def submit_query(mcp: MCPClient, yesterday: str) -> int:
    """
    提交 SQL 模板查询，返回 queryId。
    查询约课日期：>= DATA_START（不设上限，已预约的未来课程也纳入）。
    截止取数日期（指标计算截止）由 SQL 模板内部使用 yesterday 控制。
    """
    result = mcp.tool_call("sqlTemplate_run", {
        "templateId": TEMPLATE_ID,
        "runName": f"dashboard_auto_{yesterday}",
        "params": [{
            "name":      "study_date",
            "aliasName": "约课日期",
            "operator":  ">=",
            "value":     DATA_START,
            "decimal":   False,
            "required":  True,
        }],
    })
    if result.get("status") != "SUCCESS":
        raise RuntimeError(f"sqlTemplate_run failed: {result}")
    query_id = result["data"]["queryId"]
    log.info(f"Query submitted: queryId={query_id}")
    return query_id


def wait_query_done(mcp: MCPClient, query_id: int, max_wait: int = 1800) -> None:
    """
    轮询查询状态，直到完成或超时。
    每轮最多等 90 秒（低于 HTTP 超时 180s），循环重试直到 max_wait。
    """
    log.info(f"Waiting for query {query_id} (max {max_wait}s)...")
    poll_timeout = 90
    deadline = time.time() + max_wait
    while True:
        result = mcp.tool_call("adhoc_getQueryResult", {
            "queryId":        query_id,
            "timeoutSeconds": poll_timeout,
        })
        data = result.get("data", {})
        if isinstance(data, dict):
            status = data.get("status", "")
        else:
            status = str(data)
        if status in ("SUCCESS", "FINISHED", "DONE", "成功"):
            log.info(f"Query {query_id} completed. Status={status}")
            return
        failed_keywords = ("FAILED", "ERROR", "失败", "CANCELLED")
        if any(k in status.upper() for k in failed_keywords):
            raise RuntimeError(f"Query {query_id} failed. Status={status}, result={result}")
        if time.time() >= deadline:
            raise RuntimeError(f"Query {query_id} timed out after {max_wait}s. Last status={status}")
        log.info(f"Query {query_id} still running (status={status}), polling again...")


def get_csv_url(mcp: MCPClient, query_id: int) -> str:
    """获取完整数据的 CSV 下载链接。"""
    result = mcp.tool_call("adhoc_getQueryDownloadUrl", {"queryId": query_id})
    if result.get("status") != "SUCCESS":
        raise RuntimeError(f"adhoc_getQueryDownloadUrl failed: {result}")
    log.info(f"adhoc_getQueryDownloadUrl result structure: {type(result['data'])} = {str(result['data'])[:500]}")
    data = result["data"]
    if isinstance(data, str):
        url = data
    elif isinstance(data, dict):
        url = data.get("url") or data.get("downloadUrl") or data.get("csvUrl") or next(iter(data.values()))
    else:
        raise RuntimeError(f"Unexpected data type in adhoc_getQueryDownloadUrl: {type(data)}")
    log.info(f"Download URL obtained")
    return url


def download_rows(url: str) -> list:
    """
    下载 CSV 文件，返回行列表（list of dict）。
    CSV 文件为 UTF-8-BOM 编码。
    """
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    resp = requests.get(url, timeout=300)
    resp.raise_for_status()
    content = resp.content.decode("utf-8-sig")
    reader  = csv.DictReader(io.StringIO(content))
    rows    = list(reader)
    log.info(f"Downloaded {len(rows)} rows from CSV")
    return rows


# ═══════════════════════════════ 分类逻辑 ═══════════════════════════

def classify_consultant(ldap: str):
    """
    返回顾问类型字符串，或 None（不纳入统计）。
      AI顾问  ：zhongdong / zhangyuqingbj03 / zhanghuijiebj
      不统计   ：wuqiongzz02 / 空白
      真人顾问 ：其余所有
    """
    ldap = str(ldap or "").strip()
    if ldap in EXCLUDE_LDAPS:
        return None
    return "AI顾问" if ldap in AI_LDAPS else "真人顾问"


def classify_channel(row: dict):
    """
    返回渠道 ID 字符串，或 None（不属于5个统计渠道）。

    映射规则（优先级从上到下）：
      xiaohongshu : 投放渠道（二级）含"小红书"
      weibo       : 投放渠道（二级）含"微博"
      oral-app    : keyfrom 以 "kyapp-" 开头
      zebra-paid  : keyfrom 以 "app-" 开头 | keyfrom = "gwlj" | 用户来源=集团流量
      points-mall : 投放渠道（二级）含 "商城/赠课/商务合作"
    """
    keyfrom    = str(row.get("keyfrom",          "") or "").strip().lower()
    ad_channel = str(row.get("投放渠道（二级）",  "") or "").strip()
    user_from  = str(row.get("用户来源（一级）",  "") or "").strip()

    if "小红书" in ad_channel:                               return "xiaohongshu"
    if "微博"   in ad_channel:                               return "weibo"
    if keyfrom.startswith("kyapp-"):                          return "oral-app"
    if keyfrom.startswith("app-") or keyfrom == "gwlj" \
            or "集团流量" in user_from:                       return "zebra-paid"
    if any(k in ad_channel for k in ("商城", "赠课", "商务合作")):
        return "points-mall"
    return None


# ═══════════════════════════════ 聚合计算 ═══════════════════════════

ZERO_COUNTS = {
    "n": 0, "jw": 0, "dk": 0, "wk": 0, "xf": 0,
    "jw_dk": 0, "jw_wk": 0, "jw_xf": 0, "jw_wk_xf": 0,
}


def aggregate(rows: list) -> dict:
    """
    将一组原始行聚合为9个人数字段。

    去重规则：按用户ID去重，同一用户多条记录时各二分字段取 MAX（OR）。
    字段映射：
      n       = 用户ID去重总人数
      jw      = 是否加微 = 1 的人数
      dk      = 是否到课（截止D6）= 1 的人数
      wk      = 是否完课（截止D6）= 1 的人数
      xf      = 是否续费（截止D6）= 1 的人数
      jw_dk   = 加微 AND 到课 的人数
      jw_wk   = 加微 AND 完课 的人数
      jw_xf   = 加微 AND 转化 的人数
      jw_wk_xf= 加微 AND 完课 AND 转化 的人数
    """
    FLAG_COLS = [
        "是否加微", "是否到课（截止D6）", "是否完课（截止D6）", "是否续费（截止D6）",
    ]
    seen: dict = {}  # uid -> {col: bool}

    for row in rows:
        uid = str(row.get("用户ID", "") or "").strip()
        if not uid:
            continue
        flags = {col: str(row.get(col, "0") or "0").strip() == "1" for col in FLAG_COLS}
        if uid not in seen:
            seen[uid] = flags
        else:
            # 同一用户多行取 OR（有任意一行为1即算1）
            for col in FLAG_COLS:
                if flags[col]:
                    seen[uid][col] = True

    vals = list(seen.values())
    jw_f = [v["是否加微"]           for v in vals]
    dk_f = [v["是否到课（截止D6）"]  for v in vals]
    wk_f = [v["是否完课（截止D6）"]  for v in vals]
    xf_f = [v["是否续费（截止D6）"]  for v in vals]

    return {
        "n":       len(vals),
        "jw":      sum(jw_f),
        "dk":      sum(dk_f),
        "wk":      sum(wk_f),
        "xf":      sum(xf_f),
        "jw_dk":   sum(j and d for j, d in zip(jw_f, dk_f)),
        "jw_wk":   sum(j and w for j, w in zip(jw_f, wk_f)),
        "jw_xf":   sum(j and x for j, x in zip(jw_f, xf_f)),
        "jw_wk_xf":sum(j and w and x for j, w, x in zip(jw_f, wk_f, xf_f)),
    }


# ═══════════════════════════════ 数据处理 ═══════════════════════════

def week_label(study_week_str: str) -> str:
    """
    将数据库返回的学习周日期字符串转为看板显示格式。
    例：'2026-03-16' → '03-16周'，'2025-12-22' → '12-22周'
    """
    y, m, d = study_week_str.split("-")
    return f"{int(m):02d}-{int(d):02d}周"


def week_sort_key(label: str) -> tuple:
    """
    排序键：看板中月份 >= 10 属于2025年，其余属于2026年。
    倒序排列时较新的周排前面。
    """
    m, d = label.replace("周", "").split("-")
    m, d = int(m), int(d)
    year = 2025 if m >= 10 else 2026
    return (year, m, d)


TYPES = ["AI顾问", "真人顾问"]


def process(rows: list) -> tuple:
    """
    处理原始 CSV 行，生成看板所需的 WEEKS、MAIN_DATA、CHANNEL_DATA。

    返回值：
      weeks        : list[str]  周标签列表（倒序，最新在前）
      main_data    : dict       整体数据 {week: {type: counts}}
      channel_data : dict       渠道数据 {channel: {week: {type: counts}}}
    """
    # 分组容器
    main_raw: dict    = defaultdict(lambda: defaultdict(list))
    channel_raw: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for row in rows:
        ldap   = str(row.get("顾问LDAP", "") or "").strip()
        ctype  = classify_consultant(ldap)
        if ctype is None:
            continue

        sw = str(row.get("学习周", "") or "").strip()
        if not sw or sw == "unset":
            continue

        wlabel = week_label(sw)
        main_raw[wlabel][ctype].append(row)

        ch = classify_channel(row)
        if ch:
            channel_raw[ch][wlabel][ctype].append(row)

    # 周列表：倒序（最新在前）
    weeks = sorted(main_raw.keys(), key=week_sort_key, reverse=True)

    # 构建 MAIN_DATA
    main_data: dict = {}
    for wlabel in weeks:
        main_data[wlabel] = {}
        for t in TYPES:
            r = main_raw[wlabel].get(t, [])
            main_data[wlabel][t] = aggregate(r) if r else dict(ZERO_COUNTS)

    # 构建 CHANNEL_DATA（确保所有渠道都包含所有周，缺失填零）
    channel_data: dict = {}
    for ch in CHANNEL_IDS:
        channel_data[ch] = {}
        for wlabel in weeks:
            channel_data[ch][wlabel] = {}
            for t in TYPES:
                r = channel_raw[ch][wlabel].get(t, [])
                channel_data[ch][wlabel][t] = aggregate(r) if r else dict(ZERO_COUNTS)

    return weeks, main_data, channel_data


# ═══════════════════════════════ 更新 HTML ═══════════════════════════

def update_html(weeks: list, main_data: dict, channel_data: dict, yesterday: str) -> None:
    """
    就地更新 dashboard.html 中的数据部分：
      1. WEEKS 数组
      2. MAIN_DATA 对象
      3. CHANNEL_DATA 对象
      4. 头部"最后更新"日期
      5. TODAY 常量（确保为动态获取）
    """
    html = DASHBOARD.read_text(encoding="utf-8")

    weeks_js = json.dumps(weeks, ensure_ascii=False)
    main_js  = json.dumps(main_data,    ensure_ascii=False, separators=(",", ":"))
    ch_js    = json.dumps(channel_data, ensure_ascii=False, separators=(",", ":"))

    # 1. 更新 WEEKS 数组（单行）
    html = re.sub(r"const WEEKS = \[.*\];", f"const WEEKS = {weeks_js};", html)

    # 2. 更新 MAIN_DATA（单行 JSON）
    html = re.sub(r"const MAIN_DATA = \{.*\};", f"const MAIN_DATA = {main_js};", html)

    # 3. 更新 CHANNEL_DATA（单行 JSON）
    html = re.sub(r"const CHANNEL_DATA = \{.*\};", f"const CHANNEL_DATA = {ch_js};", html)

    # 4. 更新头部"最后更新"日期
    html = re.sub(r"最后更新：[\d-]+", f"最后更新：{yesterday}", html)

    # 5. 确保 TODAY 是动态的（兼容旧格式硬编码日期）
    html = re.sub(
        r"const TODAY = new Date\([^)]+\);[^\n]*",
        "const TODAY = new Date(); // 动态当前日期",
        html,
    )

    DASHBOARD.write_text(html, encoding="utf-8")
    log.info(f"Dashboard saved to {DASHBOARD}")


# ═══════════════════════════════ GitHub 推送 ═══════════════════════════

def git_push() -> None:
    """
    将更新后的 dashboard.html 提交并推送至 GitHub，以触发 GitHub Pages 更新。
    推送失败时仅记录警告，不中断主流程。
    """
    repo_dir = DASHBOARD.parent
    today    = date.today().isoformat()
    try:
        subprocess.run(
            ["git", "add", "dashboard.html"],
            check=True, cwd=repo_dir, capture_output=True, text=True,
        )
        commit = subprocess.run(
            ["git", "commit", "-m", f"chore: auto-update dashboard {today}"],
            cwd=repo_dir, capture_output=True, text=True,
        )
        if commit.returncode == 0:
            subprocess.run(
                ["git", "push"],
                check=True, cwd=repo_dir, capture_output=True, text=True,
            )
            log.info("Dashboard pushed to GitHub Pages successfully")
        else:
            # commit 失败通常意味着无变更，不视为错误
            log.info(f"Git commit skipped: {(commit.stdout + commit.stderr).strip()}")
    except subprocess.CalledProcessError as e:
        log.warning(f"Git push failed (non-fatal): {e.stderr or e}")
    except FileNotFoundError:
        log.warning("Git not found in PATH, skipping push to GitHub")


# ═══════════════════════════════ 主流程 ═══════════════════════════════

def main():
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    log.info(f"{'='*60}")
    log.info(f"Dashboard refresh started. Data cutoff: {yesterday}")

    # ── Step 1: MCP 握手 ─────────────────────────────────────────────
    mcp = MCPClient(MCP_URL, MCP_TOKEN)
    log.info("Initializing MCP session...")
    mcp.initialize()
    log.info(f"MCP session ready (session_id={mcp.session_id})")

    # ── Step 2: 提交查询 ─────────────────────────────────────────────
    query_id = submit_query(mcp, yesterday)

    # ── Step 3: 等待查询完成 ─────────────────────────────────────────
    wait_query_done(mcp, query_id, max_wait=1800)

    # ── Step 4: 获取下载链接并下载 CSV ──────────────────────────────
    csv_url = get_csv_url(mcp, query_id)
    rows    = download_rows(csv_url)

    # ── Step 5: 数据处理 ─────────────────────────────────────────────
    log.info("Processing data...")
    weeks, main_data, channel_data = process(rows)
    log.info(f"Weeks detected ({len(weeks)}): {weeks}")

    ai_total = sum(v["AI顾问"]["n"]   for v in main_data.values())
    hu_total = sum(v["真人顾问"]["n"] for v in main_data.values())
    log.info(f"AI顾问 累计分配: {ai_total}，真人顾问 累计分配: {hu_total}")

    # ── Step 6: 更新 HTML ────────────────────────────────────────────
    update_html(weeks, main_data, channel_data, yesterday)

    # ── Step 7: 推送至 GitHub Pages ──────────────────────────────────
    git_push()

    log.info(f"Dashboard refresh completed successfully!")
    log.info(f"{'='*60}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception(f"Dashboard refresh FAILED: {e}")
        sys.exit(1)
