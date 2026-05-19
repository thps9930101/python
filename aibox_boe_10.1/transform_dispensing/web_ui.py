import json
import threading
import time
from datetime import datetime

from flask import Flask, jsonify, render_template_string, request
from werkzeug.serving import make_server


HTML_TEMPLATE = """
<!doctype html>
<html lang="zh-Hant">
<head>
    <meta charset="utf-8">
    <title>{{ title }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body {
            margin: 0;
            font-family: Arial, "Microsoft JhengHei", sans-serif;
            background: #0f172a;
            color: #e2e8f0;
        }
        .wrap {
            padding: 16px;
        }
        .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            margin-bottom: 16px;
            flex-wrap: wrap;
        }
        h1 {
            margin: 0;
            font-size: 24px;
        }
        .meta {
            font-size: 13px;
            color: #94a3b8;
        }
        .btns {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }
        button {
            background: #1d4ed8;
            border: none;
            color: white;
            padding: 8px 12px;
            border-radius: 8px;
            cursor: pointer;
        }
        button.secondary {
            background: #475569;
        }
        .tabs {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            margin-bottom: 12px;
        }
        .tab {
            background: #334155;
            color: #e2e8f0;
            border: none;
            padding: 8px 12px;
            border-radius: 999px;
            cursor: pointer;
        }
        .tab.active {
            background: #2563eb;
            color: #fff;
        }
        .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 16px;
        }
        .card {
            background: #111827;
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 14px;
        }
        .card h2 {
            margin: 0 0 12px 0;
            font-size: 18px;
        }
        .mono {
            font-family: Consolas, Menlo, monospace;
            white-space: pre-wrap;
            word-break: break-word;
            font-size: 13px;
            line-height: 1.5;
        }
        #logs {
            height: 560px;
            overflow: auto;
            background: #020617;
            border-radius: 8px;
            padding: 10px;
        }
        #status {
            min-height: 560px;
            overflow: auto;
            background: #020617;
            border-radius: 8px;
            padding: 10px;
        }
        .pill {
            display: inline-block;
            background: #0f766e;
            border-radius: 999px;
            padding: 4px 10px;
            font-size: 12px;
        }
        @media (max-width: 960px) {
            .grid {
                grid-template-columns: 1fr;
            }
            #logs, #status {
                height: 360px;
                min-height: 360px;
            }
        }
    </style>
</head>
<body>
<div class="wrap">
    <div class="topbar">
        <div>
            <h1>{{ title }}</h1>
            <div class="meta" id="meta-line">loading...</div>
        </div>
        <div class="btns">
            <button class="secondary" onclick="clearLogs()">清理目前 Web Log</button>
        </div>
    </div>

    <div class="tabs">
        <button class="tab active" data-source="ALL" onclick="setSource('ALL', this)">ALL</button>
        <button class="tab" data-source="SYSTEM" onclick="setSource('SYSTEM', this)">SYSTEM</button>
        <button class="tab" data-source="DISPATCHER" onclick="setSource('DISPATCHER', this)">DISPATCHER</button>
        <button class="tab" data-source="WORKER" onclick="setSource('WORKER', this)">WORKER</button>
        <button class="tab" data-source="FFMPEG" onclick="setSource('FFMPEG', this)">FFMPEG</button>
        <button class="tab" data-source="LARAVEL" onclick="setSource('LARAVEL', this)">LARAVEL</button>
        <button class="tab" data-source="OTHER" onclick="setSource('OTHER', this)">OTHER</button>
    </div>

    <div class="grid">
        <div class="card">
            <h2>目前狀態</h2>
            <div id="status" class="mono">loading...</div>
        </div>

        <div class="card">
            <h2>分類 Log</h2>
            <div class="pill" id="source-pill">ALL</div>
            <div id="logs" class="mono">loading...</div>
        </div>
    </div>
</div>

<script>
let currentSource = "ALL";
let currentData = null;

function setSource(source, el) {
    currentSource = source;
    document.querySelectorAll(".tab").forEach(btn => btn.classList.remove("active"));
    el.classList.add("active");
    renderLogs();
}

function renderLogs() {
    if (!currentData) return;

    let logs = [];
    if (currentSource === "ALL") {
        logs = currentData.logs.all_logs || [];
    } else {
        const grouped = currentData.logs.logs_by_source || {};
        logs = grouped[currentSource] || [];
    }

    document.getElementById("source-pill").textContent = currentSource;
    document.getElementById("logs").textContent = logs.join("\\n");
}

async function refreshState() {
    try {
        const res = await fetch('/api/state');
        const data = await res.json();
        currentData = data;

        document.getElementById('status').textContent =
            JSON.stringify(data.status, null, 2);

        document.getElementById('meta-line').textContent =
            '更新時間: ' + (data.updated_at || '-') +
            ' ｜ Log 檔案: ' + (data.logs.log_file_path || '-');

        renderLogs();
    } catch (err) {
        document.getElementById('status').textContent = '讀取失敗: ' + err;
    }
}

async function clearLogs() {
    try {
        await fetch('/api/clear-logs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        await refreshState();
    } catch (err) {
        alert('清理失敗: ' + err);
    }
}

refreshState();
setInterval(refreshState, 1500);
</script>
</body>
</html>
"""


class DashboardState:
    def __init__(self, title: str):
        self.title = title
        self._lock = threading.Lock()
        self._status = {}
        self._updated_at = None

    def set_status(self, data: dict):
        with self._lock:
            self._status = data
            self._updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def snapshot(self):
        with self._lock:
            return {
                "title": self.title,
                "status": self._status,
                "updated_at": self._updated_at,
            }


def create_dashboard_app(dashboard_state: DashboardState, log_manager):
    app = Flask(__name__)

    @app.route("/")
    def index():
        return render_template_string(
            HTML_TEMPLATE,
            title=dashboard_state.title,
        )

    @app.route("/api/state")
    def api_state():
        state = dashboard_state.snapshot()
        return jsonify({
            "status": state["status"],
            "updated_at": state["updated_at"],
            "logs": log_manager.snapshot(),
        })

    @app.route("/api/clear-logs", methods=["POST"])
    def api_clear_logs():
        log_manager.clear_memory_logs()
        log_manager.log("SYSTEM", "Web UI clear logs clicked")
        return jsonify({
            "ok": True
        })

    return app


def start_dashboard_server(
    dashboard_state: DashboardState,
    log_manager,
    host: str = "0.0.0.0",
    port: int = 9100,
):
    app = create_dashboard_app(dashboard_state, log_manager)
    server = make_server(host, port, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def start_state_updater(
    dashboard_state: DashboardState,
    provider_func,
    interval_sec: float = 1.0,
    stop_event=None,
    log_manager=None,
):
    def _loop():
        while True:
            if stop_event is not None and stop_event.is_set():
                break

            try:
                dashboard_state.set_status(provider_func())
            except Exception as e:
                if log_manager is not None:
                    log_manager.log("SYSTEM", f"[UI updater error] {e}")

            time.sleep(interval_sec)

    thread = threading.Thread(target=_loop, daemon=True)
    thread.start()
    return thread