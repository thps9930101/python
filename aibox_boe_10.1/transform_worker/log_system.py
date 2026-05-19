import os
import sys
import threading
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path


ALLOWED_SOURCES = {
    "SYSTEM",
    "DISPATCHER",
    "WORKER",
    "FFMPEG",
    "LARAVEL",
    "OTHER",
}


class CategoryLogManager:
    def __init__(self, app_name: str, log_dir: str = "logs", max_memory_logs: int = 3000):
        self.app_name = app_name
        self.max_memory_logs = max_memory_logs
        self._lock = threading.Lock()

        base_dir = Path(__file__).resolve().parent
        self.log_dir = base_dir / log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file_path = self.log_dir / f"{app_name}_{ts}.log"

        self._all_logs = deque(maxlen=max_memory_logs)
        self._logs_by_source = defaultdict(lambda: deque(maxlen=max_memory_logs))
        self._fh = open(self.log_file_path, "a", encoding="utf-8", buffering=1)

    def close(self):
        with self._lock:
            try:
                self._fh.close()
            except Exception:
                pass

    def _normalize_source(self, source: str) -> str:
        if not source:
            return "OTHER"

        source = str(source).strip().upper()
        if source not in ALLOWED_SOURCES:
            return "OTHER"
        return source

    def log(self, source: str, message):
        source = self._normalize_source(source)

        if message is None:
            return

        text = str(message).rstrip()
        if not text:
            return

        now = datetime.now()
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        lines = text.splitlines() or [""]

        with self._lock:
            for line in lines:
                formatted = f"[{ts}][{source}] {line}"
                self._all_logs.append(formatted)
                self._logs_by_source[source].append(formatted)
                self._fh.write(formatted + "\n")

    def snapshot(self):
        with self._lock:
            return {
                "log_file_path": str(self.log_file_path),
                "all_logs": list(self._all_logs),
                "logs_by_source": {
                    key: list(value)
                    for key, value in self._logs_by_source.items()
                },
            }

    def clear_memory_logs(self):
        with self._lock:
            self._all_logs.clear()
            self._logs_by_source.clear()


class ConsoleMirror:
    def __init__(self, original_stream, log_manager: CategoryLogManager, default_source: str):
        self.original_stream = original_stream
        self.log_manager = log_manager
        self.default_source = default_source
        self._buffer = ""

    def write(self, text):
        self.original_stream.write(text)
        self.original_stream.flush()

        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self.log_manager.log(self.default_source, line)

    def flush(self):
        self.original_stream.flush()

    def isatty(self):
        return getattr(self.original_stream, "isatty", lambda: False)()


def install_console_capture(log_manager: CategoryLogManager, default_source: str):
    sys.stdout = ConsoleMirror(sys.stdout, log_manager, default_source)
    sys.stderr = ConsoleMirror(sys.stderr, log_manager, default_source)