import configparser
from pathlib import Path
import sys
import uuid


def _get_base_dir() -> Path:
    # 支援 python / exe
    if getattr(sys, "frozen", False):
        return Path(sys.executable).parent
    return Path(__file__).parent


def _get_config() -> configparser.ConfigParser:
    base_dir = _get_base_dir()
    cfg_path = base_dir / "config.ini"

    cfg = configparser.ConfigParser()
    read_files = cfg.read(cfg_path, encoding="utf-8")

    if not read_files:
        raise FileNotFoundError(f"找不到設定檔: {cfg_path}")

    return cfg


def _get_str(cfg, section: str, key: str, fallback: str = "") -> str:
    return cfg.get(section, key, fallback=fallback).strip()


def _get_int(cfg, section: str, key: str, fallback: int = 0) -> int:
    return cfg.getint(section, key, fallback=fallback)


cfg = _get_config()

# =========================
# Worker -> Dispatcher
# =========================
DISPATCHER_HOST = _get_str(cfg, "worker", "dispatcher_host", fallback="127.0.0.1")
DISPATCHER_PORT = _get_int(cfg, "worker", "dispatcher_port", fallback=9000)

# =========================
# Worker identity
# =========================
_worker_id = _get_str(cfg, "worker", "worker_id", fallback="")
if _worker_id:
    WORKER_ID = _worker_id
else:
    WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"

# =========================
# Worker capability
# =========================
MAX_PARALLEL_JOBS = _get_int(cfg, "worker", "max_parallel_jobs", fallback=2)
WORKER_HEARTBEAT_INTERVAL = _get_int(cfg, "worker", "worker_heartbeat_interval", fallback=10)

# =========================
# Timeout
# =========================
DOWNLOAD_TIMEOUT = _get_int(cfg, "worker", "download_timeout", fallback=600)
UPLOAD_TIMEOUT = _get_int(cfg, "worker", "upload_timeout", fallback=600)
CONNECT_TIMEOUT = _get_int(cfg, "worker", "connect_timeout", fallback=10)
RECONNECT_INTERVAL = _get_int(cfg, "worker", "reconnect_interval", fallback=5)

# =========================
# FFmpeg
# =========================
FFMPEG_PATH = _get_str(cfg, "worker", "ffmpeg_path", fallback="ffmpeg")

# =========================
# Upload mode
# put_binary: 直接 PUT 二進位到 upload_url
# post_file : multipart/form-data POST
# =========================
UPLOAD_MODE = _get_str(cfg, "worker", "upload_mode", fallback="put_binary")
UPLOAD_FIELD = _get_str(cfg, "worker", "upload_field", fallback="file")
