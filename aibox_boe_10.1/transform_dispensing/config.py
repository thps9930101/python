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
# API
# =========================
PULL_API_URL = _get_str(cfg, "api", "pull_api_url")
COMPLETE_API_URL = _get_str(cfg, "api", "complete_api_url")
HEARTBEATAPI_URL = _get_str(cfg, "api", "heartbeatapi_url", fallback="")

# =========================
# Dispatcher / Server
# =========================
DISPATCHER_ID = _get_str(cfg, "dispatcher", "dispatcher_id", fallback="dispatcher-01")
HOST = _get_str(cfg, "dispatcher", "host", fallback="0.0.0.0")
PORT = _get_int(cfg, "dispatcher", "port", fallback=9000)

# =========================
# Dispatcher heartbeat / worker polling
# =========================
HEARTBEAT_INTERVAL = _get_int(cfg, "heartbeat", "heartbeat_interval", fallback=10)
HEARTBEAT_TIMEOUT = _get_int(cfg, "heartbeat", "heartbeat_timeout", fallback=120)
API_HEARTBEAT_INTERVAL = _get_int(cfg, "heartbeat", "api_heartbeat_interval", fallback=30)
API_WORKER_INTERVAL = _get_int(cfg, "heartbeat", "api_worker_interval", fallback=3)

# =========================
# Worker
# =========================
DISPATCHER_HOST = _get_str(cfg, "worker", "dispatcher_host", fallback="127.0.0.1")
DISPATCHER_PORT = _get_int(cfg, "worker", "dispatcher_port", fallback=9000)

_worker_id = _get_str(cfg, "worker", "worker_id", fallback="")
if _worker_id:
    WORKER_ID = _worker_id
else:
    WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"

MAX_PARALLEL_JOBS = _get_int(cfg, "worker", "max_parallel_jobs", fallback=2)
WORKER_HEARTBEAT_INTERVAL = _get_int(cfg, "worker", "worker_heartbeat_interval", fallback=10)

DOWNLOAD_TIMEOUT = _get_int(cfg, "worker", "download_timeout", fallback=600)
UPLOAD_TIMEOUT = _get_int(cfg, "worker", "upload_timeout", fallback=600)

FFMPEG_PATH = _get_str(cfg, "worker", "ffmpeg_path", fallback="ffmpeg")
UPLOAD_MODE = _get_str(cfg, "worker", "upload_mode", fallback="put_binary")
UPLOAD_FIELD = _get_str(cfg, "worker", "upload_field", fallback="file")