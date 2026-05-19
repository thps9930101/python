import json
import os
import queue
import socket
import subprocess
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import requests

from config import (
    DISPATCHER_HOST,
    DISPATCHER_PORT,
    WORKER_ID,
    MAX_PARALLEL_JOBS,
    WORKER_HEARTBEAT_INTERVAL,
    DOWNLOAD_TIMEOUT,
    UPLOAD_TIMEOUT,
    CONNECT_TIMEOUT,
    RECONNECT_INTERVAL,
    FFMPEG_PATH,
    UPLOAD_MODE,
    UPLOAD_FIELD,
)

from log_system import CategoryLogManager, install_console_capture
from web_ui import DashboardState, start_dashboard_server, start_state_updater


# =======================
# Logging / Web UI
# =======================
worker_log = CategoryLogManager(app_name="worker", log_dir="logs")
install_console_capture(worker_log, default_source="WORKER")

worker_ui = DashboardState("Worker Web UI")


# =======================
# Global
# =======================
app_stop_event = threading.Event()
conn_stop_event = threading.Event()

send_lock = threading.Lock()
state_lock = threading.Lock()
socket_lock = threading.Lock()

sock = None

task_queue = queue.Queue()
running_job_ids = set()

# 斷線時暫存要送給分配機的訊息
outbox = deque()

http = requests.Session()
executor = ThreadPoolExecutor(max_workers=MAX_PARALLEL_JOBS)


# =======================
# State helpers
# =======================
def has_connection() -> bool:
    with socket_lock:
        return sock is not None


def current_running_count() -> int:
    with state_lock:
        return len(running_job_ids)


def get_running_job_ids():
    with state_lock:
        return sorted(list(running_job_ids))


def add_running_job(job_id: int):
    with state_lock:
        running_job_ids.add(job_id)


def remove_running_job(job_id: int):
    with state_lock:
        running_job_ids.discard(job_id)


def set_socket(new_sock):
    global sock
    with socket_lock:
        sock = new_sock


def close_socket():
    global sock
    with socket_lock:
        s = sock
        sock = None

    if s:
        try:
            s.close()
        except Exception:
            pass


# =======================
# Web UI status provider
# =======================
def build_worker_status():
    return {
        "type": "worker",
        "worker_id": WORKER_ID,
        "dispatcher_host": DISPATCHER_HOST,
        "dispatcher_port": DISPATCHER_PORT,
        "connected": has_connection(),
        "max_parallel_jobs": MAX_PARALLEL_JOBS,
        "running_jobs_count": current_running_count(),
        "running_job_ids": get_running_job_ids(),
        "task_queue_size": task_queue.qsize(),
        "outbox_size": len(outbox),
    }


# =======================
# Socket send / queue
# =======================
def queue_payload(payload: dict):
    with send_lock:
        outbox.append(payload)


def send_json(payload: dict, queue_on_fail: bool = False) -> bool:
    with socket_lock:
        s = sock

    if s is None:
        if queue_on_fail:
            queue_payload(payload)
        return False

    try:
        raw = json.dumps(payload, ensure_ascii=False) + "\n"
        with send_lock:
            s.sendall(raw.encode("utf-8"))
        worker_log.log("WORKER", f"[SEND] {json.dumps(payload, ensure_ascii=False)}")
        return True
    except Exception as e:
        worker_log.log("SYSTEM", f"[SEND ERROR] {e}")
        if queue_on_fail:
            queue_payload(payload)
        conn_stop_event.set()
        return False


def flush_outbox():
    while True:
        with send_lock:
            if not outbox:
                return
            payload = outbox[0]

        ok = send_json(payload, queue_on_fail=False)
        if not ok:
            return

        with send_lock:
            if outbox and outbox[0] == payload:
                outbox.popleft()


# =======================
# Worker state report
# =======================
def build_state_payload():
    running_ids = get_running_job_ids()

    if len(running_ids) == 0:
        return {
            "status": "idle",
            "data": {
                "worker_id": WORKER_ID,
                "is_vid": [True],
                "max_parallel_jobs": MAX_PARALLEL_JOBS,
                "running_jobs": 0,
                "running_job_ids": [],
            }
        }

    return {
        "status": "heartbeat",
        "data": {
            "worker_id": WORKER_ID,
            "is_vid": [True],
            "max_parallel_jobs": MAX_PARALLEL_JOBS,
            "running_jobs": len(running_ids),
            "running_job_ids": running_ids,
        }
    }


def send_state_snapshot():
    payload = build_state_payload()
    send_json(payload, queue_on_fail=False)


def send_processing_status(job_id: int):
    payload = {
        "status": "processing",
        "data": {
            "worker_id": WORKER_ID,
            "job_id": int(job_id),
            "running_jobs": current_running_count(),
            "running_job_ids": get_running_job_ids(),
        }
    }
    send_json(payload, queue_on_fail=True)


def send_done_status(
    job_id: int,
    duration_sec: float,
    output_size_bytes: int,
    per_frame_sec,
    download_sec,
    upload_sec,
):
    payload = {
        "status": "done",
        "data": {
            "worker_id": WORKER_ID,
            "job_id": int(job_id),
            "duration_sec": round(duration_sec, 2),
            "output_size_bytes": int(output_size_bytes),
            "per_frame_sec": round(per_frame_sec, 4) if per_frame_sec is not None else None,
            "download_sec": round(download_sec, 4) if download_sec is not None else None,
            "upload_sec": round(upload_sec, 4) if upload_sec is not None else None,
        }
    }
    send_json(payload, queue_on_fail=True)


def send_failed_status(job_id: int, duration_sec: float, error_message: str, download_sec=None, upload_sec=None):
    payload = {
        "status": "failed",
        "data": {
            "worker_id": WORKER_ID,
            "job_id": int(job_id),
            "duration_sec": round(duration_sec, 2),
            "error_message": str(error_message)[:2000],
            "download_sec": round(download_sec, 4) if download_sec is not None else None,
            "upload_sec": round(upload_sec, 4) if upload_sec is not None else None,
        }
    }
    send_json(payload, queue_on_fail=True)


# =======================
# Dispatcher recv
# =======================
def recv_loop():
    buffer = b""

    while not conn_stop_event.is_set() and not app_stop_event.is_set():
        try:
            with socket_lock:
                s = sock

            if s is None:
                conn_stop_event.set()
                break

            data = s.recv(4096)
            if not data:
                worker_log.log("SYSTEM", "[SOCKET] dispatcher disconnected")
                conn_stop_event.set()
                break

            buffer += data

            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue

                try:
                    parsed = json.loads(line.decode("utf-8"))
                except Exception as e:
                    worker_log.log("WORKER", f"[RECV JSON ERROR] {e} raw={line[:300]}")
                    continue

                worker_log.log("WORKER", f"[RECV] {json.dumps(parsed, ensure_ascii=False)}")
                handle_dispatcher_message(parsed)

        except Exception as e:
            worker_log.log("SYSTEM", f"[RECV ERROR] {e}")
            conn_stop_event.set()
            break


def handle_dispatcher_message(parsed: dict):
    status = parsed.get("status")

    if status == "task":
        jobs = parsed.get("message", [])
        if not isinstance(jobs, list):
            return

        for job in jobs:
            if "id" not in job:
                continue

            job_id = int(job["id"])

            with state_lock:
                if job_id in running_job_ids:
                    continue

            task_queue.put(job)
            worker_log.log("WORKER", f"[TASK ENQUEUE] job_id={job_id}")

    elif status == "success":
        return

    else:
        worker_log.log("WORKER", f"[DISPATCHER] unknown status={status}")


def heartbeat_loop():
    while not conn_stop_event.is_set() and not app_stop_event.is_set():
        time.sleep(WORKER_HEARTBEAT_INTERVAL)

        if conn_stop_event.is_set() or app_stop_event.is_set():
            break

        payload = {
            "status": "heartbeat",
            "data": {
                "worker_id": WORKER_ID,
                "is_vid": [True],
                "max_parallel_jobs": MAX_PARALLEL_JOBS,
                "running_jobs": current_running_count(),
                "running_job_ids": get_running_job_ids(),
            }
        }
        send_json(payload, queue_on_fail=False)


# =======================
# File helpers
# =======================
def guess_input_suffix(job: dict) -> str:
    source_filename = job.get("source_filename")
    if source_filename:
        ext = Path(source_filename).suffix
        if ext:
            return ext

    source_url = job.get("source_url", "")
    try:
        parsed = urlparse(source_url)
        ext = Path(parsed.path).suffix
        if ext:
            return ext
    except Exception:
        pass

    return ".bin"


def download_file(url: str, output_path: str):
    with http.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT) as r:
        r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def upload_file(upload_url: str, file_path: str):
    if UPLOAD_MODE == "put_binary":
        with open(file_path, "rb") as f:
            headers = {
                "Content-Type": "video/mp4"
            }
            r = http.put(upload_url, data=f, headers=headers, timeout=UPLOAD_TIMEOUT)
            r.raise_for_status()
        return

    if UPLOAD_MODE == "post_file":
        with open(file_path, "rb") as f:
            files = {
                UPLOAD_FIELD: ("output.mp4", f, "video/mp4")
            }
            r = http.post(upload_url, files=files, timeout=UPLOAD_TIMEOUT)
            r.raise_for_status()
        return

    raise RuntimeError(f"Unsupported UPLOAD_MODE={UPLOAD_MODE}")


def probe_fps(input_path: str) -> float:
    try:
        ffprobe_path = FFMPEG_PATH.replace("ffmpeg", "ffprobe")
        command = [
            ffprobe_path,
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=r_frame_rate",
            "-of", "default=noprint_wrappers=1:nokey=1",
            input_path,
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            return 30.0

        text = result.stdout.strip()
        if "/" in text:
            a, b = text.split("/", 1)
            return float(a) / float(b)
        return float(text)
    except Exception:
        return 30.0


def convert_to_mp4(job: dict, input_path: str, output_path: str):
    output_width = int(job.get("output_width", 1200))
    output_height = int(job.get("output_height", 1920))

    vf_value = f"fps=30,scale={output_width}:{output_height}"

    command = [
        FFMPEG_PATH,
        "-y",
        "-hide_banner",
        "-loglevel", "error",
        "-i", input_path,
        "-vf", vf_value,
        "-c:v", "hevc_nvenc",
        "-b:v", "20M",
        "-maxrate", "20M",
        "-bufsize", "20M",
        "-preset", "fast",
        "-pix_fmt", "yuv420p",
        "-c:a", "copy",
        output_path,
    ]

    worker_log.log("FFMPEG", " ".join(command))

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        err = result.stderr or result.stdout or "ffmpeg failed"
        worker_log.log("FFMPEG", err[-4000:])
        raise RuntimeError(err[-4000:])

    worker_log.log("FFMPEG", f"[JOB {job.get('id')}] ffmpeg finished successfully")


# =======================
# Job process
# =======================
def process_one_job(job: dict):
    job_id = int(job["id"])
    started = time.time()

    download_started = None
    convert_started = None
    upload_started = None

    add_running_job(job_id)
    send_processing_status(job_id)

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            suffix = guess_input_suffix(job)
            input_path = str(Path(tmpdir) / f"input{suffix}")
            output_path = str(Path(tmpdir) / "output.mp4")

            source_url = job["source_url"]
            upload_url = job["upload_url"]

            worker_log.log("WORKER", f"[JOB {job_id}] downloading...")
            download_started = time.time()
            download_file(source_url, input_path)
            download_sec = time.time() - download_started

            fps = probe_fps(input_path)

            worker_log.log("WORKER", f"[JOB {job_id}] converting...")
            convert_started = time.time()
            convert_to_mp4(job, input_path, output_path)
            _convert_sec = time.time() - convert_started

            worker_log.log("WORKER", f"[JOB {job_id}] uploading...")
            upload_started = time.time()
            upload_file(upload_url, output_path)
            upload_sec = time.time() - upload_started

            duration_sec = time.time() - started
            output_size_bytes = os.path.getsize(output_path)

            per_frame_sec = None
            if fps > 0:
                per_frame_sec = 1.0 / fps

            worker_log.log(
                "WORKER",
                f"[JOB {job_id}] done duration={duration_sec:.2f}s "
                f"download={download_sec:.4f}s upload={upload_sec:.4f}s size={output_size_bytes}"
            )

            send_done_status(
                job_id=job_id,
                duration_sec=duration_sec,
                output_size_bytes=output_size_bytes,
                per_frame_sec=per_frame_sec,
                download_sec=download_sec,
                upload_sec=upload_sec,
            )

    except Exception as e:
        duration_sec = time.time() - started

        if download_started is None:
            download_sec = None
        elif convert_started is None:
            download_sec = time.time() - download_started
        else:
            download_sec = convert_started - download_started

        if upload_started is None:
            upload_sec = None
        else:
            upload_sec = time.time() - upload_started

        worker_log.log("WORKER", f"[JOB {job_id}] failed error={e}")

        send_failed_status(
            job_id=job_id,
            duration_sec=duration_sec,
            error_message=str(e),
            download_sec=download_sec,
            upload_sec=upload_sec,
        )

    finally:
        remove_running_job(job_id)
        send_state_snapshot()


# =======================
# Scheduler
# =======================
def scheduler_loop():
    while not app_stop_event.is_set():
        try:
            job = task_queue.get(timeout=1)
        except queue.Empty:
            continue

        executor.submit(process_one_job, job)


# =======================
# Reconnect
# =======================
def connect_dispatcher_forever():
    while not app_stop_event.is_set():
        try:
            worker_log.log("SYSTEM", f"[CONNECT] {DISPATCHER_HOST}:{DISPATCHER_PORT}")
            s = socket.create_connection(
                (DISPATCHER_HOST, DISPATCHER_PORT),
                timeout=CONNECT_TIMEOUT
            )
            s.settimeout(None)

            set_socket(s)
            conn_stop_event.clear()

            worker_log.log("SYSTEM", "[CONNECTED] dispatcher connected")

            # 先送目前狀態，告訴分配機目前仍在跑哪些 job
            send_state_snapshot()

            # 再補送斷線期間暫存的結果
            flush_outbox()

            recv_thread = threading.Thread(target=recv_loop, daemon=True)
            hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
            recv_thread.start()
            hb_thread.start()

            while not conn_stop_event.is_set() and not app_stop_event.is_set():
                time.sleep(1)

            worker_log.log("SYSTEM", "[DISCONNECTED] will reconnect")
            close_socket()

        except Exception as e:
            worker_log.log("SYSTEM", f"[CONNECT ERROR] {e}")

        if app_stop_event.is_set():
            break

        close_socket()
        worker_log.log("SYSTEM", f"[RECONNECT] after {RECONNECT_INTERVAL} sec")
        time.sleep(RECONNECT_INTERVAL)


# =======================
# Main
# =======================
def main():
    start_dashboard_server(
        worker_ui,
        worker_log,
        host="0.0.0.0",
        port=9101,
    )

    start_state_updater(
        worker_ui,
        build_worker_status,
        interval_sec=1.0,
        stop_event=app_stop_event,
        log_manager=worker_log,
    )

    worker_log.log("SYSTEM", "Worker Web UI started at http://127.0.0.1:9101")

    threading.Thread(target=scheduler_loop, daemon=True).start()

    try:
        connect_dispatcher_forever()
    except KeyboardInterrupt:
        worker_log.log("SYSTEM", "[EXIT] KeyboardInterrupt")
    finally:
        app_stop_event.set()
        conn_stop_event.set()
        close_socket()
        executor.shutdown(wait=False)
        worker_log.close()


if __name__ == "__main__":
    main()