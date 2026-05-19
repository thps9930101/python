import json
import signal
import socket
import threading
import time
from collections import deque
from typing import Dict, Any, Optional, List
from log_system import CategoryLogManager, install_console_capture
from web_ui import DashboardState, start_dashboard_server, start_state_updater
import requests

from config import (
    PULL_API_URL,
    COMPLETE_API_URL,
    HEARTBEATAPI_URL,
    DISPATCHER_ID,
    HOST,
    PORT,
    HEARTBEAT_INTERVAL,
    HEARTBEAT_TIMEOUT,
    API_HEARTBEAT_INTERVAL,
    API_WORKER_INTERVAL,
)

lock = threading.Lock()
stop_event = threading.Event()

connected_clients: Dict[int, Dict[str, Any]] = {}
client_counter = 0

pending_jobs = deque()
pending_job_ids = set()

job_owner: Dict[int, int] = {}
pending_complete_reports = deque()

http = requests.Session()
server_socket = None

dispatcher_log = CategoryLogManager(app_name="dispatcher", log_dir="logs")
install_console_capture(dispatcher_log, default_source="DISPATCHER")

dispatcher_ui = DashboardState("Dispatcher Web UI")

def now_ts() -> float:
    return time.time()

# ------------web---------------
def build_dispatcher_status():
    with lock:
        clients = []
        for client_id, c in connected_clients.items():
            clients.append({
                "client_id": client_id,
                "worker_id": c.get("worker_id"),
                "ip": c.get("ip"),
                "is_vid": c.get("is_vid"),
                "status": c.get("status"),
                "max_parallel_jobs": c.get("max_parallel_jobs"),
                "running_jobs": list(c.get("running_jobs", {}).keys()),
                "last_seen_ago_sec": round(now_ts() - c.get("last_seen", now_ts()), 1),
            })

        return {
            "type": "dispatcher",
            "dispatcher_id": DISPATCHER_ID,
            "host": HOST,
            "port": PORT,
            "client_count": len(clients),
            "pending_jobs_count": len(pending_jobs),
            "pending_complete_reports_count": len(pending_complete_reports),
            "job_owner_count": len(job_owner),
            "clients": clients,
        }
# ------------web---------------

def json_dumps(obj: dict) -> str:
    return json.dumps(obj, ensure_ascii=False)


def send_json_line(conn: socket.socket, payload: dict) -> bool:
    try:
        raw = json_dumps(payload) + "\n"
        conn.sendall(raw.encode("utf-8"))
        return True
    except Exception as e:
        print(f"[Send FAIL] {e}")
        return False


def send_to_client(client_id: int, payload: dict) -> bool:
    with lock:
        client = connected_clients.get(client_id)

    if not client:
        print(f"[Send FAIL] client #{client_id} not found")
        return False

    ok = send_json_line(client["conn"], payload)
    if ok:
        print(f"[Send] #{client_id} --> {json_dumps(payload)}")
    return ok


def parse_is_vid(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, list):
        return True in value
    return False


def get_client_free_slots(client: dict) -> int:
    if not client.get("is_vid", False):
        return 0

    max_parallel_jobs = max(1, int(client.get("max_parallel_jobs", 1)))
    running_jobs = client.get("running_jobs", {})
    free_slots = max_parallel_jobs - len(running_jobs)
    return max(free_slots, 0)


def get_total_free_slots() -> int:
    with lock:
        total = 0
        for client in connected_clients.values():
            total += get_client_free_slots(client)
        return total


def append_pending_job(job: dict):
    job_id = int(job["id"])
    with lock:
        if job_id in pending_job_ids:
            return
        if job_id in job_owner:
            return
        pending_jobs.append(job)
        pending_job_ids.add(job_id)


def pop_next_pending_job() -> Optional[dict]:
    with lock:
        while pending_jobs:
            job = pending_jobs.popleft()
            job_id = int(job["id"])
            pending_job_ids.discard(job_id)

            if job_id in job_owner:
                continue

            return job
    return None


def requeue_job(job: dict):
    append_pending_job(job)


def enqueue_complete_report(report: dict):
    with lock:
        pending_complete_reports.append(report)


def set_client_job_assigned(client_id: int, job_id: int):
    with lock:
        client = connected_clients.get(client_id)
        if not client:
            return

        client["running_jobs"][job_id] = {
            "assigned_at": now_ts(),
            "started_at": None,
        }
        client["status"] = "processing"
        job_owner[job_id] = client_id


def mark_client_job_started(client_id: int, job_id: int):
    with lock:
        client = connected_clients.get(client_id)
        if not client:
            return

        if job_id not in client["running_jobs"]:
            client["running_jobs"][job_id] = {
                "assigned_at": now_ts(),
                "started_at": now_ts(),
            }
        else:
            client["running_jobs"][job_id]["started_at"] = now_ts()

        client["status"] = "processing"
        job_owner[job_id] = client_id


def sync_client_running_jobs_from_worker(client_id: int, running_job_ids: List[int]):
    with lock:
        client = connected_clients.get(client_id)
        if not client:
            return

        old_job_ids = set(client["running_jobs"].keys())
        new_job_ids = set(running_job_ids)

        # 刪掉 worker 不再聲稱自己在跑的 job
        for job_id in old_job_ids - new_job_ids:
            client["running_jobs"].pop(job_id, None)
            if job_owner.get(job_id) == client_id:
                job_owner.pop(job_id, None)

        # 補上 worker 目前仍在跑的 job
        for job_id in new_job_ids:
            if job_id not in client["running_jobs"]:
                client["running_jobs"][job_id] = {
                    "assigned_at": now_ts(),
                    "started_at": None,
                }
            job_owner[job_id] = client_id

        if len(client["running_jobs"]) > 0:
            client["status"] = "processing"
        else:
            client["status"] = "idle"


def clear_client_job(client_id: int, job_id: int):
    with lock:
        client = connected_clients.get(client_id)
        if client:
            client["running_jobs"].pop(job_id, None)
            if len(client["running_jobs"]) == 0:
                client["status"] = "idle"

        if job_owner.get(job_id) == client_id:
            job_owner.pop(job_id, None)


def cleanup_disconnected_client(client_id: int):
    with lock:
        client = connected_clients.get(client_id)
        if not client:
            return

        for job_id in list(client["running_jobs"].keys()):
            if job_owner.get(job_id) == client_id:
                job_owner.pop(job_id, None)

        try:
            client["conn"].close()
        except Exception:
            pass

        del connected_clients[client_id]


def close_all_clients():
    with lock:
        client_ids = list(connected_clients.keys())

    for client_id in client_ids:
        cleanup_disconnected_client(client_id)


def request_shutdown(signum=None, frame=None):
    if stop_event.is_set():
        return

    print("[DISPATCHER] shutdown requested")
    stop_event.set()

    global server_socket
    if server_socket:
        try:
            server_socket.close()
        except Exception:
            pass

    close_all_clients()


def handle_worker_message(client_id: int, parsed: dict):
    status = parsed.get("status")
    data = parsed.get("data", {}) or {}

    with lock:
        client = connected_clients.get(client_id)
        if not client:
            return

        client["last_seen"] = now_ts()

        worker_id = data.get("worker_id")
        if worker_id:
            client["worker_id"] = worker_id

        if "is_vid" in data:
            client["is_vid"] = parse_is_vid(data.get("is_vid"))

        if "max_parallel_jobs" in data:
            try:
                client["max_parallel_jobs"] = max(1, int(data["max_parallel_jobs"]))
            except Exception:
                client["max_parallel_jobs"] = 1

    if status in ("idle", "heartbeat"):
        running_job_ids = data.get("running_job_ids", [])
        if not isinstance(running_job_ids, list):
            running_job_ids = []

        try:
            running_job_ids = [int(x) for x in running_job_ids]
        except Exception:
            running_job_ids = []

        sync_client_running_jobs_from_worker(client_id, running_job_ids)

        send_to_client(client_id, {
            "status": "success",
            "data": {}
        })

    elif status == "processing":
        job_id = data.get("job_id")
        if job_id is not None:
            job_id = int(job_id)
            mark_client_job_started(client_id, job_id)

        send_to_client(client_id, {
            "status": "success",
            "data": {}
        })

    elif status == "done":
        job_id = data.get("job_id")
        if job_id is None:
            return

        job_id = int(job_id)
        duration_sec = data.get("duration_sec")
        worker_id = data.get("worker_id")
        per_frame_sec = data.get("per_frame_sec")
        download_sec = data.get("download_sec")
        upload_sec = data.get("upload_sec")

        clear_client_job(client_id, job_id)

        enqueue_complete_report({
            "id": job_id,
            "status": 1,
            "duration_sec": duration_sec,
            "error_message": None,
            "worker_id": worker_id,
            "per_frame_sec": per_frame_sec,
            "download_sec": download_sec,
            "upload_sec": upload_sec,
        })

        send_to_client(client_id, {
            "status": "success",
            "data": {}
        })

        distribute_jobs()

    elif status == "failed":
        job_id = data.get("job_id")
        if job_id is None:
            return

        job_id = int(job_id)
        duration_sec = data.get("duration_sec")
        error_message = data.get("error_message", "unknown error")
        worker_id = data.get("worker_id")
        download_sec = data.get("download_sec")
        upload_sec = data.get("upload_sec")

        clear_client_job(client_id, job_id)

        enqueue_complete_report({
            "id": job_id,
            "status": 2,
            "duration_sec": duration_sec,
            "error_message": error_message,
            "worker_id": worker_id,
            "per_frame_sec": None,
            "download_sec": download_sec,
            "upload_sec": upload_sec,
        })

        send_to_client(client_id, {
            "status": "success",
            "data": {}
        })

        distribute_jobs()

    else:
        print(f"[Recv] #{client_id} unknown status: {status}")


def handle_client(conn: socket.socket, addr):
    global client_counter

    ip = addr[0]

    with lock:
        client_counter += 1
        client_id = client_counter
        connected_clients[client_id] = {
            "ip": ip,
            "conn": conn,
            "last_seen": now_ts(),
            "worker_id": None,
            "is_vid": False,
            "status": "idle",
            "max_parallel_jobs": 1,
            "running_jobs": {},
        }

    print(f"[Connected] #{client_id} {ip}, Now: {len(connected_clients)} clients")

    buffer = b""

    try:
        while not stop_event.is_set():
            data = conn.recv(4096)
            if not data:
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
                    print(f"[JSON ERROR] #{client_id} {e} raw={line[:300]}")
                    continue

                print(f"[Recv] #{client_id} {ip} --> {json_dumps(parsed)}")
                handle_worker_message(client_id, parsed)

    except Exception as e:
        print(f"[Client Error] #{client_id} {ip} error={e}")

    finally:
        cleanup_disconnected_client(client_id)
        print(f"[Disconnected] #{client_id} {ip}, Now: {len(connected_clients)} clients")


def get_dispatchable_clients() -> List[int]:
    with lock:
        result = []
        for client_id, client in connected_clients.items():
            if client.get("is_vid", False) and get_client_free_slots(client) > 0:
                result.append(client_id)
        return result


def distribute_jobs():
    while not stop_event.is_set():
        candidate_clients = get_dispatchable_clients()
        if not candidate_clients:
            return

        assigned_any = False

        for client_id in candidate_clients:
            with lock:
                client = connected_clients.get(client_id)
                if not client:
                    continue
                free_slots = get_client_free_slots(client)

            for _ in range(free_slots):
                job = pop_next_pending_job()
                if not job:
                    return

                payload = {
                    "status": "task",
                    "message": [job]
                }

                ok = send_to_client(client_id, payload)
                if ok:
                    set_client_job_assigned(client_id, int(job["id"]))
                    assigned_any = True
                else:
                    requeue_job(job)
                    return

        if not assigned_any:
            return


def heartbeat_sender():
    while not stop_event.is_set():
        time.sleep(HEARTBEAT_INTERVAL)

        now = now_ts()
        remove_list = []

        with lock:
            for client_id, data in connected_clients.items():
                last_time = data["last_seen"]
                if now - last_time > HEARTBEAT_TIMEOUT:
                    remove_list.append(client_id)

        for client_id in remove_list:
            print(f"[Heartbeat Timeout] #{client_id} removed")
            cleanup_disconnected_client(client_id)


def api_pull_worker():
    while not stop_event.is_set():
        time.sleep(API_WORKER_INTERVAL)

        free_slots = get_total_free_slots()

        with lock:
            local_pending_count = len(pending_jobs)

        need_amount = free_slots - local_pending_count
        if need_amount <= 0:
            continue

        payload = {
            "msg": "pull",
            "data": {
                "amount": need_amount
            }
        }

        dispatcher_log.log("LARAVEL", f"[PULL REQUEST] payload={payload}")
        try:
            r = http.post(PULL_API_URL, json=payload, timeout=60)
            if not r.text:
                print("[PULL API] empty response")
                continue

            data = r.json()
            print(f"[PULL API] response = {data}")
            dispatcher_log.log("LARAVEL", f"[PULL RESPONSE] {data}")

            if data.get("msg") != "success":
                print("[PULL API] msg != success")
                continue

            jobs = data.get("message", []) or []
            if not isinstance(jobs, list):
                print("[PULL API] message is not list")
                continue

            for job in jobs:
                append_pending_job(job)

            distribute_jobs()

        except Exception as e:
            print(f"[PULL API ERROR] {e}")
            dispatcher_log.log("LARAVEL", f"[PULL ERROR] {e}")


def api_complete_worker():
    while not stop_event.is_set():
        time.sleep(1)

        report = None
        with lock:
            if pending_complete_reports:
                report = pending_complete_reports.popleft()

        if not report:
            continue

        payload = {
            "msg": "complete",
            "data": report
        }
        dispatcher_log.log("LARAVEL", f"[COMPLETE REQUEST] payload={payload}")

        try:
            r = http.post(COMPLETE_API_URL, json=payload, timeout=60)
            if not r.text:
                raise RuntimeError("empty response")

            data = r.json()
            print(f"[COMPLETE API] response = {data}")
            dispatcher_log.log("LARAVEL", f"[COMPLETE RESPONSE] {data}")

            if data.get("msg") != "success":
                raise RuntimeError(f"complete api msg != success: {data}")

        except Exception as e:
            print(f"[COMPLETE API ERROR] {e}, requeue report={report}")
            dispatcher_log.log("LARAVEL", f"[COMPLETE ERROR] {e}")
            enqueue_complete_report(report)
            time.sleep(3)


def api_heartbeat_sender():
    if not HEARTBEATAPI_URL:
        return

    while not stop_event.is_set():
        try:
            response = http.post(HEARTBEATAPI_URL, timeout=10)
            print(f"[API Heartbeat] status={response.status_code}")
        except Exception as e:
            print(f"[API Heartbeat Error] {e}")

        time.sleep(API_HEARTBEAT_INTERVAL)


def monitor_worker():
    while not stop_event.is_set():
        time.sleep(5)

        with lock:
            client_summary = []
            for client_id, c in connected_clients.items():
                client_summary.append({
                    "client_id": client_id,
                    "worker_id": c.get("worker_id"),
                    "ip": c.get("ip"),
                    "is_vid": c.get("is_vid"),
                    "status": c.get("status"),
                    "max_parallel_jobs": c.get("max_parallel_jobs"),
                    "running_jobs": list(c.get("running_jobs", {}).keys()),
                    "last_seen_ago_sec": round(now_ts() - c.get("last_seen", now_ts()), 1),
                })

            pending_count = len(pending_jobs)
            report_count = len(pending_complete_reports)

        print("========== DISPATCHER STATUS ==========")
        print(f"clients={len(client_summary)} pending_jobs={pending_count} pending_reports={report_count}")
        for item in client_summary:
            print(item)
        print("=======================================")


def start_server(host=HOST, port=PORT):
    global server_socket

    signal.signal(signal.SIGINT, request_shutdown)
    try:
        signal.signal(signal.SIGTERM, request_shutdown)
    except Exception:
        pass

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((host, port))
    server_socket.listen(50)
    server_socket.settimeout(1.0)

    print(f"[DISPATCHER] Listening at {host}:{port}")

    start_dashboard_server(
        dispatcher_ui,
        dispatcher_log,
        host="0.0.0.0",
        port=9100,
    )

    start_state_updater(
        dispatcher_ui,
        build_dispatcher_status,
        interval_sec=1.0,
        stop_event=stop_event,
        log_manager=dispatcher_log,
    )

    dispatcher_log.log("SYSTEM", "Dispatcher Web UI started at http://127.0.0.1:9100")

    threading.Thread(target=heartbeat_sender, daemon=True).start()
    threading.Thread(target=api_pull_worker, daemon=True).start()
    threading.Thread(target=api_complete_worker, daemon=True).start()
    threading.Thread(target=api_heartbeat_sender, daemon=True).start()
    threading.Thread(target=monitor_worker, daemon=True).start()

    try:
        while not stop_event.is_set():
            try:
                conn, addr = server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                if stop_event.is_set():
                    break
                raise

            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
    finally:
        request_shutdown()
        print("[DISPATCHER] stopped")


if __name__ == "__main__":
    start_server()