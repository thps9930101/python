import socket
import threading
import time
import requests
import json
import os
import mimetypes
from pathlib import Path
from EmailSender import EmailSender

from config import (
    API_URL,HEARTBEATAPI_URL,DEVICE_TYPE_ID,
    HOST, PORT,
    HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT,
    PROCESS_TIMEOUT,
    API_HEARTBEAT_INTERVAL, API_WORKER_INTERVAL,
    EMAIL_RECIPIENTS
)

connected_clients = {}   # { ip : last_heartbeat_time }
client_counter = 0
lock = threading.Lock()
data_count=0
process_start_time=None

sender = EmailSender(
        smtp_host="smtp.office365.com",
        smtp_port=587,
        username="no-reply1@lightmatrix3d.com",
        password="MeowMeow3d",
        use_tls=True,
    )

# =======================
#  Client Handler
# =======================
def handle_client(conn, addr):
    global client_counter
    global data_count
    global process_start_time
    
    ip = addr[0]

    # -------- 產生 client_id --------
    with lock:
        client_counter += 1
        data_count += 10
        client_id = client_counter
        connected_clients[client_id] = {
            "ip": ip,
            "conn": conn,
            "last_seen": time.time(),
            "is_vid":False,
            "is_img":False,
            "status":"idle"
        }

        print(f"[Connected] #{client_id} {ip}, Now: {len(connected_clients)} clients")

    # -------- 收資料 --------
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break

            msg = data.decode().strip()
            parsed = json.loads(msg) 
            status = parsed.get("status")

            #TODO::尚未測試<JACK說的新排序法>  這一行而已
            have_video = parsed.get("data", {}).get("is_vid", [])

            if status == "idle":
                #with lock:
                    if connected_clients[client_id]["status"]!="idle":
                        print("add")
                        connected_clients[client_id]["status"]="idle"

                        data_count = sum(
                                    1 for c in connected_clients.values()
                                    if c.get("status") == "idle"
                                    )*10
                        process_start_time=None
                        
                    #TODO::尚未測試<JACK說的新排序法>-------------
                    if isinstance(have_video, bool):
                        have_video = [have_video]

                    has_true = True in have_video
                    has_false = False in have_video

                    connected_clients[client_id]["is_vid"] = has_true
                    connected_clients[client_id]["is_img"] = has_false
                    #---------------------------

                    print(f"[Recv] #{client_id} {ip} --> {msg}")
                    print(f"data_count= {data_count}")
                    send_to_client(client_id, '{"status": "success", "data": {}} \n')
                    connected_clients[client_id]["last_seen"] = time.time()
            elif status =="processing":
                print(f"[Recv] #{client_id} {ip} --> {msg}")
                print(f"data_count= {data_count}")
                if connected_clients[client_id]["status"]=="idle":
                    process_start_time= time.time()
                    connected_clients[client_id]["status"]="processing"
                    data_count = sum(
                                    1 for c in connected_clients.values()
                                    if c.get("status") == "idle"
                                    )*10
            else:
                print(f"[Recv] #{client_id} {ip} --> {msg}")
                

    except Exception:
        pass

    # -------- 離線 --------
    finally:
        conn.close()
        with lock:
            if client_id in connected_clients:
                del connected_clients[client_id]
                client_counter-=1
        print(f"[Disconnected] #{client_id} {ip}, Now: {len(connected_clients)} clients")
        client_counter=len(connected_clients)
        data_count = sum(
            1 for c in connected_clients.values()
            if c.get("status") == "idle"
            )*10

# =======================
#  Client 個別傳送訊息  //send_to_client(3, "Hello Client #3 !")
# =======================
def send_to_client(client_id: int, message: str) -> bool:
    with lock:
        client = connected_clients.get(client_id)

    if not client:
        print(f"[Send FAIL] client #{client_id} not found")
        return False

    try:
        conn = client["conn"]
        conn.sendall(message.encode())
        print(f"[Send] #{client_id} --> {message}")
        return True
    except Exception as e:
        print(f"[Send FAIL] #{client_id} error: {e}")
        return False

# =======================
#  Server 心跳 Ping Thread
# =======================
def heartbeat_sender():
    global client_counter
    global process_start_time
    global data_count
    while True:
        if client_counter > 0 :
            time.sleep(HEARTBEAT_INTERVAL)

            with lock:
                now = time.time()
                remove_list = []

                for ip, data in connected_clients.items():
                    last_time = data["last_seen"]
                    status = data["status"]
                    if now - last_time > HEARTBEAT_TIMEOUT and status!="processing":
                        print(f"[Heartbeat Timeout] {ip} removed{now - last_time}")
                        send_email(message= "[Heartbeat Timeout] 錯誤 " )
                        remove_list.append(ip)

                    if process_start_time is not None:
                        if now -process_start_time>PROCESS_TIMEOUT:
                            send_email(message= "[PROCESS_TIMEOUT Error] 錯誤 " )
                            process_start_time=None

                for ip in remove_list:
                    del connected_clients[ip]
                    client_counter=len(connected_clients)
                    data_count = sum(
                            1 for c in connected_clients.values()
                            if c.get("status") == "idle"
                            )*10

# =======================
#  Data 分類 傳送
# =======================
def distribute_chunks_by_device(data, connected_clients, save_dir="unassigned"):
    global data_count

    """
    每個 client 一次拿一個 chunk：
    - chunk = 最多 10 張同 device_type_id 的圖片 OR 1 部影片
    - 剩餘 chunk 寫入 TXT
    - Print 保留完整 JSON 結構

    舊資料（TXT 內未分配的 chunk）優先分配，新的資料再分配
    未滿 10 張圖片的舊 chunk，優先用新資料補齊

    回傳:
        used_count: 本次分配資料筆數
        full_json: JSON dict，用於 send_to_client
    """
    Path(save_dir).mkdir(exist_ok=True)

    # =====================
    # Step 0: 讀取上次未分配 chunk
    # =====================
    leftover_chunks = []
    for txt_file in Path(save_dir).glob("*.txt"):
        with open(txt_file, "r", encoding="utf-8") as f:
            try:
                chunk = json.load(f)
                leftover_chunks.extend(chunk)
            except Exception as e:
                print(f"[warn] load {txt_file}: {e}")
        txt_file.unlink()

    # =====================
    # Step 1: 將資料分成舊資料與新資料
    # =====================
    old_data = leftover_chunks  # TXT 內舊資料
    new_data = data["message"]  # 新資料

    # =====================
    # Step 2: 重新排列並分組（保持原始資料不變）
    # =====================
    def sort_items(items):
        groups = {}
        for item in items:
            device_type = item["device_type_id"]
            mime, _ = mimetypes.guess_type(item["path"])
            sort_key = 2
            item["is_vid"]=False
            if mime:
                if mime.startswith("image"):
                    sort_key = 0
                    item["is_vid"]=False
                elif mime.startswith("video"):
                    sort_key = 1
                    item["is_vid"]=True
                

            if device_type not in groups:
                groups[device_type] = []
            groups[device_type].append((sort_key, item))

        sorted_items = []
        for device_type in sorted(groups.keys()):
            sorted_list = sorted(groups[device_type], key=lambda x: x[0])
            sorted_items.extend([x[1] for x in sorted_list])
        return sorted_items

    # Step 3: 分別排序舊資料與新資料
    sorted_old_items = sort_items(old_data)
    sorted_new_items = sort_items(new_data)

    # =====================
    # Step 4: 將舊資料未滿 10 張圖片的 chunk 補齊
    # =====================
    temp_image_chunks = []
    remaining_new_items = []
    used_new_indices = set()

    # 將舊資料按 device_type_id 分組
    old_chunks_by_device = {}
    for item in sorted_old_items:
        if mimetypes.guess_type(item["path"])[0].startswith("image"):
            dt = item["device_type_id"]
            old_chunks_by_device.setdefault(dt, []).append(item)
        else:
            temp_image_chunks.append([item])  # 影片單獨一組

    # 補齊舊圖片 chunk
    for dt, items in old_chunks_by_device.items():
        while items:
            chunk = items[:10]
            items = items[10:]
            if len(chunk) < 10:
                # 用新資料補齊
                for idx, new_item in enumerate(sorted_new_items):
                    if idx in used_new_indices:
                        continue
                    if new_item["device_type_id"] == dt:
                        mime, _ = mimetypes.guess_type(new_item["path"])
                        if mime and mime.startswith("image"):
                            chunk.append(new_item)
                            used_new_indices.add(idx)
                    if len(chunk) == 10:
                        break
            temp_image_chunks.append(chunk)

    # 剩餘新資料（未被補齊過的）
    for idx, item in enumerate(sorted_new_items):
        if idx not in used_new_indices:
            mime, _ = mimetypes.guess_type(item["path"])
            if mime and mime.startswith("video"):
                temp_image_chunks.append([item])  # 影片單獨一組
            else:
                # 分組圖片 chunk
                if not temp_image_chunks or mimetypes.guess_type(temp_image_chunks[-1][0]["path"])[0].startswith("video") or temp_image_chunks[-1][0]["device_type_id"] != item["device_type_id"] or len(temp_image_chunks[-1]) >= 10:
                    temp_image_chunks.append([item])
                else:
                    temp_image_chunks[-1].append(item)

    all_chunks = temp_image_chunks

    # =====================
    # Step 5: 分配給 client（每個 client 只拿一個 chunk）
    # =====================

    distributed = {}
    used_count = 0

    idle_clients = [
                client_id
                for client_id, info in connected_clients.items()
                if info.get("status") == "idle"
                ]

    #註解的是原先的版本
    """for i, client in enumerate(idle_clients):
        if i < len(all_chunks):
            distributed[client] = all_chunks[i]
            used_count += len(all_chunks[i])

            # 標記為 processing
            connected_clients[client]["status"] = "processing"
            data_count -= 10
        else:
            distributed[client] = [] """

    #TODO::尚未測試<JACK說的新排序法>=============================
    for i, client in enumerate(idle_clients):
        if i < len(all_chunks):

            chunk = all_chunks[i]

            first_item = chunk[0]
            path = first_item.get("path") or first_item.get("original", "")
            ext = os.path.splitext(path)[1].lower()
            is_video_chunk = ext in [".mp4", ".mov", ".avi", ".mkv"] #是影片 就=True

            # 能力不符就跳過，不分配
            if is_video_chunk and not connected_clients[client]["is_vid"]:
                continue
            elif not is_video_chunk and not connected_clients[client]["is_img"]:
                continue
            else:
                distributed[client] = chunk
                used_count += len(chunk)


            # 標記為 processing
            connected_clients[client]["status"] = "processing"
            data_count -= 10

        else:
            distributed[client] = []

    #========================================================
    
    # =====================
    # Step 6: 剩餘 chunk 寫回 TXT
    # =====================
    remaining_chunks = all_chunks[len(connected_clients):]
    for i, chunk in enumerate(remaining_chunks):
        txt_path = Path(save_dir) / f"unassigned_{i}.txt"
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(chunk, ensure_ascii=False, indent=4))

    # Step 7: Print 分配結果（完整 JSON 結構）
    print("\n=== Distribution Result ===")
    for client, chunk in distributed.items():
        if not chunk:
            print(f"{client}: No chunk assigned\n")
        else:
            print(f"{client}:")
            print(json.dumps(chunk, ensure_ascii=False, indent=4))
            send_to_client(client,str(json.dumps(chunk)+"\n"))
            print(f"Used count for {client}: {len(chunk)} data_count = {data_count}\n") 
    data_count = sum(
                    1 for c in connected_clients.values()
                    if c.get("status") == "idle"
                    )*10

# =======================
#  web API heartbeat Worker
# =======================
def api_heartbeat_sender():
    while True:
        
        try:
            response = requests.post(HEARTBEATAPI_URL, timeout=10)
            if response.status_code == 200:
                print("heartbeat ok")
            print(f"[API Heartbeat response.status_code] : {response.status_code}")
        except Exception as e:
            print("[API Heartbeat Error]", e)
            send_email(message= "[API Heartbeat Error] 錯誤 : "+str(e) )

        try:
            if client_counter > 0 and data_count>0 :
                has_txt = (
                    os.path.isdir("unassigned")
                    and any(name.lower().endswith(".txt") for name in os.listdir("unassigned"))
                )
                if has_txt:
                    data={}
                    distribute_chunks_by_device(data, connected_clients)

        except Exception as e:
            print("[Dispensing ERROR]", e)
        time.sleep(API_HEARTBEAT_INTERVAL)

# =======================
#  web API Worker
# =======================
def api_worker():
    while True:
        time.sleep(API_WORKER_INTERVAL)

        payload={
            "device_type_id":DEVICE_TYPE_ID,
            "amount":data_count
        }

        if client_counter > 0 and data_count>0 :
            try:
                r1 = requests.post(API_URL,json=payload, timeout=100)  # json=payload 會自動加上 Content-Type: application/json
                if r1.text=="":
                    print("[API] empty response")
                    return
                print("Raw text:", repr(r1.text))
                data = r1.json()

                print(f"[API Work] : {data}")
                print(f"data_count= {data_count}")
                #print(data["message"]["device_type_id"])
                distribute_chunks_by_device(data, connected_clients)

            except Exception as e:
                print("[API Work Error]", e)
                send_email(message= "[WebAPI Work Error] 錯誤 : "+str(e) )

# =======================
#  信件傳送
# =======================
def send_email(message: str, alert_type: str = "Error"):
    """
    寄送通知郵件，支援 API 錯誤或 Client 斷線。
    - message: 主要文字訊息
    - alert_type: "Error" 或 "Warning"
    """
    # 顏色對應
    colors = {
        "Error": "#ff4d4f",     # 紅色
        "Warning": "#faad14",   # 橘色
        "Info": "#1890ff"       # 藍色
    }
    color = colors.get(alert_type, "#1890ff")

    # HTML 郵件模板
    html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; line-height: 1.5;">
        <h2 style="color: {color};">[{alert_type}] 系統通知</h2>
        <p>{message}</p>
        <hr>
        <p style="font-size: 0.85em; color: #888;">
            這是自動發送通知，請勿直接回覆。
        </p>
    </body>
    </html>
    """

    ok = sender.send(
        subject=f"[{alert_type}] LightMatrix3D 系統通知",
        to_addrs=EMAIL_RECIPIENTS,
        body_text=message,  # 簡單文字版本
        body_html=html_content,
        attachments=None
    )

    print("Sent:", ok)

# =======================
#  Start Server
# =======================
def start_server(host=HOST, port=PORT):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(20)

    print(f"[SERVER] Listening at {host}:{port}")

    
    threading.Thread(target=heartbeat_sender, daemon=True).start()
    threading.Thread(target=api_worker, daemon=True).start()
    threading.Thread(target=api_heartbeat_sender, daemon=True).start()

    while True:
        conn, addr = server.accept()

        # 接到 client 後 → 啟動 handler thread
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()

if __name__ == "__main__":
    
    start_server()
