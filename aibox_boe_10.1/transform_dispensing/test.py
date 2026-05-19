import socket
import time

HOST = "192.168.0.21"#59.120.6.64
PORT = 777

RECONNECT_DELAY = 5      # 斷線後幾秒重連
HEARTBEAT_INTERVAL = 10  # 心跳間隔（秒）


def connect():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(15)  # 連線 timeout
    sock.connect((HOST, PORT))
    sock.settimeout(None)  # 連線成功後改回 blocking
    print(f"[OK] Connected to {HOST}:{PORT}")
    return sock


def run():
    sock = None
    last_ping = 0

    while True:
        try:
            if sock is None:
                sock = connect()

            # ---- 心跳 ----
            now = time.time()
            if now - last_ping > HEARTBEAT_INTERVAL:
                sock.sendall(b"PING\n")
                last_ping = now

            # ---- 接收資料 ----
            data = sock.recv(4096)
            if not data:
                raise ConnectionError("server closed connection")

            print("[RECV]", data.decode(errors="ignore").strip())

        except Exception as e:
            print("[WARN] connection lost:", e)
            try:
                if sock:
                    sock.close()
            except:
                pass
            sock = None
            time.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    run()
