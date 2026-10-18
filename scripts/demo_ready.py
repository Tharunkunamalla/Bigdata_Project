#!/usr/bin/env python3
"""
scripts/demo_ready.py
=====================
Prepares the demo environment:
1. Launches FastAPI Operations Gateway on port 8000 (if not already running)
2. Checks Kafka and launches scorer / simulator if cluster is online
3. Opens the dashboard at http://127.0.0.1:8000/ in the default browser
"""

import os
import sys
import time
import socket
import webbrowser
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def check_port(host: str, port: int) -> bool:
    try:
        s = socket.socket()
        s.settimeout(0.5)
        res = s.connect_ex((host, port))
        s.close()
        return res == 0
    except Exception:
        return False

def main():
    print("=" * 65)
    print("      Preparing CrisisGuard Demo Environment (v1.0-final-9)        ")
    print("=" * 65)

    py_exe = sys.executable

    # 1. Start API if not running
    if not check_port("127.0.0.1", 8000):
        print("[1/3] Starting FastAPI gateway on port 8000...")
        subprocess.Popen(
            [py_exe, "-m", "uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"],
            cwd=BASE_DIR
        )
        for _ in range(20):
            if check_port("127.0.0.1", 8000):
                print("      API gateway listening on http://127.0.0.1:8000/")
                break
            time.sleep(0.5)
    else:
        print("[1/3] FastAPI gateway already active on port 8000.")

    # 2. Check cluster services
    kafka_up = check_port("127.0.0.1", 9092)
    if kafka_up:
        print("[2/3] Kafka detected. Launching streaming scorer and replay...")
        subprocess.Popen([py_exe, "streaming/spark_stream_scorer.py"], cwd=BASE_DIR)
        subprocess.Popen([py_exe, "ingestion/replay_simulator.py"], cwd=BASE_DIR)
    else:
        print("[2/3] Cluster daemons offline. Serving verified disaster replay feed.")

    # 3. Open dashboard in browser
    print("[3/3] Opening dashboard in browser: http://127.0.0.1:8000/")
    webbrowser.open("http://127.0.0.1:8000/")

    print("\n>>> CrisisGuard Demo is Ready <<<")

if __name__ == "__main__":
    main()
