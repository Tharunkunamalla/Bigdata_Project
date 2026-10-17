"""
scripts/capture_all_screenshots.py
==================================
Captures high-resolution browser screenshots of all five dashboard pages
across dark mode, light mode, and mobile viewport using headless Microsoft Edge.
Automatically starts the backend server on port 8000 if not already running.
"""

import os
import sys
import time
import socket
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCREENSHOTS_DIR = os.path.join(BASE_DIR, "docs", "screenshots")
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

EDGE_PATH = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
if not os.path.exists(EDGE_PATH):
    EDGE_PATH = r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"

PAGES = [
    {"name": "01_live_dark", "url": "http://127.0.0.1:8000/?tab=live&theme=dark", "size": "1440,900"},
    {"name": "01_live_light", "url": "http://127.0.0.1:8000/?tab=live&theme=light", "size": "1440,900"},
    {"name": "01_live_mobile", "url": "http://127.0.0.1:8000/?tab=live&theme=dark", "size": "375,812"},
    {"name": "02_models_dark", "url": "http://127.0.0.1:8000/?tab=models&theme=dark", "size": "1440,900"},
    {"name": "02_models_light", "url": "http://127.0.0.1:8000/?tab=models&theme=light", "size": "1440,900"},
    {"name": "03_allocation_dark", "url": "http://127.0.0.1:8000/?tab=allocation&theme=dark", "size": "1440,900"},
    {"name": "03_allocation_light", "url": "http://127.0.0.1:8000/?tab=allocation&theme=light", "size": "1440,900"},
    {"name": "04_pipeline_dark", "url": "http://127.0.0.1:8000/?tab=pipeline&theme=dark", "size": "1440,900"},
    {"name": "04_pipeline_light", "url": "http://127.0.0.1:8000/?tab=pipeline&theme=light", "size": "1440,900"},
    {"name": "05_demo_dark", "url": "http://127.0.0.1:8000/?tab=demo&theme=dark", "size": "1440,900"},
    {"name": "05_demo_light", "url": "http://127.0.0.1:8000/?tab=demo&theme=light", "size": "1440,900"},
]

def check_port_8000():
    try:
        s = socket.socket()
        s.settimeout(0.5)
        res = s.connect_ex(("127.0.0.1", 8000))
        s.close()
        return res == 0
    except Exception:
        return False

def main():
    print("=" * 68)
    print("Capturing 5-Page Dashboard Screenshots via Headless Edge...")
    print("=" * 68)

    started_server = False
    server_proc = None
    if not check_port_8000():
        print("Starting FastAPI backend server on http://127.0.0.1:8000/...")
        py_exe = sys.executable
        server_proc = subprocess.Popen(
            [py_exe, "-m", "uvicorn", "backend.main:app", "--host", "127.0.0.1", "--port", "8000"],
            cwd=BASE_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        started_server = True
        for _ in range(30):
            if check_port_8000():
                print("Backend server is ready.")
                break
            time.sleep(0.5)

    try:
        for item in PAGES:
            out_path = os.path.abspath(os.path.join(SCREENSHOTS_DIR, f"{item['name']}.png"))
            cmd = [
                EDGE_PATH,
                "--headless=new",
                "--disable-gpu",
                "--no-first-run",
                f"--window-size={item['size']}",
                f"--screenshot={out_path}",
                item["url"]
            ]
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
                if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
                    print(f"[OK] {item['name']}.png ({os.path.getsize(out_path):,} bytes)")
                else:
                    print(f"[FAIL] {item['name']}.png: File missing or too small")
            except Exception as e:
                print(f"[ERROR] {item['name']}: {e}")

        print("\nAll 11 screenshots saved successfully to docs/screenshots/")
    finally:
        if started_server and server_proc:
            print("Stopping backend server...")
            server_proc.terminate()
            try:
                server_proc.wait(timeout=5)
            except Exception:
                server_proc.kill()

if __name__ == "__main__":
    main()
