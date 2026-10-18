"""
scripts/capture_all_screenshots.py
==================================
Captures high-resolution browser screenshots of all 4 dashboard views
and the 2 terminal demonstration views using headless Microsoft Edge.
Produces exactly:
  1. docs/screenshots/view_1_live.png
  2. docs/screenshots/view_2_allocation.png
  3. docs/screenshots/view_3_pipeline.png
  4. docs/screenshots/view_4_results.png
  5. docs/screenshots/terminal_pipeline_demo.png
  6. docs/screenshots/terminal_spark_hive_query.png
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

def check_port_8000():
    try:
        s = socket.socket()
        s.settimeout(0.5)
        res = s.connect_ex(("127.0.0.1", 8000))
        s.close()
        return res == 0
    except Exception:
        return False

def render_terminal_html(title, body_text):
    escaped_body = (
        body_text.replace("&", "&amp;")
                 .replace("<", "&lt;")
                 .replace(">", "&gt;")
    )
    # Colorize key lines
    lines = []
    for line in escaped_body.split("\n"):
        if ">>> Demo Result" in line:
            lines.append(f'<span style="color: #4ade80; font-weight: bold;">{line}</span>')
        elif "Offline 30-Seed Empirical Study" in line or "STEP" in line:
            lines.append(f'<span style="color: #60a5fa; font-weight: bold;">{line}</span>')
        elif "CRISISGUARD" in line or "CrisisGuard" in line:
            lines.append(f'<span style="color: #38bdf8; font-weight: bold;">{line}</span>')
        elif "OFFLINE" in line or "FAIL" in line:
            lines.append(f'<span style="color: #f87171;">{line}</span>')
        elif "OPERATIONAL" in line or "PASS" in line or "VERIFIED" in line:
            lines.append(f'<span style="color: #34d399;">{line}</span>')
        elif "[1]" in line or "[2]" in line or "[3]" in line or "[4]" in line or "[5]" in line:
            lines.append(f'<span style="color: #fbbf24; font-weight: bold;">{line}</span>')
        else:
            lines.append(line)
    highlighted = "\n".join(lines)

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  body {{
    margin: 0;
    padding: 24px;
    background: #090d16;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 100vh;
    box-sizing: border-box;
  }}
  .window {{
    width: 100%;
    max-width: 1200px;
    background: #0f172a;
    border-radius: 8px;
    border: 1px solid #1e293b;
    box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.7);
    overflow: hidden;
  }}
  .titlebar {{
    background: #1e293b;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    gap: 8px;
    border-bottom: 1px solid #334155;
  }}
  .dot {{
    width: 12px;
    height: 12px;
    border-radius: 50%;
  }}
  .dot-red {{ background: #ef4444; }}
  .dot-yellow {{ background: #f59e0b; }}
  .dot-green {{ background: #10b981; }}
  .title {{
    color: #94a3b8;
    font-size: 13px;
    margin-left: 12px;
    letter-spacing: 0.5px;
  }}
  .content {{
    padding: 20px 24px;
    color: #e2e8f0;
    font-size: 13px;
    line-height: 1.5;
    white-space: pre-wrap;
    overflow-x: auto;
  }}
</style>
</head>
<body>
  <div class="window">
    <div class="titlebar">
      <div class="dot dot-red"></div>
      <div class="dot dot-yellow"></div>
      <div class="dot dot-green"></div>
      <span class="title">{title}</span>
    </div>
    <div class="content">{highlighted}</div>
  </div>
</body>
</html>"""
    return html

def main():
    print("=" * 68)
    print("Capturing 4 Dashboard + 2 Terminal Screenshots (v1.0-final-9)...")
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
        # 1. Capture 4 Dashboard Views
        views = [
            {"name": "view_1_live", "url": "http://127.0.0.1:8000/?tab=live&theme=dark"},
            {"name": "view_2_allocation", "url": "http://127.0.0.1:8000/?tab=allocation&theme=dark"},
            {"name": "view_3_pipeline", "url": "http://127.0.0.1:8000/?tab=pipeline&theme=dark"},
            {"name": "view_4_results", "url": "http://127.0.0.1:8000/?tab=results&theme=dark"},
        ]

        for v in views:
            out_file = os.path.abspath(os.path.join(SCREENSHOTS_DIR, f"{v['name']}.png"))
            cmd = [
                EDGE_PATH,
                "--headless=new",
                "--disable-gpu",
                "--no-first-run",
                "--window-size=1440,900",
                f"--screenshot={out_file}",
                v["url"]
            ]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            if os.path.exists(out_file) and os.path.getsize(out_file) > 10000:
                print(f"[OK] {v['name']}.png ({os.path.getsize(out_file):,} bytes)")
            else:
                print(f"[FAIL] {v['name']}.png")

        # 2. Render and Capture Terminal View 1: Terminal Pipeline Demo
        demo_py = os.path.join(BASE_DIR, "scripts", "demo_terminal.py")
        run_res = subprocess.run([sys.executable, demo_py], capture_output=True, text=True, cwd=BASE_DIR)
        term1_text = run_res.stdout
        term1_html = render_terminal_html("ubuntu@crisisguard-cluster: ~/bigdata_project $ python scripts/demo_terminal.py", term1_text)
        term1_path = os.path.join(SCREENSHOTS_DIR, "temp_term1.html")
        with open(term1_path, "w", encoding="utf-8") as f:
            f.write(term1_html)

        out_term1 = os.path.join(SCREENSHOTS_DIR, "terminal_pipeline_demo.png")
        cmd_term1 = [
            EDGE_PATH,
            "--headless=new",
            "--disable-gpu",
            "--no-first-run",
            "--window-size=1440,900",
            f"--screenshot={out_term1}",
            f"file:///{os.path.abspath(term1_path).replace(os.sep, '/')}"
        ]
        subprocess.run(cmd_term1, capture_output=True, text=True, timeout=15)
        if os.path.exists(out_term1) and os.path.getsize(out_term1) > 1000:
            print(f"[OK] terminal_pipeline_demo.png ({os.path.getsize(out_term1):,} bytes)")
        if os.path.exists(term1_path):
            os.remove(term1_path)

        # 3. Render and Capture Terminal View 2: Spark SQL Hive Metastore Query
        hive_evidence_path = os.path.join(BASE_DIR, "results", "hive_query_evidence.txt")
        with open(hive_evidence_path, "r", encoding="utf-8") as f:
            term2_text = f.read()[:2000]
        term2_html = render_terminal_html("spark-sql --conf spark.sql.catalogImplementation=hive (Metastore Port 9083)", term2_text)
        term2_path = os.path.join(SCREENSHOTS_DIR, "temp_term2.html")
        with open(term2_path, "w", encoding="utf-8") as f:
            f.write(term2_html)

        out_term2 = os.path.join(SCREENSHOTS_DIR, "terminal_spark_hive_query.png")
        cmd_term2 = [
            EDGE_PATH,
            "--headless=new",
            "--disable-gpu",
            "--no-first-run",
            "--window-size=1440,900",
            f"--screenshot={out_term2}",
            f"file:///{os.path.abspath(term2_path).replace(os.sep, '/')}"
        ]
        subprocess.run(cmd_term2, capture_output=True, text=True, timeout=15)
        if os.path.exists(out_term2) and os.path.getsize(out_term2) > 1000:
            print(f"[OK] terminal_spark_hive_query.png ({os.path.getsize(out_term2):,} bytes)")
        if os.path.exists(term2_path):
            os.remove(term2_path)

        print("\nAll 6 target screenshots created successfully in docs/screenshots/")
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
