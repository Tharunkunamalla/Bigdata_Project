import subprocess
import time
import os

EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
OUT = os.path.abspath(r"docs\screenshots\dashboard_overview.png")

cmd = [
    EDGE,
    "--headless=new",
    "--disable-gpu",
    "--window-size=1600,1000",
    f"--screenshot={OUT}",
    "http://localhost:5173"
]

print("Running command:", " ".join(cmd))
res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
print("Return code:", res.returncode)
print("Stdout:", res.stdout)
print("Stderr:", res.stderr)
print("Exists:", os.path.exists(OUT), "Size:", os.path.getsize(OUT) if os.path.exists(OUT) else 0)
