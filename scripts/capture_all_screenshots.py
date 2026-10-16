"""
scripts/capture_all_screenshots.py
==================================
Captures high-resolution browser screenshots of all five dashboard pages
across dark mode, light mode, and mobile viewport using headless Microsoft Edge.
"""

import os
import sys
import time
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

def main():
    print("=" * 68)
    print("Capturing 5-Page Dashboard Screenshots via Headless Edge...")
    print("=" * 68)

    for item in PAGES:
        out_path = os.path.join(SCREENSHOTS_DIR, f"{item['name']}.png")
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
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
            if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
                print(f"[OK] {item['name']}.png ({os.path.getsize(out_path):,} bytes)")
            else:
                print(f"[FAIL] {item['name']}.png: File missing or too small")
        except Exception as e:
            print(f"[ERROR] {item['name']}: {e}")

    print("\nScreenshots saved to docs/screenshots/")

if __name__ == "__main__":
    main()
