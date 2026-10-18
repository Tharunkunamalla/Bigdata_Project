#!/usr/bin/env python3
"""
scripts/demo_stop.py
====================
Stops demo processes (FastAPI gateway, streaming scorer, replay simulator).
"""

import os
import sys
import subprocess

def main():
    print("Stopping CrisisGuard demo services...")
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/IM", "uvicorn.exe"], capture_output=True)
    else:
        subprocess.run(["pkill", "-f", "uvicorn"], capture_output=True)
        subprocess.run(["pkill", "-f", "spark_stream_scorer"], capture_output=True)
        subprocess.run(["pkill", "-f", "replay_simulator"], capture_output=True)
    print("Demo services stopped.")

if __name__ == "__main__":
    main()
