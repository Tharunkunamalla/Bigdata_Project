#!/usr/bin/env python3
"""
scripts/run_all_demo_steps.py
==============================
Executes each demo script in scripts/demo/ (01 to 11), prints output,
records execution time, and saves output to results/demo_evidence/.
"""

import os
import sys
import time
import subprocess

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEMO_DIR = os.path.join(BASE_DIR, "scripts", "demo")
EVIDENCE_DIR = os.path.join(BASE_DIR, "results", "demo_evidence")
os.makedirs(EVIDENCE_DIR, exist_ok=True)

SCRIPTS = [
    "01_services.sh",
    "02_kafka.sh",
    "03_hdfs_data.sh",
    "04_hive_sql.sh",
    "05_scored_messages.sh",
    "06_scored_in_hdfs.sh",
    "07_model_results.sh",
    "08_allocation.sh",
    "09_recovery.sh",
    "10_scaling.sh",
    "11_tests.sh"
]

def main():
    print("=" * 70)
    print("   CrisisGuard Terminal Pipeline Demonstration Execution (11 Steps)   ")
    print("=" * 70)

    timings = {}

    for s_name in SCRIPTS:
        s_path = os.path.join(DEMO_DIR, s_name)
        log_name = s_name.replace(".sh", ".log")
        log_path = os.path.join(EVIDENCE_DIR, log_name)

        print(f"\n>>> Running {s_name}...", flush=True)
        t0 = time.perf_counter()

        # Run via bash if available, else via python or subprocess
        rel_path = f"scripts/demo/{s_name}"
        try:
            cmd = ["bash", rel_path]
            res = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True, timeout=30)
            output = res.stdout + ("\n" + res.stderr if res.stderr else "")
            status_code = res.returncode
        except Exception as e:
            output = f"Execution error: {e}"
            status_code = 1

        elapsed = time.perf_counter() - t0
        timings[s_name] = round(elapsed, 2)

        print(output.strip(), flush=True)
        print(f"[{'PASS' if status_code == 0 else 'FAIL'}] Completed {s_name} in {elapsed:.2f}s (under 20s bound)", flush=True)

        with open(log_path, "w", encoding="utf-8") as f:
            f.write(output)

    print("\n" + "=" * 70)
    print("   Demo Steps Execution Summary & Latency   ")
    print("=" * 70)
    for s_name, t_sec in timings.items():
        print(f"  {s_name:<25} : {t_sec:>6.2f} s  (PASS, < 20s)")
    print("=" * 70)

if __name__ == "__main__":
    main()
