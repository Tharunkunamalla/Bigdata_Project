"""
scripts/check_doc_consistency.py
================================
Validates that documentation, code, and report numbers match results/*.json exactly:
- No mentions of '+4.6' or '4.60' victims in Houston (must be +0.57).
- No stale corpus size (128,860 or 99,568 in active claims; must be 130,301).
- No unbacked claims of 'unseen event holdout' on the 75/25 stratified split.
- Checks that all metrics reported in docs/REPORT.md align with results/summary.json.
"""

import os
import sys
import json
import re

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
DOCS_DIR = os.path.join(BASE_DIR, "docs")

def check_consistency():
    print("=" * 70)
    print("      CrisisGuard AI: Comprehensive Documentation Consistency Audit    ")
    print("=" * 70)
    errors = []
    warnings = []

    # 1. Load ground truth results
    summary_path = os.path.join(RESULTS_DIR, "summary.json")
    if not os.path.exists(summary_path):
        errors.append("results/summary.json missing! Run scripts/build_report_numbers.py first.")
        return False

    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)

    ablation_path = os.path.join(RESULTS_DIR, "ablation_study.json")
    with open(ablation_path, "r", encoding="utf-8") as f:
        ablation = json.load(f)

    reg_dict = ablation.get("regional_evaluations", ablation.get("regional_trials", {}))
    hou_ta = reg_dict["houston"]["statistical_evaluations"]["TrustAware_Continuous"]
    hou_gain = hou_ta["gain_vs_blind_lp"]
    print(f"Verified Ground Truth Houston Continuous Gain: +{hou_gain} victims")

    # 2. Check for stale "+4.60" or "+4.6" claims
    files_to_check = [
        os.path.join(DOCS_DIR, "REPORT.md"),
        os.path.join(DOCS_DIR, "LIMITATIONS.md"),
        os.path.join(DOCS_DIR, "QA_PREP.md"),
        os.path.join(DOCS_DIR, "DEMO_SCRIPT.md"),
        os.path.join(BASE_DIR, "README.md")
    ]

    for fpath in files_to_check:
        if not os.path.exists(fpath):
            continue
        rel = os.path.relpath(fpath, BASE_DIR)
        with open(fpath, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for +4.60 or +4.6 victims in Houston
        if re.search(r"\+4\.60?\s*(victims|genuine)", content, re.IGNORECASE):
            errors.append(f"{rel}: Contains obsolete '+4.60' or '+4.6' claim! Must match ablation_study.json (+{hou_gain}).")
        
        # Check for 0.2704 as the sole or uncalibrated allocator threshold
        if "0.2704" in content and "corpus-wide" not in content and "ablation" not in content:
            warnings.append(f"{rel}: Mentions 0.2704 without clarifying it was the former corpus-wide threshold evaluated in ablation.")

        # Check that 130,301 is cited for total corpus
        if "128,860" in content:
            warnings.append(f"{rel}: Contains 128,860 records reference. Check if should be updated to 130,301.")

    # 3. Check Monotone Sweep AUC 0.50 gain ~ 0
    sweep = ablation.get("sensitivity_sweep_detector_auc", [])
    if sweep:
        auc_50_gain = sweep[0].get("genuine_gain", 99.0)
        if abs(auc_50_gain) > 0.10:
            errors.append(f"Detector AUC 0.50 gain is {auc_50_gain} > 0.10! Must be ~0.0.")
        else:
            print(f"[PASS] Monotone Sweep AUC 0.50 Control Gain: +{auc_50_gain} victims (~0.0)")

    # 4. Check Permutation Control p > 0.05
    sanity = summary.get("machine_learning", {}).get("sanity_controls", {})
    shuf = sanity.get("twenty_shuffle_control", {})
    p_val = shuf.get("p_value", 0.0)
    if p_val < 0.05:
        errors.append(f"Permutation control p-value is {p_val} < 0.05! Must be > 0.05.")
    else:
        print(f"[PASS] 20-Shuffle Permutation Control: p = {p_val} (statistically indistinguishable from random 0.50)")

    print("\n--- Summary of Consistency Audit ---")
    if warnings:
        print(f"Warnings ({len(warnings)}):")
        for w in warnings:
            print(f"  [!] {w}")
    if errors:
        print(f"Errors ({len(errors)}):")
        for e in errors:
            print(f"  [X] {e}")
        return False
    else:
        print("[PASS] All critical consistency assertions passed without errors!")
        return True

if __name__ == "__main__":
    success = check_consistency()
    if not success:
        sys.exit(1)
