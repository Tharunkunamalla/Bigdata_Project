"""
tests/test_ui_cleanliness.py
============================
Verification test ensuring:
1. Non-disaster sources (HC3, MAGE, LIAR) never appear in the feed or sample posts.
2. No hard-coded metric constants ("37 TP", "2,420 events/s", "Stream AUC 0.9183") in UI components.
3. Feed verdict strictly depends on model probability and operating threshold, never on ground truth.
"""

import os
import sys
import re
import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from backend.main import get_sample_posts, format_post_payload, ALLOWED_DISASTER_SOURCES

def test_no_non_disaster_sources_in_feed():
    """Verify that general-domain corpora (HC3, MAGE, LIAR) never appear in sample-posts."""
    for reg in ["houston", "california", "nepal", "miami"]:
        res = get_sample_posts(region=reg, limit=50, hoax_rate=0.20)
        posts = res["posts"]
        for p in posts:
            source = str(p.get("source", ""))
            assert source not in ["HC3_Benchmark", "MAGE_Benchmark", "LIAR_FactChecking"], \
                f"Non-disaster general corpus source '{source}' leaked into {reg} feed!"

def test_no_hardcoded_numbers_in_frontend():
    """Verify that obsolete hard-coded metric constants are completely removed from UI code."""
    frontend_src = os.path.join(BASE_DIR, "frontend", "src")
    banned_literals = [
        r"37\s*TP",
        r"Stream\s*AUC\s*0\.9183",
        r"2,420\s*events/s\s*peak",
        r"2420\s*events/s",
        r"Operations Console",
        r"Distributed Big Data Infrastructure"
    ]

    for root, _, files in os.walk(frontend_src):
        for f in files:
            if f.endswith((".jsx", ".js", ".tsx", ".ts")):
                fpath = os.path.join(root, f)
                with open(fpath, "r", encoding="utf-8") as fh:
                    content = fh.read()
                for pat in banned_literals:
                    match = re.search(pat, content, re.IGNORECASE)
                    assert not match, f"Found banned literal pattern '{pat}' in {os.path.relpath(fpath, BASE_DIR)}"

def test_feed_verdict_rule_independent_of_ground_truth():
    """Verify verdict uses only model score and validation threshold (0.1786)."""
    # 1. Low score hoax -> must not be flagged
    p1 = format_post_payload(
        {"text": "Flood alert water rising", "is_synthetic": 1, "is_hoax": 1, "prob_synthetic_score": 0.05},
        1, "houston", 29.76, -95.36, "10:00:00"
    )
    assert p1["is_flagged"] is False
    assert p1["is_flagged_misinformation"] == 0

    # 2. High score authentic civilian report -> must be flagged
    p2 = format_post_payload(
        {"text": "Urgent boat needed street drowned", "is_synthetic": 0, "is_hoax": 0, "prob_synthetic_score": 0.75},
        2, "houston", 29.76, -95.36, "10:00:01"
    )
    assert p2["is_flagged"] is True
    assert p2["is_flagged_misinformation"] == 1
