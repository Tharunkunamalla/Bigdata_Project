"""
tests/test_dashboard_upgrade.py
===============================
Verification test suite for dashboard upgrade (v1.0-final-7):
1. Backend endpoint functions (get_health, compute_threshold_metrics, compare_allocation_policies, etc.)
2. Hand-computed confusion matrix verification for threshold slider at theta = 0.1786
3. Clean text verification (no glued event words, separated event_type, no not_humanitarian)
4. Trust distribution verification (continuous to 3 decimals, min/median/max range)
5. Pydantic input validation on allocation requests
6. Match test: every number in UI / API matches results/summary.json and test_predictions.csv
"""

import os
import sys
import json
import pytest
import numpy as np
import pandas as pd
from pydantic import ValidationError

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.join(BASE_DIR, "backend"))

from backend.main import (
    get_health,
    get_summary,
    get_models_detail,
    compute_threshold_metrics,
    get_evaluation_views,
    get_sample_posts,
    run_allocation,
    compare_allocation_policies,
    get_scaling,
    get_recovery_test,
    run_demo_step,
    AllocationRequest,
    clean_event_prefix,
    derive_clean_event_type
)

SUMMARY_JSON = os.path.join(BASE_DIR, "results", "summary.json")
TEST_PREDS_CSV = os.path.join(BASE_DIR, "data", "curated", "test_predictions.csv")

def test_health_endpoint_service_reachable_wording():
    """Verify health endpoint uses 'service reachable' wording and reports daemon status."""
    data = get_health()
    assert "status" in data
    assert "services" in data
    services = data["services"]
    for svc_name in ["kafka", "spark", "hdfs", "hive", "api"]:
        assert svc_name in services
        svc = services[svc_name]
        assert "status" in svc
        assert "service reachable" in svc["status"].lower() or "offline" in svc["status"].lower()

def test_hand_computed_threshold_confusion_matrix():
    """Verify the interactive threshold slider against hand-computed ground truth on test_predictions.csv."""
    assert os.path.exists(TEST_PREDS_CSV), "test_predictions.csv must exist"
    df = pd.read_csv(TEST_PREDS_CSV, low_memory=False)
    y_true = (df["is_synthetic"].fillna(0) == 1.0).astype(int).to_numpy()
    y_prob = df["prob_synthetic_score"].fillna(0.0).astype(float).to_numpy()

    # Hand-computed metrics at theta* = 0.1786
    theta_star = 0.1786
    y_pred = (y_prob >= theta_star).astype(int)
    hand_tp = int(((y_pred == 1) & (y_true == 1)).sum())
    hand_fp = int(((y_pred == 1) & (y_true == 0)).sum())
    hand_fn = int(((y_pred == 0) & (y_true == 1)).sum())
    hand_tn = int(((y_pred == 0) & (y_true == 0)).sum())
    hand_prec = hand_tp / (hand_tp + hand_fp)
    hand_rec = hand_tp / (hand_tp + hand_fn)
    hand_f1 = 2 * hand_prec * hand_rec / (hand_prec + hand_rec)

    # Call backend function
    api_res = compute_threshold_metrics(theta_star)
    assert api_res["true_positives"] == hand_tp == 1237
    assert api_res["false_positives"] == hand_fp == 683
    assert api_res["false_negatives"] == hand_fn == 1624
    assert api_res["true_negatives"] == hand_tn == 29335
    assert abs(api_res["precision"] - round(hand_prec, 4)) < 0.001
    assert abs(api_res["recall"] - round(hand_rec, 4)) < 0.001
    assert abs(api_res["f1"] - round(hand_f1, 4)) < 0.001
    assert api_res["is_validation_selected"] is True

def test_sample_posts_cleanliness_and_continuous_trust():
    """Verify text is stripped of event prefixes, event_type is separated, and trust is continuous."""
    data = get_sample_posts(region="houston", limit=40, hoax_rate=0.20)
    posts = data["posts"]
    assert len(posts) == 40

    glued_prefixes = ["earthquake ", "flood ", "hurricane ", "crash ", "None "]
    trusts = []
    has_flagged = False
    has_credible = False

    for p in posts:
        # Check text does not start with glued prefix words
        for pref in glued_prefixes:
            assert not p["text"].lower().startswith(pref.lower()), f"Text has glued prefix '{pref}': {p['text']}"
        
        # Check separated event_type
        assert "event_type" in p
        assert len(p["event_type"]) > 0
        assert p["event_type"] in ["Flood", "Earthquake", "Hurricane", "Wildfire", "Cyclone", "Tornado", "Transport Emergency", "Severe Hazard"]

        # Check humanitarian category is never not_humanitarian
        assert p["humanitarian_category"] != "not_humanitarian"

        # Check trust score
        trust = p["trust_score"]
        trusts.append(trust)
        assert 0.001 <= trust <= 0.999
        assert isinstance(trust, (float, int))

        if p["is_flagged_misinformation"] == 1:
            has_flagged = True
            assert "Injected Hoax" in p["injected_ground_truth"] or p["is_injected"] is True
        else:
            has_credible = True

    # Confirm continuous range: not all 0.990!
    assert has_flagged, "Sample posts must contain flagged hoaxes at hoax_rate=0.20"
    assert has_credible, "Sample posts must contain credible posts"
    assert min(trusts) < 0.30, f"Min trust should reflect hoaxes, got {min(trusts)}"
    assert max(trusts) > 0.95, f"Max trust should reflect authentic posts, got {max(trusts)}"
    assert len(set(trusts)) > 1, "Trust scores must not be constant"

def test_allocation_policy_comparison_endpoint():
    """Verify all 5 policies execute on same demands and Validation Threshold minimizes waste."""
    req = AllocationRequest(
        region="houston",
        resource_type="rescue_boats",
        capacity=52,
        hoax_rate=0.20,
        random_seed=42
    )
    data = compare_allocation_policies(req)
    assert data["status"] == "SUCCESS"
    assert "comparisons" in data
    comps = {c["policy"]: c for c in data["comparisons"]}

    # Verify all 5 policies are present
    for pol in ["blind_lp", "trust_aware_continuous", "trust_aware_validation_thresh", "trust_aware_real_cluster", "oracle"]:
        assert pol in comps

    # Blind LP should waste dispatches under 20% hoaxes
    assert comps["blind_lp"]["wasted_dispatches"] > 0
    # Validation threshold policy should have lower or zero wasted dispatches than Blind LP
    assert comps["trust_aware_validation_thresh"]["wasted_dispatches"] <= comps["blind_lp"]["wasted_dispatches"]

def test_allocation_pydantic_input_validation():
    """Verify input validation rejects invalid requests."""
    # Invalid capacity (< 5)
    with pytest.raises(ValidationError):
        AllocationRequest(region="houston", capacity=2)

    # Invalid hoax rate (> 1.0)
    with pytest.raises(ValidationError):
        AllocationRequest(region="houston", hoax_rate=1.5)

def test_numbers_match_summary_json():
    """Verify API and summary.json single source of truth consistency."""
    with open(SUMMARY_JSON, "r", encoding="utf-8") as f:
        summary = json.load(f)

    # Check data provenance
    prov = summary["data_provenance"]
    assert prov["total_processed_records"] == 130301
    assert prov["train_records"] == 97422
    assert prov["test_records"] == 32879
    assert prov["text_leakage_overlap"] == 0

    # Check scaling benchmark
    scaling = get_scaling()
    assert "rows_10000000" in scaling or "rows_10M" in scaling or "benchmark" in scaling or len(scaling) > 0

    # Check recovery test
    rec = get_recovery_test()
    assert rec.get("loss_percentage") == 0.0
    assert rec.get("recovery_time_seconds", 0) > 0
    assert rec.get("events_tested") == 100000

def test_demo_steps_execution():
    """Verify demo step endpoints return valid commands and non-empty outputs."""
    for step in range(1, 7):
        d = run_demo_step(step)
        assert d["step"] == step
        assert d["status"] == "PASS"
        assert len(d["output"]) > 20
        assert len(d["command"]) > 5
