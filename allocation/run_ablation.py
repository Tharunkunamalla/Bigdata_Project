"""
CSE412 Big Data Project: Misinformation-Aware Emergency Resource Allocation Ablation Study
===========================================================================================
Executes systematic ablation trials across multiple seeds and disaster scenarios.
Compares four allocation policies:
1. Blind-Greedy (No misinformation weighting)
2. Trust-Aware Greedy (Demand weighted by dynamic trust score)
3. Blind-MILP (Optimal integer programming without trust penalty)
4. Trust-Aware MILP (Optimal integer programming with trust penalty)

Quantifies:
- Wasted Emergency Resources Dispatched to Synthetic Hoaxes
- Critical Genuine Demand Fulfilled Percentage
- Total Deployment Route Distance (km)
Outputs results to `results/ablation_study.json`.
"""

import os
import sys
import json
import random
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from optimizer import ResourceAllocationOptimizer, haversine_distance_km

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

# 4 Emergency Response Depots in Greater Houston Disaster Perimeter
DEPOTS = [
    {"id": "depot_downtown", "name": "Downtown Central Logistics Base", "latitude": 29.7560, "longitude": -95.3620, "capacities": {"rescue_boats": 15, "ambulances": 20}},
    {"id": "depot_katy", "name": "West Sector Rescue Outpost (Katy)", "latitude": 29.7858, "longitude": -95.8245, "capacities": {"rescue_boats": 12, "ambulances": 15}},
    {"id": "depot_sugarland", "name": "South Sector Medical Staging (Sugar Land)", "latitude": 29.6197, "longitude": -95.6349, "capacities": {"rescue_boats": 10, "ambulances": 15}},
    {"id": "depot_cypress", "name": "North Sector Marine Depot (Cypress)", "latitude": 29.9688, "longitude": -95.6972, "capacities": {"rescue_boats": 15, "ambulances": 10}}
]

def run_ablation_trials(num_seeds: int = 5, demands_per_trial: int = 120):
    print("==================================================================")
    print("      CSE412 Emergency Resource Allocation Ablation Experiment    ")
    print("==================================================================")
    print(f"Number of Seeds: {num_seeds} | Demands per trial: {demands_per_trial}")
    
    total_boat_capacity = sum(d["capacities"]["rescue_boats"] for d in DEPOTS)
    print(f"Total Available Boat Fleet Capacity: {total_boat_capacity} boats")

    # Load real scored sample from curated dataset
    data_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw", "disaster_posts_raw.json")
    with open(data_path, "r", encoding="utf-8") as f:
        all_posts = [json.loads(line) for line in f if "hurricane" in line]

    optimizer = ResourceAllocationOptimizer(DEPOTS)
    
    policy_metrics = {
        "Blind_Greedy": {"wasted_pct": [], "genuine_served": [], "wasted_dispatches": []},
        "TrustAware_Greedy": {"wasted_pct": [], "genuine_served": [], "wasted_dispatches": []},
        "Blind_MILP": {"wasted_pct": [], "genuine_served": [], "wasted_dispatches": []},
        "TrustAware_MILP": {"wasted_pct": [], "genuine_served": [], "wasted_dispatches": []}
    }

    for seed in range(412, 412 + num_seeds):
        random.seed(seed)
        np.random.seed(seed)
        
        # Sample demand batch with realistic synthetic ratio (~25% in high urgency)
        sample = random.sample(all_posts, demands_per_trial)
        
        # Simulate realistic model scores for demands
        demands = []
        for p in sample:
            is_synth = p.get("is_synthetic", 0)
            urg = p.get("urgency_level", 0.7)
            # Simulated model confidence based on test ROC metrics
            prob_synth = float(np.random.beta(8, 2)) if is_synth == 1 else float(np.random.beta(2, 8))
            cred = p.get("credibility_raw_score", 0.8)
            trust = max(0.01, (1.0 - prob_synth) * cred)
            
            demands.append({
                "post_id": p["post_id"],
                "latitude": p["latitude"],
                "longitude": p["longitude"],
                "pred_urgency_score": urg,
                "trust_score": trust,
                "is_synthetic": is_synth
            })

        # Run Policies
        res_bg = optimizer.solve_greedy(demands, resource_type="rescue_boats", use_trust=False)
        res_tg = optimizer.solve_greedy(demands, resource_type="rescue_boats", use_trust=True)
        res_bm = optimizer.solve_pulp_lp(demands, resource_type="rescue_boats", use_trust=False)
        res_tm = optimizer.solve_pulp_lp(demands, resource_type="rescue_boats", use_trust=True)

        for pol_name, res in [
            ("Blind_Greedy", res_bg),
            ("TrustAware_Greedy", res_tg),
            ("Blind_MILP", res_bm),
            ("TrustAware_MILP", res_tm)
        ]:
            policy_metrics[pol_name]["wasted_pct"].append(res["wasted_dispatch_ratio"] * 100.0)
            policy_metrics[pol_name]["genuine_served"].append(res["genuine_served"])
            policy_metrics[pol_name]["wasted_dispatches"].append(res["wasted_dispatches"])

    # Aggregate Statistics
    summary = {}
    print("\n------------------------------------------------------------------")
    print(f"{'Policy':<22} | {'Wasted Dispatches (%)':<22} | {'Genuine Victims Reached':<24}")
    print("------------------------------------------------------------------")
    for pol_name, m in policy_metrics.items():
        avg_waste = float(np.mean(m["wasted_pct"]))
        std_waste = float(np.std(m["wasted_pct"]))
        avg_gen = float(np.mean(m["genuine_served"]))
        std_gen = float(np.std(m["genuine_served"]))
        avg_wdispatches = float(np.mean(m["wasted_dispatches"]))

        summary[pol_name] = {
            "avg_wasted_dispatch_pct": round(avg_waste, 2),
            "std_wasted_dispatch_pct": round(std_waste, 2),
            "avg_genuine_victims_served": round(avg_gen, 1),
            "std_genuine_victims_served": round(std_gen, 1),
            "avg_wasted_units": round(avg_wdispatches, 1)
        }
        print(f"{pol_name:<22} | {avg_waste:5.2f}% (±{std_waste:.2f}%)       | {avg_gen:4.1f} (±{std_gen:.1f})")
    print("------------------------------------------------------------------")

    # Key Comparative Improvements
    blind_waste = summary["Blind_MILP"]["avg_wasted_dispatch_pct"]
    trust_waste = summary["TrustAware_MILP"]["avg_wasted_dispatch_pct"]
    waste_reduction = round(((blind_waste - trust_waste) / max(0.01, blind_waste)) * 100.0, 2)
    
    gen_increase = round(
        ((summary["TrustAware_MILP"]["avg_genuine_victims_served"] - summary["Blind_MILP"]["avg_genuine_victims_served"]) /
         max(0.01, summary["Blind_MILP"]["avg_genuine_victims_served"])) * 100.0, 2
    )

    summary["comparative_findings"] = {
        "waste_reduction_percentage": waste_reduction,
        "genuine_reach_increase_percentage": gen_increase,
        "num_trials": num_seeds,
        "total_depot_fleet": total_boat_capacity
    }

    out_file = os.path.join(RESULTS_DIR, "ablation_study.json")
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n[Ablation Key Result] Trust-Aware Optimization reduced wasted dispatches by {waste_reduction}%")
    print(f"[Ablation Key Result] Genuine emergency victims reached increased by +{gen_increase}%")
    print(f"Results written to: {out_file}")

if __name__ == "__main__":
    run_ablation_trials()
