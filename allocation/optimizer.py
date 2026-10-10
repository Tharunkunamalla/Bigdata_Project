"""
CSE412 Big Data Project: Trust-Aware Emergency Resource Allocation Engine
==========================================================================
Formulates and solves the multi-depot emergency vehicle allocation problem
under adversarial misinformation conditions.

Features:
- Geodesic distance matrix calculation (Haversine formula)
- Baseline Greedy Dispatcher (misinformation-blind)
- Trust-Aware Linear Programming Solver (using PuLP)
- Multi-resource capacity constraints (Ambulances, Rescue Boats, Supplies)
"""

import math
from typing import List, Dict, Any
import pulp

def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Computes great-circle distance between two GPS coordinates in kilometers."""
    R = 6371.0  # Earth radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2.0) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2.0) ** 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

class ResourceAllocationOptimizer:
    def __init__(self, depots: List[Dict[str, Any]]):
        """
        depots: List of depots with 'id', 'name', 'latitude', 'longitude', 'capacities' dict.
        """
        self.depots = depots

    def solve_greedy(self, demands: List[Dict[str, Any]], resource_type: str = "rescue_boats", use_trust: bool = False) -> Dict[str, Any]:
        """
        Greedy nearest-highest-priority dispatch.
        If use_trust=False, prioritizes purely on raw reported urgency.
        If use_trust=True, prioritizes on effective urgency (urgency * trust).
        """
        depot_capacities = {d["id"]: d["capacities"].get(resource_type, 0) for d in self.depots}
        
        # Sort demands by priority
        def priority_key(item):
            urg = item.get("pred_urgency_score", item.get("urgency_level", 0.5))
            if use_trust:
                trust = item.get("trust_score", 1.0)
                return urg * trust
            return urg

        sorted_demands = sorted(demands, key=priority_key, reverse=True)
        
        allocations = []
        wasted_dispatches = 0
        genuine_served = 0
        total_distance = 0.0

        for demand in sorted_demands:
            d_lat = demand["latitude"]
            d_lon = demand["longitude"]
            is_synthetic = demand.get("is_synthetic", 0)

            # Find nearest depot with available capacity
            best_depot = None
            best_dist = float("inf")
            for depot in self.depots:
                if depot_capacities[depot["id"]] > 0:
                    dist = haversine_distance_km(d_lat, d_lon, depot["latitude"], depot["longitude"])
                    if dist < best_dist:
                        best_dist = dist
                        best_depot = depot

            if best_depot is not None:
                depot_capacities[best_depot["id"]] -= 1
                allocations.append({
                    "demand_id": demand.get("post_id", "unknown"),
                    "depot_id": best_depot["id"],
                    "distance_km": round(best_dist, 2),
                    "resource_type": resource_type,
                    "is_synthetic": is_synthetic
                })
                total_distance += best_dist
                if is_synthetic == 1:
                    wasted_dispatches += 1
                else:
                    genuine_served += 1

        return {
            "method": "Greedy" + ("_TrustAware" if use_trust else "_Blind"),
            "allocations_count": len(allocations),
            "genuine_served": genuine_served,
            "wasted_dispatches": wasted_dispatches,
            "wasted_dispatch_ratio": round(wasted_dispatches / max(1, len(allocations)), 4),
            "total_distance_km": round(total_distance, 2),
            "allocations": allocations
        }

    def solve_pulp_lp(self, demands: List[Dict[str, Any]], resource_type: str = "rescue_boats", use_trust: bool = True, dist_penalty: float = 0.05) -> Dict[str, Any]:
        """
        Optimal Mixed-Integer Linear Program (MILP) allocation.
        Objective: Maximize sum(urgency * trust * x_ij) - dist_penalty * dist_ij * x_ij
        """
        prob = pulp.LpProblem("Emergency_Resource_Allocation", pulp.LpMaximize)

        # Decision variables x_ij: 1 if depot j serves demand i, else 0
        x = {}
        for i, dem in enumerate(demands):
            for j, dep in enumerate(self.depots):
                x[(i, j)] = pulp.LpVariable(f"x_{i}_{j}", cat=pulp.LpBinary)

        # Objective function coefficients
        obj_terms = []
        for i, dem in enumerate(demands):
            urg = dem.get("pred_urgency_score", dem.get("urgency_level", 0.5))
            trust = dem.get("trust_score", 1.0) if use_trust else 1.0
            benefit = urg * trust

            for j, dep in enumerate(self.depots):
                dist = haversine_distance_km(dem["latitude"], dem["longitude"], dep["latitude"], dep["longitude"])
                net_value = benefit - (dist_penalty * (dist / 100.0))
                obj_terms.append(net_value * x[(i, j)])

        prob += pulp.lpSum(obj_terms)

        # Capacity constraints per depot
        for j, dep in enumerate(self.depots):
            cap = dep["capacities"].get(resource_type, 0)
            prob += pulp.lpSum([x[(i, j)] for i in range(len(demands))]) <= cap, f"Cap_Depot_{j}"

        # Each demand can be served at most once
        for i in range(len(demands)):
            prob += pulp.lpSum([x[(i, j)] for j in range(len(self.depots))]) <= 1, f"Demand_Once_{i}"

        # Solve silently
        prob.solve(pulp.PULP_CBC_CMD(msg=False))

        allocations = []
        wasted_dispatches = 0
        genuine_served = 0
        total_distance = 0.0

        for i, dem in enumerate(demands):
            for j, dep in enumerate(self.depots):
                if pulp.value(x[(i, j)]) is not None and pulp.value(x[(i, j)]) > 0.5:
                    dist = haversine_distance_km(dem["latitude"], dem["longitude"], dep["latitude"], dep["longitude"])
                    is_synth = dem.get("is_synthetic", 0)
                    allocations.append({
                        "demand_id": dem.get("post_id", f"dem_{i}"),
                        "depot_id": dep["id"],
                        "distance_km": round(dist, 2),
                        "resource_type": resource_type,
                        "is_synthetic": is_synth
                    })
                    total_distance += dist
                    if is_synth == 1:
                        wasted_dispatches += 1
                    else:
                        genuine_served += 1

        return {
            "method": "MILP_Optimal" + ("_TrustAware" if use_trust else "_Blind"),
            "status": pulp.LpStatus[prob.status],
            "objective_value": round(float(pulp.value(prob.objective)), 4),
            "allocations_count": len(allocations),
            "genuine_served": genuine_served,
            "wasted_dispatches": wasted_dispatches,
            "wasted_dispatch_ratio": round(wasted_dispatches / max(1, len(allocations)), 4),
            "total_distance_km": round(total_distance, 2),
            "allocations": allocations
        }
