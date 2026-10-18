"""
backend/disaster_regions.py
===========================
Single source of truth configuration for regional disaster event mapping.
Defines the strict disaster-event binding for emergency coordination:
- Houston     <-> Hurricane Harvey (Flood & Water Rescue)
- California  <-> California Wildfires
- Nepal       <-> Nepal Earthquake
- Miami       <-> Hurricane Irma / Coastal Storm

Filters authentic disaster posts and injected hoaxes so that no off-topic,
general-domain (HC3, MAGE, LIAR), or unreadable content enters the coordinator feed.
"""

import re
from typing import Dict, Any, List

REGIONAL_CONFIGS = {
    "houston": {
        "region_key": "houston",
        "name": "Houston Metro Area",
        "disaster": "Hurricane Harvey (Flood)",
        "disaster_name": "Hurricane Harvey (Flood)",
        "event_tag": "Flood",
        "resource": "rescue_boats",
        "resource_type": "rescue_boats",
        "center": [29.7604, -95.3698],
        "bounds": {"min_lat": 29.3, "max_lat": 30.2, "min_lon": -95.9, "max_lon": -95.0},
        "default_depots": [
            {"id": "hou_downtown", "name": "Downtown Central Marina Base", "latitude": 29.7560, "longitude": -95.3620, "capacities": {"rescue_boats": 15}},
            {"id": "hou_katy", "name": "West Sector Rescue Outpost (Katy)", "latitude": 29.7858, "longitude": -95.8245, "capacities": {"rescue_boats": 12}},
            {"id": "hou_sugarland", "name": "South Sector Marine Staging", "latitude": 29.6197, "longitude": -95.6349, "capacities": {"rescue_boats": 10}},
            {"id": "hou_cypress", "name": "North Sector Boat Depot", "latitude": 29.9688, "longitude": -95.6972, "capacities": {"rescue_boats": 15}}
        ]
    },
    "california": {
        "region_key": "california",
        "name": "Northern California",
        "disaster": "California Wildfires",
        "disaster_name": "California Wildfires",
        "event_tag": "Wildfire",
        "resource": "wildfire_units",
        "resource_type": "wildfire_units",
        "center": [38.5816, -121.4944],
        "bounds": {"min_lat": 36.5, "max_lat": 39.5, "min_lon": -123.0, "max_lon": -119.5},
        "default_depots": [
            {"id": "ca_sacramento", "name": "Sacramento Fire Air Base", "latitude": 38.5816, "longitude": -121.4944, "capacities": {"wildfire_units": 14}},
            {"id": "ca_napa", "name": "Napa Valley Fire Ops", "latitude": 38.2975, "longitude": -122.2869, "capacities": {"wildfire_units": 12}},
            {"id": "ca_oakland", "name": "Oakland Bay Dispatch", "latitude": 37.8044, "longitude": -122.2712, "capacities": {"wildfire_units": 12}},
            {"id": "ca_fresno", "name": "Fresno Central Fire Logistics", "latitude": 36.7468, "longitude": -119.7726, "capacities": {"wildfire_units": 14}}
        ]
    },
    "nepal": {
        "region_key": "nepal",
        "name": "Kathmandu Valley",
        "disaster": "Nepal Earthquake",
        "disaster_name": "Nepal Earthquake",
        "event_tag": "Earthquake",
        "resource": "earthquake_rescue_teams",
        "resource_type": "earthquake_rescue_teams",
        "center": [27.7172, 85.3240],
        "bounds": {"min_lat": 27.4, "max_lat": 28.3, "min_lon": 83.8, "max_lon": 85.6},
        "default_depots": [
            {"id": "nep_kathmandu", "name": "Kathmandu Central Search Base", "latitude": 27.7172, "longitude": 85.3240, "capacities": {"earthquake_rescue_teams": 15}},
            {"id": "nep_pokhara", "name": "Pokhara Rescue Outpost", "latitude": 28.2096, "longitude": 83.9856, "capacities": {"earthquake_rescue_teams": 12}},
            {"id": "nep_lalitpur", "name": "Lalitpur Medical Base", "latitude": 27.6588, "longitude": 85.3247, "capacities": {"earthquake_rescue_teams": 12}},
            {"id": "nep_bhaktapur", "name": "Bhaktapur Collapse Ops", "latitude": 27.6710, "longitude": 85.4298, "capacities": {"earthquake_rescue_teams": 13}}
        ]
    },
    "miami": {
        "region_key": "miami",
        "name": "South Florida Metro",
        "disaster": "Hurricane Irma & Coastal Storm",
        "disaster_name": "Hurricane Irma & Coastal Storm",
        "event_tag": "Hurricane",
        "resource": "ambulances",
        "resource_type": "ambulances",
        "center": [25.7617, -80.1918],
        "bounds": {"min_lat": 25.5, "max_lat": 26.2, "min_lon": -80.5, "max_lon": -80.0},
        "default_depots": [
            {"id": "mia_downtown", "name": "Miami Health District Depot", "latitude": 25.7907, "longitude": -80.2104, "capacities": {"ambulances": 16}},
            {"id": "mia_beach", "name": "Miami Beach Urgent Transport", "latitude": 25.7907, "longitude": -80.1300, "capacities": {"ambulances": 12}},
            {"id": "mia_coralgables", "name": "Coral Gables Regional Staging", "latitude": 25.7215, "longitude": -80.2684, "capacities": {"ambulances": 12}},
            {"id": "mia_hialeah", "name": "Hialeah Fleet Headquarters", "latitude": 25.8576, "longitude": -80.2781, "capacities": {"ambulances": 14}}
        ]
    }
}

ALLOWED_SOURCES = {
    "HumAID_CrisisBench",
    "CrisisLex_DisasterTweets",
    "PHEME_RumourDataset",
    "hoax_generator_a",
    "hoax_generator_b",
    "hoax_generator_c"
}

BANNED_SOURCES = {
    "HC3_Benchmark",
    "MAGE_Benchmark",
    "LIAR_FactChecking"
}

def matches_region_event(record: Dict[str, Any], region_key: str) -> bool:
    """
    Strict regional disaster-event matching.
    Drops any post from generic or unknown disasters, or general-domain corpora.
    """
    source = str(record.get("source", ""))
    if source in BANNED_SOURCES or (source and source not in ALLOWED_SOURCES):
        return False

    ev = str(record.get("event_type", "")).lower()
    dt = str(record.get("disaster_type", "")).lower()

    # Drop CrisisLex generic / off-topic posts
    if "crisislex_general" in ev or "general" in dt:
        return False
    if ev in ["disaster_events", "general", "unknown", ""]:
        return False

    text = str(record.get("text", "")).lower()
    blob = f"{ev} {dt} {text}"

    reg = region_key.lower()
    if reg == "houston":
        # Houston <-> Hurricane Harvey & Flood
        if "harvey" in ev or ("flood" in dt and any(k in blob for k in ["houston", "harvey", "texas", "harris", "katy"])):
            return True
        # For injected hoaxes assigned to Houston:
        if "hoax_generator" in source and dt in ["flood", "hurricane"]:
            return True
        return False

    elif reg == "california":
        # California <-> Wildfire
        if dt == "wildfire" or any(k in ev for k in ["wildfire", "bushfire", "california"]):
            return True
        if "hoax_generator" in source and dt == "wildfire":
            return True
        return False

    elif reg == "nepal":
        # Nepal <-> Nepal Earthquake
        if "nepal" in ev or ("earthquake" in dt and any(k in blob for k in ["nepal", "kathmandu", "pokhara", "lalitpur", "bhaktapur"])):
            return True
        if "hoax_generator" in source and dt == "earthquake":
            return True
        return False

    elif reg == "miami":
        # Miami <-> Hurricane Irma & Florida Coastal Storm
        if any(k in ev for k in ["irma", "maria", "florida"]) or ("hurricane" in dt and any(k in blob for k in ["florida", "miami", "irma", "key west"])):
            return True
        if "hoax_generator" in source and dt in ["hurricane", "cyclone", "flood"]:
            return True
        return False

    return False
