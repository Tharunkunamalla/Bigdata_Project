"""
Test live download URLs for real datasets:
- USGS Earthquakes GeoJSON
- NWS Active Weather Alerts
- OpenFEMA Disaster Declarations
- LIAR Rumor/Fake News dataset
- QCRI HumAID dataset
- Hugging Face HC3 dataset
"""

import urllib.request
import json
import ssl

ctx = ssl.create_default_context()

def test_url(name, url, headers=None, is_json=True):
    try:
        req = urllib.request.Request(url, headers=headers or {"User-Agent": "CrisisGuard-Research/1.0"})
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            content = resp.read()
            if is_json:
                data = json.loads(content)
                print(f"[OK] {name}: Successfully retrieved JSON ({len(content)} bytes)")
                return True, data
            else:
                print(f"[OK] {name}: Successfully retrieved text ({len(content)} bytes, {len(content.splitlines())} lines)")
                return True, content
    except Exception as e:
        print(f"[FAIL] {name}: {e}")
        return False, None

def main():
    print("Testing external dataset endpoints...")
    test_url("USGS Earthquakes", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson")
    test_url("NWS Weather Alerts", "https://api.weather.gov/alerts/active", headers={"User-Agent": "CrisisGuard-Academic/1.0 (contact@cse412.edu)"})
    test_url("OpenFEMA Declarations", "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries?$top=5")
    test_url("LIAR Dataset Train", "https://raw.githubusercontent.com/tobsecret/LIAR-PLUS/master/dataset/tsv/train.tsv", is_json=False)
    test_url("HumAID Index / Metadata", "https://raw.githubusercontent.com/qcri/HumAID/main/README.md", is_json=False)

if __name__ == "__main__":
    main()
