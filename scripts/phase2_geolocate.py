"""Phase 2 — Geolocation.

Extracts location information from contract descriptions,
geocodes unique municipalities via Nominatim, and produces a geolocated cohort.
"""

import json
import re
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests

DATA_DIR = Path("data")
CACHE_FILE = DATA_DIR / "geocode_cache.json"

# Department bounding boxes (approximate lat/lon ranges for validation)
DEPT_BOUNDS = {
    "ANTIOQUIA":          {"lat": (5.4, 8.9),  "lon": (-77.1, -73.9)},
    "CUNDINAMARCA":       {"lat": (3.7, 5.9),  "lon": (-74.9, -73.0)},
    "BOYACA":             {"lat": (4.7, 7.1),  "lon": (-74.5, -72.3)},
    "SANTANDER":          {"lat": (5.7, 8.1),  "lon": (-74.3, -72.5)},
    "ATLANTICO":          {"lat": (10.2, 11.1), "lon": (-75.3, -74.7)},
    "VALLE DEL CAUCA":    {"lat": (3.0, 5.0),  "lon": (-77.5, -75.4)},
    "NORTE DE SANTANDER": {"lat": (6.9, 9.3),  "lon": (-73.7, -72.0)},
    "TOLIMA":             {"lat": (2.8, 5.4),  "lon": (-76.2, -74.7)},
    "HUILA":              {"lat": (1.5, 3.8),  "lon": (-76.7, -74.8)},
    "META":               {"lat": (1.6, 4.9),  "lon": (-74.9, -71.1)},
}

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
HEADERS = {
    "User-Agent": "viz-colombia-demo/1.0 (university-research-project)",
    "Accept": "application/json",
}


def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def extract_work_municipality(description: str) -> str | None:
    """Extract the municipality where work is performed from the description."""
    if not description:
        return None

    desc = description.upper()

    # Pattern 1: "MUNICIPIO DE X"
    m = re.search(
        r"MUNICIPIO\s+DE\s+([A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s]+?)(?:\s*[;,\-\.\n]|\s+(?:DEL?\s+)?(?:DEPARTAMENTO|EN\s+EL|CUNDINAMARCA|BOYAC|SANTANDER|ANTIOQUIA|VALLE|TOLIMA|HUILA|META|ATLANTICO|NORTE)|\s*$)",
        desc,
    )
    if m:
        return m.group(1).strip()

    # Pattern 2: "DISTRITO ESPECIAL DE X" / "CIUDAD DE X"
    m = re.search(
        r"(?:DISTRITO\s+(?:ESPECIAL\s+)?DE|CIUDAD\s+DE)\s+([A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s]+?)(?:\s*[;,\-\.\n]|\s*$)",
        desc,
    )
    if m:
        name = m.group(1).strip()
        name = re.sub(r"^SANTIAGO\s+DE\s+", "", name)
        return name

    return None


def extract_landmark(description: str) -> str | None:
    """Extract a specific landmark name for potential precision."""
    if not description:
        return None
    desc = description.upper()
    patterns = [
        r"(ESTADIO\s+(?:DE\s+)?(?:ATLETISMO\s+)?[A-ZÁÉÍÓ��ÜÑ][A-ZÁÉÍÓÚÜÑ\s]{3,30}?)(?:\s*[;,\-]|\s+EN\s+|\s+DEL?\s+)",
        r"(PARQUE\s+(?:RECREO\s+)?(?:DEPORTIVO\s+)?(?:PRINCIPAL\s+)?[A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s]{3,30}?)(?:\s*[;,\-]|\s+EN\s+|\s+DEL?\s+)",
        r"(PLAZA\s+DE\s+MERCADO)",
        r"(INSTITUCI[OÓ]N\s+EDUCATIVA\s+[A-ZÁÉÍÓÚÜÑ][A-ZÁÉÍÓÚÜÑ\s]{3,30}?)(?:\s*[;,\-]|\s+EN\s+|\s+DEL?\s+)",
    ]
    for pat in patterns:
        m = re.search(pat, desc)
        if m:
            return m.group(1).strip()
    return None


class NominatimGeocoder:
    """Direct Nominatim geocoder with cache and rate limiting."""

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.cache: dict[str, dict | None] = {}
        if cache_path.exists():
            with open(cache_path) as f:
                self.cache = json.load(f)
        self._last_request = 0.0
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < 1.5:
            time.sleep(1.5 - elapsed)
        self._last_request = time.time()

    def geocode(self, query: str) -> dict | None:
        cache_key = query.lower().strip()
        if cache_key in self.cache:
            return self.cache[cache_key]

        # Try up to 3 times with backoff
        for attempt in range(3):
            self._rate_limit()
            try:
                resp = self.session.get(
                    NOMINATIM_URL,
                    params={
                        "q": query,
                        "countrycodes": "co",
                        "format": "json",
                        "limit": 1,
                    },
                    timeout=10,
                )
                if resp.status_code == 403:
                    wait = 5 * (attempt + 1)
                    print(f"    403 rate limited, waiting {wait}s...", flush=True)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                print(f"    Error (attempt {attempt + 1}): {e}", flush=True)
                time.sleep(3 * (attempt + 1))
                data = []

        if data:
            r = data[0]
            result = {
                "lat": float(r["lat"]),
                "lon": float(r["lon"]),
                "display_name": r.get("display_name", ""),
            }
        else:
            result = None

        self.cache[cache_key] = result
        self._save_cache()
        return result

    def _save_cache(self):
        with open(self.cache_path, "w") as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)


def validate_in_department(lat: float, lon: float, dept_norm: str) -> bool:
    bounds = DEPT_BOUNDS.get(dept_norm)
    if not bounds:
        return True
    return (bounds["lat"][0] <= lat <= bounds["lat"][1] and
            bounds["lon"][0] <= lon <= bounds["lon"][1])


def main():
    print("=" * 60, flush=True)
    print("PHASE 2 — GEOLOCATION", flush=True)
    print("=" * 60, flush=True)

    cohort = pd.read_parquet(DATA_DIR / "cohort_candidates.parquet")
    print(f"Loaded {len(cohort)} candidates", flush=True)

    geocoder = NominatimGeocoder(CACHE_FILE)

    # Step 1: Extract target municipality for each contract
    print("\n[Step 1] Extracting municipalities from descriptions...", flush=True)
    extractions = []
    for _, row in cohort.iterrows():
        desc = row["object_description"] if pd.notna(row["object_description"]) else ""
        entity_muni = row["municipality_norm"] if pd.notna(row["municipality_norm"]) else None
        dept = row["department_norm"]

        work_muni = extract_work_municipality(desc)
        landmark = extract_landmark(desc)

        if work_muni:
            muni = work_muni.title()
            source = "description"
        elif entity_muni:
            muni = entity_muni.title()
            source = "entity"
        else:
            muni = None
            source = "none"

        extractions.append({
            "contract_id": row["contract_id"],
            "department_norm": dept,
            "municipality": muni,
            "muni_source": source,
            "landmark": landmark,
        })

    ext_df = pd.DataFrame(extractions)
    no_muni = ext_df["municipality"].isna().sum()
    print(f"  Extracted: {len(ext_df) - no_muni} with municipality, {no_muni} without", flush=True)
    print(f"  From description: {(ext_df['muni_source'] == 'description').sum()}", flush=True)
    print(f"  From entity: {(ext_df['muni_source'] == 'entity').sum()}", flush=True)

    # Step 2: Get unique municipality+department combinations
    has_muni = ext_df[ext_df["municipality"].notna()].copy()
    unique_locations = has_muni[["municipality", "department_norm"]].drop_duplicates()
    print(f"\n[Step 2] Geocoding {len(unique_locations)} unique municipality+department pairs...", flush=True)

    muni_coords: dict[tuple[str, str], dict | None] = {}
    for i, (_, loc) in enumerate(unique_locations.iterrows()):
        muni = loc["municipality"]
        dept = loc["department_norm"]
        dept_title = dept.title()
        query = f"{muni}, {dept_title}, Colombia"
        print(f"  [{i + 1}/{len(unique_locations)}] {query}", end="", flush=True)

        result = geocoder.geocode(query)
        key = (muni, dept)

        if result:
            lat, lon = result["lat"], result["lon"]
            if validate_in_department(lat, lon, dept):
                muni_coords[key] = result
                print(f" -> ({lat:.4f}, {lon:.4f})", flush=True)
            else:
                muni_coords[key] = None
                print(f" -> REJECTED (outside {dept})", flush=True)
        else:
            muni_coords[key] = None
            print(f" -> NOT FOUND", flush=True)

    found = sum(1 for v in muni_coords.values() if v is not None)
    print(f"\n  Geocoded: {found}/{len(unique_locations)} unique locations", flush=True)

    # Step 3: Map coordinates back to contracts
    print(f"\n[Step 3] Mapping coordinates to contracts...", flush=True)

    results = []
    rejection_reasons: dict[str, int] = {}

    for _, ext in ext_df.iterrows():
        cid = ext["contract_id"]
        muni = ext["municipality"]
        dept = ext["department_norm"]

        if muni is None:
            rejection_reasons["no_municipality"] = rejection_reasons.get("no_municipality", 0) + 1
            continue

        coords = muni_coords.get((muni, dept))
        if coords is None:
            rejection_reasons["geocode_failed_or_rejected"] = rejection_reasons.get("geocode_failed_or_rejected", 0) + 1
            continue

        results.append({
            "contract_id": cid,
            "lat": coords["lat"],
            "lon": coords["lon"],
            "geocode_tier": "B",
            "geocode_confidence": 0.6 if ext["muni_source"] == "description" else 0.4,
            "geocode_source": f"Nominatim: {muni}, {dept.title()}, Colombia",
            "extracted_municipality": muni,
            "extracted_landmark": ext["landmark"],
            "display_name": coords["display_name"],
        })

    print(f"  Mapped {len(results)} contracts to coordinates", flush=True)

    # Step 4: Save
    if not results:
        print("ERROR: No sites geocoded!", flush=True)
        sys.exit(1)

    geo_df = pd.DataFrame(results)
    out = cohort.merge(geo_df, on="contract_id", how="inner")
    out_path = DATA_DIR / "cohort_geolocated.parquet"
    out.to_parquet(out_path, index=False, compression="zstd")
    print(f"\nSaved {len(out)} rows to {out_path}", flush=True)

    # --- Phase Report ---
    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 2 REPORT", flush=True)
    print(f"{'=' * 60}", flush=True)

    print(f"\nFinal cohort size: {len(out)}", flush=True)

    print(f"\n--- Geocode Confidence Distribution ---", flush=True)
    for conf, count in out["geocode_confidence"].value_counts().sort_index().items():
        print(f"  {conf:.2f}: {count}", flush=True)

    print(f"\n--- Rejection Reasons ---", flush=True)
    for reason, count in sorted(rejection_reasons.items()):
        print(f"  {reason}: {count}", flush=True)

    print(f"\n--- Sites by Department ---", flush=True)
    dept_counts = out["department_norm"].value_counts()
    for dept, count in dept_counts.items():
        print(f"  {dept}: {count}", flush=True)
    zero_depts = set(DEPT_BOUNDS.keys()) - set(dept_counts.index)
    if zero_depts:
        print(f"\n  Departments with ZERO sites: {', '.join(sorted(zero_depts))}", flush=True)

    # 5 examples for spot-check
    print(f"\n--- 5 Example Sites (spot-check) ---", flush=True)
    for _, r in out.head(5).iterrows():
        print(f"\n  Contract: {r['contract_id']}", flush=True)
        print(f"  Dept: {r['department_norm']}, Entity Muni: {r.get('municipality_norm', 'N/A')}", flush=True)
        print(f"  Extracted muni: {r['extracted_municipality']}", flush=True)
        print(f"  Landmark: {r['extracted_landmark']}", flush=True)
        print(f"  Coords: ({r['lat']:.4f}, {r['lon']:.4f})", flush=True)
        print(f"  Source: {r['geocode_source']}", flush=True)
        desc_short = str(r['object_description'])[:120]
        print(f"  Desc: {desc_short}", flush=True)


if __name__ == "__main__":
    main()
