#!/usr/bin/env python3
"""Build Dashboard Data.

Reads parquet outputs, computes department aggregations,
prepares GeoJSON, and writes dashboard/public/data.json.

Usage:
    uv run python scripts/build_dashboard.py
"""

import hashlib
import json
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import requests

DATA_DIR = Path("data")
OUT_DIR = Path("dashboard/public")
GEOJSON_CACHE = DATA_DIR / "colombia_departments.geojson"
GEOJSON_URL = (
    "https://gist.githubusercontent.com/john-guerra/"
    "43c7656821069d00dcbc/raw/colombia.geo.json"
)

COP_TO_USD = 4_000
PAGE_SIZE = 100  # contracts per page in frontend table

CATEGORIES = {
    "all":           [],
    "execution":     ["z_stall", "z_slip_contract"],
    "competition":   ["z_single_bidder_entity", "z_bunching_entity", "z_award_speed_abs"],
    "pricing":       ["z_creep_contract", "z_creep_contractor", "z_fragmentation"],
    "relationships": ["z_hhi_entity", "z_relationship"],
}

DEPT_CENTROIDS = {
    "AMAZONAS":(-1.0,-71.9),"ANTIOQUIA":(7.0,-75.5),"ARAUCA":(6.5,-70.7),
    "ATLANTICO":(10.7,-75.0),"BOLIVAR":(8.6,-74.0),"BOYACA":(5.9,-73.4),
    "CALDAS":(5.3,-75.5),"CAQUETA":(1.0,-75.6),"CASANARE":(5.3,-71.3),
    "CAUCA":(2.3,-76.8),"CESAR":(9.3,-73.5),"CHOCO":(5.7,-76.6),
    "CORDOBA":(8.3,-75.6),"CUNDINAMARCA":(5.0,-74.0),
    "DISTRITO CAPITAL DE BOGOTA":(4.65,-74.1),"GUAINIA":(2.6,-68.2),
    "GUAVIARE":(2.0,-72.6),"HUILA":(2.5,-75.5),"LA GUAJIRA":(11.4,-72.4),
    "MAGDALENA":(10.0,-74.0),"META":(3.3,-73.0),"NARINO":(1.3,-78.0),
    "NORTE DE SANTANDER":(7.9,-72.5),"PUTUMAYO":(0.4,-76.0),
    "QUINDIO":(4.5,-75.7),"RISARALDA":(5.0,-75.7),
    "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA":(12.5,-81.7),
    "SANTANDER":(6.6,-73.1),"SUCRE":(9.0,-75.4),"TOLIMA":(3.9,-75.2),
    "VALLE DEL CAUCA":(3.5,-76.5),"VAUPES":(1.0,-70.2),"VICHADA":(4.4,-69.3),
}

MUNI_CACHE = DATA_DIR / "municipality_coords.json"

Z_MAP = {
    "z_stall": "stall", "z_creep_contract": "creep_c",
    "z_creep_contractor": "creep_k", "z_slip_contract": "slip_c",
    "z_slip_contractor": "slip_k", "z_bunching_entity": "bunch",
    "z_hhi_entity": "hhi", "z_single_bidder_entity": "single",
    "z_award_speed_abs": "speed", "z_relationship": "rel",
    "z_fragmentation": "frag",
}

# Maps z-column base names to short keys (for global z-scores)
Z_GLOBAL_MAP = {
    "z_stall_global": "stall", "z_creep_contract_global": "creep_c",
    "z_creep_contractor_global": "creep_k", "z_slip_contract_global": "slip_c",
    "z_slip_contractor_global": "slip_k", "z_bunching_entity_global": "bunch",
    "z_hhi_entity_global": "hhi", "z_single_bidder_entity_global": "single",
    "z_award_speed_abs_global": "speed", "z_relationship_global": "rel",
    "z_fragmentation_global": "frag",
}

# Signal exemptions by cohort
COHORT_EXEMPTIONS = {
    "mandato": ["bunch", "single"],
    "directa": ["bunch", "single"],
    "eice": ["bunch"],
    "especial": ["bunch"],
    "especial_ese": ["bunch"],
    "especial_universidad": ["bunch"],
    "especial_d092": ["bunch"],
    "especial_convenio": ["bunch"],
    "especial_otro": ["bunch"],
}

SIGNAL_NAMES = {
    "stall": "Payment stall", "creep_c": "Value creep",
    "creep_k": "Contractor creep", "slip_c": "Schedule slip",
    "slip_k": "Contractor slip", "bunch": "Threshold bunching",
    "hhi": "Contractor concentration", "single": "Single bidder",
    "speed": "Award speed anomaly", "rel": "Relationship intensity",
    "frag": "Fragmentation",
}


# ── Utilities ──────────────────────────────────────────────────────

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.integer,)):  return int(obj)
        if isinstance(obj, (np.floating,)):
            return None if (np.isnan(obj) or np.isinf(obj)) else round(float(obj), 4)
        if isinstance(obj, np.ndarray):     return obj.tolist()
        if isinstance(obj, pd.Timestamp):   return obj.isoformat()
        return super().default(obj)


def strip_accents(s: str) -> str:
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


def sf(v, d=2):
    if v is None or (isinstance(v, float) and (np.isnan(v) or np.isinf(v))): return 0
    return round(float(v), d)


def si(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return 0
    return int(v)


DEPT_SPREAD = {
    "AMAZONAS": 1.5, "META": 1.0, "VICHADA": 1.2, "GUAINIA": 1.0,
    "CAQUETA": 1.0, "GUAVIARE": 0.8, "VAUPES": 0.8, "PUTUMAYO": 0.6,
    "CASANARE": 0.6, "CHOCO": 0.7, "ANTIOQUIA": 0.7, "BOLIVAR": 0.6,
    "SANTANDER": 0.6, "BOYACA": 0.5, "CUNDINAMARCA": 0.4, "TOLIMA": 0.5,
    "HUILA": 0.5, "CAUCA": 0.5, "NARINO": 0.5, "ARAUCA": 0.4,
    "NORTE DE SANTANDER": 0.5, "CESAR": 0.5, "MAGDALENA": 0.5,
    "LA GUAJIRA": 0.5, "CORDOBA": 0.5, "SUCRE": 0.3,
    "ATLANTICO": 0.15, "QUINDIO": 0.15, "RISARALDA": 0.2,
    "CALDAS": 0.25, "VALLE DEL CAUCA": 0.4,
    "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA": 0.1,
    "DISTRITO CAPITAL DE BOGOTA": 0.08,
}

COL_BOUNDS = {"lat_min": -4.2, "lat_max": 13.4, "lon_min": -81.7, "lon_max": -66.9}


def build_municipality_lookup(refpop, geocode=False):
    """Build (dept, muni) → (lat, lon) lookup from cache. Pass geocode=True to fetch missing."""
    import time

    muni_coords: dict[str, list[float] | None] = {}
    if MUNI_CACHE.exists():
        with open(MUNI_CACHE) as f:
            muni_coords = json.load(f)

    # Seed from phase-2 geocode cache
    geo_cache_path = DATA_DIR / "geocode_cache.json"
    if geo_cache_path.exists():
        with open(geo_cache_path) as f:
            geo_cache = json.load(f)
        for key, val in geo_cache.items():
            if val is None:
                continue
            parts = key.split(",")
            if len(parts) >= 2:
                muni = strip_accents(parts[0].strip())
                dept = strip_accents(parts[1].strip())
                cache_key = f"{dept}|{muni}"
                if cache_key not in muni_coords:
                    muni_coords[cache_key] = [val["lat"], val["lon"]]

    if geocode:
        munis = refpop.groupby(["department_norm", "municipality_norm"]).size().reset_index(name="n")
        munis = munis.sort_values("n", ascending=False)
        to_geocode = []
        for _, r in munis.iterrows():
            dept, muni = r["department_norm"], r["municipality_norm"]
            if pd.isna(muni) or not muni:
                continue
            cache_key = f"{dept}|{muni}"
            if cache_key not in muni_coords:
                to_geocode.append((dept, muni, cache_key))

        if to_geocode:
            print(f"  Geocoding {len(to_geocode)} municipalities via Nominatim...", flush=True)
            headers = {"User-Agent": "colombia-procurement-dashboard/0.1"}
            for i, (dept, muni, cache_key) in enumerate(to_geocode):
                # Try multiple query variants for tricky names
                variants = [
                    f"{muni.title()}, {dept.title()}, Colombia",
                    f"{muni.title()}, Colombia",
                ]
                # Strip common prefixes: "VILLA DE SAN DIEGO DE X" → "X"
                words = muni.split()
                if len(words) > 2:
                    variants.append(f"{words[-1].title()}, {dept.title()}, Colombia")
                # "GUADALAJARA DE BUGA" → "Buga"
                if " DE " in muni:
                    short = muni.split(" DE ")[-1].title()
                    variants.append(f"{short}, {dept.title()}, Colombia")
                # "PUERTO ASIS" → keep as is but also try "Puerto Asís"
                result = None
                for query in variants:
                    try:
                        resp = requests.get(
                            "https://nominatim.openstreetmap.org/search",
                            params={"q": query, "format": "json", "limit": 1, "countrycodes": "co"},
                            headers=headers, timeout=10,
                        )
                        results = resp.json()
                        if results:
                            result = [float(results[0]["lat"]), float(results[0]["lon"])]
                            break
                    except Exception:
                        pass
                    time.sleep(1.1)
                muni_coords[cache_key] = result
                if (i + 1) % 50 == 0:
                    print(f"    {i + 1}/{len(to_geocode)}...", flush=True)
                    with open(MUNI_CACHE, "w") as f:
                        json.dump(muni_coords, f)
                if not result:
                    time.sleep(1.1)

    # Save
    with open(MUNI_CACHE, "w") as f:
        json.dump(muni_coords, f)

    # Build lookup
    lookup: dict[tuple[str, str], tuple[float, float]] = {}
    for key, val in muni_coords.items():
        if val is None:
            continue
        parts = key.split("|")
        if len(parts) == 2:
            lookup[(parts[0], parts[1])] = (val[0], val[1])
    return lookup

def jitter_coords(contract_id: str, base_lat: float, base_lon: float, dept: str = ""):
    """Deterministic gaussian-like jitter, clamped to Colombia bounds."""
    import math
    spread = DEPT_SPREAD.get(dept, 0.4)
    h = hashlib.md5(contract_id.encode()).hexdigest()
    u1 = max(int(h[:8], 16) / 0xFFFFFFFF, 0.01)
    u2 = int(h[8:16], 16) / 0xFFFFFFFF
    r = min(spread * 0.45 * math.sqrt(-2 * math.log(u1)), spread)  # cap at 1x spread
    theta = 2 * math.pi * u2
    lat = base_lat + r * math.sin(theta)
    lon = base_lon + r * math.cos(theta)
    lat = max(COL_BOUNDS["lat_min"], min(COL_BOUNDS["lat_max"], lat))
    lon = max(COL_BOUNDS["lon_min"], min(COL_BOUNDS["lon_max"], lon))
    return round(lat, 4), round(lon, 4)


def top_signals_text(z: dict) -> str:
    ranked = sorted(
        ((k, abs(v)) for k, v in z.items()),
        key=lambda x: x[1], reverse=True,
    )
    return "; ".join(
        f"{SIGNAL_NAMES.get(k, k)} ({v:.1f}\u03c3)"
        for k, v in ranked[:3] if v > 0.5
    )


# ── Department Stats ───────────────────────────────────────────────

def compute_dept_stats(refpop, scored, demo):
    dept_map = refpop.set_index("contract_id")["department_norm"]
    sc = scored[~scored.get("dq_excluded", pd.Series(False, index=scored.index))].copy()
    sc["department_norm"] = sc["contract_id"].map(dept_map)

    for cat, signals in CATEGORIES.items():
        if signals:
            sc[f"cat_{cat}"] = sc[signals].mean(axis=1)

    counts = refpop.groupby("department_norm").agg(
        n_contracts=("contract_id", "count"),
        total_value_cop=("awarded_value_cop", "sum"),
    ).reset_index()

    # Flag contracts at P90+ composite percentile (consistent with KPI n_flagged)
    sc_flagged = sc[sc["composite_percentile"] >= 0.9].copy()
    flagged = sc_flagged.groupby("department_norm").agg(
        n_flagged=("contract_id", "count"),
        flagged_value_cop=("awarded_value_cop", "sum"),
    ).reset_index()

    # Use adjusted composite for department-level stats
    sc["composite_for_dept"] = sc["composite_adjusted"].fillna(sc["composite"])
    agg = {"composite": ("composite_for_dept", "mean")}
    for cat, signals in CATEGORIES.items():
        if signals:
            agg[f"cat_{cat}"] = (f"cat_{cat}", "mean")
    scores = sc.groupby("department_norm").agg(**agg).reset_index()

    stats = counts.merge(flagged, on="department_norm", how="left")
    stats = stats.merge(scores, on="department_norm", how="left")
    stats["n_flagged"] = stats["n_flagged"].fillna(0).astype(int)
    stats["flagged_value_cop"] = stats["flagged_value_cop"].fillna(0).astype(int)
    stats["flag_rate"] = stats["n_flagged"] / stats["n_contracts"]
    stats["cat_all"] = stats["composite"]
    return stats


# ── GeoJSON ────────────────────────────────────────────────────────

def simplify_coords(coords, precision=3):
    if isinstance(coords[0], (int, float)):
        return [round(c, precision) for c in coords]
    return [simplify_coords(c, precision) for c in coords]


def load_geojson():
    if GEOJSON_CACHE.exists():
        print(f"  Using cached GeoJSON", flush=True)
        with open(GEOJSON_CACHE) as f:
            return json.load(f)
    print(f"  Downloading GeoJSON...", flush=True)
    try:
        resp = requests.get(GEOJSON_URL, timeout=15)
        resp.raise_for_status()
        geo = resp.json()
        if geo.get("type") == "FeatureCollection" and len(geo.get("features", [])) > 10:
            with open(GEOJSON_CACHE, "w") as f:
                json.dump(geo, f)
            return geo
    except Exception as e:
        print(f"  Download failed: {e}", flush=True)
    print("  Using centroid fallback", flush=True)
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature",
             "geometry": {"type": "Point", "coordinates": [lon, lat]},
             "properties": {"NOMBRE_DPT": name}}
            for name, (lat, lon) in DEPT_CENTROIDS.items()
        ],
    }


def find_name_key(geojson):
    props = geojson["features"][0]["properties"]
    for k in ["NOMBRE_DPT", "DPTO_CNMBR", "DEPARTAMENTO", "NAME_1", "name"]:
        if k in props:
            return k
    return None


def enrich_geojson(geojson, dept_stats):
    name_key = find_name_key(geojson)
    if not name_key:
        return geojson

    stats = {r["department_norm"]: r for _, r in dept_stats.iterrows()}
    dept_names = set(dept_stats["department_norm"])

    specials = {
        "BOGOTA D.C.": "DISTRITO CAPITAL DE BOGOTA",
        "BOGOTA, D.C.": "DISTRITO CAPITAL DE BOGOTA",
        "BOGOTA DC": "DISTRITO CAPITAL DE BOGOTA",
        "BOGOTA": "DISTRITO CAPITAL DE BOGOTA",
        "SANTAFE DE BOGOTA D.C.": "DISTRITO CAPITAL DE BOGOTA",
        "SAN ANDRES": "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA",
        "SAN ANDRES Y PROVIDENCIA": "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA",
        "ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y SANTA CATALINA": "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA",
        "GUAJIRA": "LA GUAJIRA",
    }

    def resolve(raw):
        norm = strip_accents(raw)
        if norm in dept_names: return norm
        for pat, tgt in specials.items():
            if pat in norm or norm in pat: return tgt
        for dn in dept_names:
            if dn in norm or norm in dn: return dn
        return None

    matched = 0
    for feat in geojson["features"]:
        raw = feat["properties"].get(name_key, "")
        dept = resolve(raw)
        if feat.get("geometry") and feat["geometry"].get("coordinates"):
            feat["geometry"]["coordinates"] = simplify_coords(feat["geometry"]["coordinates"])
        p = {"name": dept or strip_accents(raw)}
        if dept and dept in stats:
            matched += 1
            s = stats[dept]
            p["n_contracts"] = si(s.get("n_contracts"))
            p["n_flagged"] = si(s.get("n_flagged"))
            p["flag_rate"] = sf(s.get("flag_rate"), 4)
            p["total_value"] = si(s.get("total_value_cop"))
            p["exposure"] = si(s.get("flagged_value_cop"))
            p["composite"] = sf(s.get("composite", 0), 3)
            for cat in CATEGORIES:
                p[f"cat_{cat}"] = sf(s.get(f"cat_{cat}", s.get("composite", 0) if cat == "all" else 0), 3)
        else:
            p.update({k: 0 for k in ["n_contracts","n_flagged","flag_rate","total_value","exposure","composite"]})
            for cat in CATEGORIES:
                p[f"cat_{cat}"] = 0
        feat["properties"] = p

    print(f"  Matched {matched}/{len(geojson['features'])} departments", flush=True)
    return geojson


# ── Build Dots + Details ───────────────────────────────────────────

def _trim_cards(raw_cards):
    """Trim context cards for JSON output."""
    if not raw_cards:
        return None
    cards = []
    for card in raw_cards:
        c_slim = {
            "type": card.get("type"),
            "confidence": card.get("confidence"),
            "headline": str(card.get("headline", ""))[:120],
            "explanation": str(card.get("explanation", ""))[:250],
            "affected_signals": card.get("affected_signals", []),
        }
        if card.get("members"):
            c_slim["members"] = card["members"][:5]
        cards.append(c_slim)
    return cards or None


def build_contracts(scored, refpop, demo, muni_lookup,
                    context_cards=None, muni_covariates=None):
    """Build slim dots for ALL contracts and full details."""

    if context_cards is None:
        context_cards = {}
    if muni_covariates is None:
        muni_covariates = {}

    # Merge scored with refpop for names / descriptions
    rp_cols = ["contract_id", "entity_name", "supplier_name", "department_norm",
               "municipality_norm", "object_description", "status_raw",
               "source_record_uri"]
    if "codigo_divipola" in refpop.columns:
        rp_cols.append("codigo_divipola")
    # Drop columns already in scored to avoid _x/_y suffix conflicts
    rp_cols = [c for c in rp_cols if c not in scored.columns or c == "contract_id"]
    merged = scored.merge(refpop[rp_cols], on="contract_id", how="left")

    # Real coords from demo cohort
    real_coords = {}
    for _, r in demo.iterrows():
        if pd.notna(r.get("lat")) and pd.notna(r.get("lon")):
            real_coords[r["contract_id"]] = (float(r["lat"]), float(r["lon"]))

    # ── Pre-computation ──
    merged["dept_str"] = merged["department_norm"].fillna("")
    merged["muni_str"] = merged["municipality_norm"].fillna("")

    # ── Pre-compute category scores ──
    cat_signals = {
        "execution":     ["z_stall", "z_slip_contract"],
        "competition":   ["z_single_bidder_entity", "z_bunching_entity", "z_award_speed_abs"],
        "pricing":       ["z_creep_contract", "z_creep_contractor", "z_fragmentation"],
        "relationships": ["z_hhi_entity", "z_relationship"],
    }
    for cat_name, cols in cat_signals.items():
        present = [c for c in cols if c in merged.columns]
        if present:
            merged[f"_cat_{cat_name}"] = merged[present].fillna(0).mean(axis=1)
        else:
            merged[f"_cat_{cat_name}"] = 0.0

    # Extract year from signature date if available
    sig_date_col = None
    for col_name in ["signature_date", "fecha_de_firma", "start_date"]:
        if col_name in merged.columns:
            sig_date_col = col_name
            break
    if sig_date_col:
        merged["_year"] = pd.to_datetime(merged[sig_date_col], errors="coerce").dt.year.fillna(0).astype(int)
    else:
        merged["_year"] = 0

    # ── Dots using numpy arrays (no iterrows) ──
    # Use adjusted composite for map coloring
    cids = merged["contract_id"].values
    depts = merged["dept_str"].values
    munis = merged["muni_str"].values
    composites_adj = merged["composite_adjusted"].fillna(merged["composite"]).values
    award_values = merged["awarded_value_cop"].values
    entities = merged["entity_name"].fillna("").str[:60].values
    dq_arr = merged["dq_excluded"].fillna(False).values
    years = merged["_year"].values
    cat_exec = merged["_cat_execution"].values
    cat_comp = merged["_cat_competition"].values
    cat_price = merged["_cat_pricing"].values
    cat_rel = merged["_cat_relationships"].values

    dots = []
    for i in range(len(cids)):
        dept = depts[i]
        if not dept:
            continue
        cid = cids[i]
        muni = munis[i]

        if cid in real_coords:
            lat, lon = real_coords[cid]
        elif (dept, muni) in muni_lookup:
            base_lat, base_lon = muni_lookup[(dept, muni)]
            lat, lon = jitter_coords(cid, base_lat, base_lon, dept="QUINDIO")
        elif dept in DEPT_CENTROIDS:
            lat, lon = jitter_coords(cid, *DEPT_CENTROIDS[dept], dept=dept)
        else:
            continue

        dots.append({
            "i": cid,
            "a": round(lat, 4),
            "o": round(lon, 4),
            "c": sf(composites_adj[i]),
            "v": si(award_values[i]),
            "d": dept,
            "m": muni,
            "e": entities[i],
            "q": 1 if bool(dq_arr[i]) else 0,
            "y": int(years[i]),
            "cx": sf(cat_exec[i], 2),
            "cc": sf(cat_comp[i], 2),
            "cp": sf(cat_price[i], 2),
            "cr": sf(cat_rel[i], 2),
        })

    print(f"  Dots: {len(dots):,} contracts with coordinates", flush=True)

    # Build a slim merged for details streaming (drop heavy columns)
    detail_cols = (
        ["contract_id", "entity_nit", "supplier_id",
         "dept_str", "muni_str", "awarded_value_cop",
         "composite", "composite_percentile", "composite_adjusted",
         "composite_adjusted_percentile", "cohort_key",
         "is_mandato", "is_eice", "dq_excluded", "dq_flags",
         "s1_not_evaluated", "ranking_unstable"]
        + list(Z_MAP.keys()) + list(Z_GLOBAL_MAP.keys())
    )
    # Add text columns from refpop
    for col in ["entity_name", "supplier_name", "object_description",
                "status_raw", "source_record_uri", "codigo_divipola"]:
        if col in merged.columns:
            detail_cols.append(col)
    detail_cols = [c for c in detail_cols if c in merged.columns]
    merged_slim = merged[detail_cols].copy()

    return dots, merged_slim, context_cards, muni_covariates


def write_details_json(out_path, merged, context_cards, muni_covariates):
    """Write details.json by streaming, avoiding huge in-memory dict."""
    z_cols = list(Z_MAP.keys())
    z_global_cols = list(Z_GLOBAL_MAP.keys())

    # Pre-fill string columns to avoid per-row NaN checks
    for col in ["entity_name", "supplier_name", "object_description",
                "status_raw", "source_record_uri", "dq_flags"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna("").astype(str)

    def _get(row, col, default=""):
        return getattr(row, col, default) if hasattr(row, col) else default

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("{")
        first = True
        for idx, row in enumerate(merged.itertuples(index=False)):
            cid = row.contract_id
            dept = row.dept_str
            cohort_key = row.cohort_key if pd.notna(row.cohort_key) else "competitive"
            exempt = COHORT_EXEMPTIONS.get(cohort_key, [])

            z = {}
            for col in z_cols:
                val = getattr(row, col, 0)
                z[Z_MAP[col]] = sf(val)
            z_global = {}
            for col in z_global_cols:
                val = getattr(row, col, 0)
                z_global[Z_GLOBAL_MAP[col]] = sf(val)

            detail = {
                "id": cid,
                "entity": _get(row, "entity_name")[:100],
                "entity_nit": _get(row, "entity_nit"),
                "supplier": _get(row, "supplier_name")[:100],
                "supplier_nit": _get(row, "supplier_id"),
                "dept": dept,
                "muni": row.muni_str[:30],
                "value": si(row.awarded_value_cop),
                "status": _get(row, "status_raw")[:20],
                "composite": sf(row.composite),
                "pctl": sf(row.composite_percentile, 3),
                "z": z,
                "z_global": z_global,
                "cohort": cohort_key,
                "is_mandato": bool(row.is_mandato),
                "is_eice": bool(row.is_eice),
                "exempt": exempt,
                "desc": _get(row, "object_description")[:300],
                "signals": top_signals_text(z),
                "url": _get(row, "source_record_uri")[:200],
                "dq_excluded": bool(row.dq_excluded),
                "dq_flags": _get(row, "dq_flags"),
                "s1_not_eval": bool(getattr(row, "s1_not_evaluated", False)),
                "ranking_unstable": bool(getattr(row, "ranking_unstable", False)),
            }

            # Context enrichment (only when present)
            comp_adj = sf(getattr(row, "composite_adjusted", row.composite))
            if comp_adj != detail["composite"]:
                detail["composite_adj"] = comp_adj
                detail["pctl_adj"] = sf(getattr(row, "composite_adjusted_percentile", row.composite_percentile), 3)
            cards = _trim_cards(context_cards.get(cid))
            if cards:
                detail["cards"] = cards
            divipola = getattr(row, "codigo_divipola", None)
            if divipola and pd.notna(divipola) and str(divipola) in muni_covariates:
                detail["ctx"] = muni_covariates[str(divipola)]

            if not first:
                f.write(",")
            f.write(f'"{cid}":')
            f.write(json.dumps(detail, cls=NpEncoder, ensure_ascii=False))
            first = False

            if (idx + 1) % 5000 == 0:
                print(f"    {idx + 1:,}/{len(merged):,} details written...", flush=True)

        f.write("}")
    print(f"  Written {out_path} ({out_path.stat().st_size // 1024} KB)", flush=True)


def prepare_contractors(dk):
    return [{
        "id": r["supplier_id"],
        "name": str(r.get("supplier_name", ""))[:100],
        "composite": sf(r["portfolio_composite"]),
        "n": int(r["n_contracts_active"]),
        "exposure": si(r["total_exposure_cop"]),
        "flagged": int(r["n_flagged"]),
        "signals": str(r.get("top_2_signals", "")),
    } for _, r in dk.iterrows()]


def prepare_departments(dept_stats):
    return sorted([{
        "name": r["department_norm"],
        "n_contracts": si(r["n_contracts"]),
        "n_flagged": si(r["n_flagged"]),
        "flag_rate": sf(r["flag_rate"], 4),
        "exposure": si(r["flagged_value_cop"]),
        "composite": sf(r.get("composite", 0), 3),
    } for _, r in dept_stats.iterrows()], key=lambda x: x["n_flagged"], reverse=True)


# ── Main ───────────────────────────────────────────────────────────

def main():
    import sys
    do_geocode = "--geocode" in sys.argv

    print("=" * 50, flush=True)
    print("BUILD DASHBOARD DATA", flush=True)
    print("=" * 50, flush=True)

    refpop = pd.read_parquet(DATA_DIR / "reference_population.parquet")
    scored = pd.read_parquet(DATA_DIR / "anomaly_scored.parquet")
    demo = pd.read_parquet(DATA_DIR / "demo_cohort.parquet")
    contractors = pd.read_parquet(DATA_DIR / "demo_contractors.parquet")
    print(f"  Loaded {len(refpop):,} refpop, {len(demo)} demo, {len(contractors)} contractors", flush=True)

    # Load context cards (graceful if absent)
    context_cards: dict[str, list[dict]] = {}
    cards_path = DATA_DIR / "context_cards.json"
    if cards_path.exists():
        with open(cards_path, encoding="utf-8") as f:
            context_cards = json.load(f)
        print(f"  Loaded context cards for {len(context_cards):,} contracts", flush=True)

    # Load municipality covariates (graceful if absent)
    muni_covariates: dict[str, dict] = {}
    cov_path = DATA_DIR / "covariates" / "municipality_covariates.parquet"
    if cov_path.exists():
        cov_df = pd.read_parquet(cov_path)
        for _, r in cov_df.iterrows():
            code = str(r["codigo_divipola"])
            muni_covariates[code] = {
                "is_pdet": bool(r.get("is_pdet", False)),
                "is_zomac": bool(r.get("is_zomac", False)),
                "fiscal_cat": si(r.get("fiscal_category")),
                "pop": si(r.get("total_population", 0)),
                "rurality": sf(r.get("rurality_ratio", 0), 3),
                "dist_capital_km": sf(r.get("distance_to_capital_km", 0), 1),
            }
        print(f"  Loaded covariates for {len(muni_covariates)} municipalities", flush=True)

    # Load bootstrap stability (graceful if absent)
    bootstrap_path = DATA_DIR / "bootstrap_stability.parquet"
    if bootstrap_path.exists():
        bootstrap = pd.read_parquet(bootstrap_path)
        scored = scored.merge(
            bootstrap[["contract_id", "ranking_unstable"]],
            on="contract_id", how="left",
        )
        scored["ranking_unstable"] = scored["ranking_unstable"].fillna(False)
        print(f"  Loaded bootstrap stability for {len(bootstrap):,} contracts", flush=True)
    else:
        scored["ranking_unstable"] = False

    dept_stats = compute_dept_stats(refpop, scored, demo)
    geojson = enrich_geojson(load_geojson(), dept_stats)

    muni_lookup = build_municipality_lookup(refpop, geocode=do_geocode)
    print(f"  {len(muni_lookup)} municipalities with coords" +
          (" (pass --geocode to fetch missing)" if not do_geocode else ""), flush=True)

    dots, merged, cc, mc = build_contracts(
        scored, refpop, demo, muni_lookup,
        context_cards=context_cards,
        muni_covariates=muni_covariates,
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Write details to separate file (streamed to avoid OOM)
    print("  Writing details.json (streaming)...", flush=True)
    write_details_json(OUT_DIR / "details.json", merged, cc, mc)

    # Build slim contracts list for table from scored DataFrame directly
    print("  Building contracts list...", flush=True)
    clean_mask = ~scored["dq_excluded"].fillna(False)
    n_dq = (~clean_mask).sum()
    n_clean = clean_mask.sum()
    n_flagged = int((scored.loc[clean_mask, "composite_percentile"] >= 0.9).sum())
    exposure = int(scored.loc[clean_mask & (scored["composite_percentile"] >= 0.9), "awarded_value_cop"].sum())

    # Compute n_context_shifted from scored directly
    raw_pctl = scored.loc[clean_mask, "composite_percentile"].fillna(0)
    adj_pctl = scored.loc[clean_mask, "composite_adjusted_percentile"].fillna(raw_pctl)
    n_context_shifted = int((abs(raw_pctl - adj_pctl) > 0.10).sum())

    # Slim contracts list — build from merged_slim using itertuples (fast)
    rp_lookup = refpop.set_index("contract_id")[
        ["entity_name", "supplier_name", "department_norm", "municipality_norm"]
    ].fillna("").to_dict("index")

    slim_cols = ["contract_id", "supplier_id", "composite", "composite_percentile",
                 "composite_adjusted", "composite_adjusted_percentile",
                 "cohort_key", "dq_excluded", "dq_flags", "awarded_value_cop",
                 "s1_not_evaluated", "ranking_unstable"]
    z_sig_cols = list(Z_MAP.keys())
    slim_df = scored[slim_cols + z_sig_cols].copy()
    slim_df["dq_flags"] = slim_df["dq_flags"].fillna("")
    slim_df["cohort_key"] = slim_df["cohort_key"].fillna("competitive")

    contracts_list = []
    for row in slim_df.itertuples(index=False):
        cid = row.contract_id
        rp = rp_lookup.get(cid, {})
        z = {Z_MAP[col]: sf(getattr(row, col, 0)) for col in z_sig_cols}
        c = {
            "id": cid,
            "entity": str(rp.get("entity_name", ""))[:100],
            "supplier": str(rp.get("supplier_name", ""))[:100],
            "supplier_nit": row.supplier_id,
            "dept": str(rp.get("department_norm", "")),
            "muni": str(rp.get("municipality_norm", ""))[:30],
            "value": si(row.awarded_value_cop),
            "status": "",
            "composite": sf(row.composite),
            "pctl": sf(row.composite_percentile, 3),
            "signals": top_signals_text(z),
            "cohort": row.cohort_key,
            "dq_excluded": bool(row.dq_excluded),
            "dq_flags": row.dq_flags,
            "s1_not_eval": bool(getattr(row, "s1_not_evaluated", False)),
            "ranking_unstable": bool(getattr(row, "ranking_unstable", False)),
        }
        comp_adj = sf(row.composite_adjusted)
        if comp_adj != c["composite"]:
            c["composite_adj"] = comp_adj
            c["pctl_adj"] = sf(row.composite_adjusted_percentile, 3)
        contracts_list.append(c)

    contracts_list.sort(key=lambda x: x["composite"], reverse=True)
    print(f"  Contracts list: {len(contracts_list):,}", flush=True)

    # ── Enrich municipality GeoJSON with per-muni stats ──
    muni_geojson = None
    muni_geo_path = DATA_DIR / "colombia_municipalities.geojson"
    if muni_geo_path.exists():
        with open(muni_geo_path) as f:
            muni_geojson = json.load(f)
        # Build (dept, muni) → stats from dots
        from collections import defaultdict
        muni_stats: dict[tuple[str, str], dict] = defaultdict(lambda: {"n": 0, "n_flagged": 0, "total_v": 0, "sum_c": 0.0})
        pctl_threshold = 0.9
        pctl_lookup = scored.set_index("contract_id")["composite_percentile"].to_dict()
        for dot in dots:
            if dot["q"] == 1:
                continue
            key = (dot["d"], dot.get("m", ""))
            s = muni_stats[key]
            s["n"] += 1
            s["total_v"] += dot["v"]
            s["sum_c"] += dot["c"]
            cid_pctl = pctl_lookup.get(dot["i"], 0)
            if cid_pctl >= pctl_threshold:
                s["n_flagged"] += 1

        # Normalize department names in muni GeoJSON to match dot data
        MUNI_DEPT_ALIASES = {
            "SANTAFE DE BOGOTA D.C": "DISTRITO CAPITAL DE BOGOTA",
            "SANTAFE DE BOGOTA D.C.": "DISTRITO CAPITAL DE BOGOTA",
            "BOGOTA D.C.": "DISTRITO CAPITAL DE BOGOTA",
            "BOGOTA": "DISTRITO CAPITAL DE BOGOTA",
            "ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA": "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA",
            "ARCHIPIELAGO DE SAN ANDRES, PROVIDENCIA Y SANTA CATALINA": "SAN ANDRES, PROVIDENCIA Y SANTA CATALINA",
        }

        # Also map muni names that differ between GeoJSON and refpop
        MUNI_NAME_ALIASES = {
            "SANTAFE DE BOGOTA D.C.": ["BOGOTA", "DISTRITO CAPITAL"],
        }

        matched = 0
        for feat in muni_geojson["features"]:
            p = feat["properties"]
            dept_name = MUNI_DEPT_ALIASES.get(p["dept"], p["dept"])
            p["dept"] = dept_name  # normalize in-place for frontend matching
            # Try original name, then aliases
            candidates = [p["muni"]] + MUNI_NAME_ALIASES.get(p["muni"], [])
            s = None
            for cand in candidates:
                s = muni_stats.get((dept_name, cand))
                if s and s["n"] > 0:
                    break
            # Merge multiple aliases into one stat
            if not s or s["n"] == 0:
                merged_s = {"n": 0, "n_flagged": 0, "total_v": 0, "sum_c": 0.0}
                for cand in candidates:
                    cs = muni_stats.get((dept_name, cand))
                    if cs:
                        merged_s["n"] += cs["n"]
                        merged_s["n_flagged"] += cs["n_flagged"]
                        merged_s["total_v"] += cs["total_v"]
                        merged_s["sum_c"] += cs["sum_c"]
                if merged_s["n"] > 0:
                    s = merged_s
            if s and s["n"] > 0:
                matched += 1
                p["n"] = s["n"]
                p["n_flagged"] = s["n_flagged"]
                p["flag_rate"] = round(s["n_flagged"] / s["n"], 4) if s["n"] > 0 else 0
                p["total_v"] = s["total_v"]
                p["avg_c"] = round(s["sum_c"] / s["n"], 3)
            else:
                p["n"] = 0
                p["n_flagged"] = 0
                p["flag_rate"] = 0
                p["total_v"] = 0
                p["avg_c"] = 0

        print(f"  Municipality GeoJSON: matched {matched}/{len(muni_geojson['features'])} features", flush=True)
        # Write to public dir
        muni_out = OUT_DIR / "municipalities.geojson"
        with open(muni_out, "w") as f:
            json.dump(muni_geojson, f)
        print(f"  Written {muni_out} ({muni_out.stat().st_size // 1024} KB)", flush=True)
    else:
        print(f"  Municipality GeoJSON not found at {muni_geo_path}, skipping", flush=True)

    data = {
        "kpi": {
            "n_analyzed": int(n_clean),
            "n_flagged": n_flagged,
            "exposure_cop": exposure,
            "n_dq_excluded": int(n_dq),
            "n_total": len(contracts_list),
            "n_context_shifted": n_context_shifted,
        },
        "departments": prepare_departments(dept_stats),
        "geojson": geojson,
        "dots": dots,
        "contracts": contracts_list,
        "contractors": prepare_contractors(contractors),
        "methodology": Path("METHODOLOGY.md").read_text(encoding="utf-8"),
    }

    out_path = OUT_DIR / "data.json"
    payload = json.dumps(data, cls=NpEncoder, ensure_ascii=False)
    out_path.write_text(payload, encoding="utf-8")
    print(f"  Written {out_path} ({len(payload)//1024} KB)", flush=True)


if __name__ == "__main__":
    main()
