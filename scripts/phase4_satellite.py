"""Phase 4 — Satellite Stack Pull via Google Earth Engine.

Pulls Sentinel-1 GRD and Sentinel-2 L2A monthly stacks for each
geolocated site. Stores per-site .npz files with checkpointing.

Prerequisites:
  uv pip install earthengine-api numpy
  earthengine authenticate --project=YOUR_PROJECT_ID

Usage:
  uv run python scripts/phase4_satellite.py --project=YOUR_PROJECT_ID
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import ee
except ImportError:
    print("earthengine-api not installed. Run: uv pip install earthengine-api")
    sys.exit(1)

DATA_DIR = Path("data")
STACKS_DIR = DATA_DIR / "stacks"
ERROR_LOG = STACKS_DIR / "fetch_errors.jsonl"

BUFFER_M = 200  # 200m buffer → 400m square chip
S2_BANDS = ["B2", "B3", "B4", "B8"]  # Blue, Green, Red, NIR
S2_SCL_VALID = [4, 5, 6]  # Vegetation, bare soil, water
CHIP_PX = 20  # 20x20 pixels at 10m resolution


def init_ee(project: str):
    """Initialize Earth Engine."""
    ee.Initialize(project=project)
    print(f"GEE initialized (project={project})", flush=True)


def get_aoi(lat: float, lon: float) -> ee.Geometry:
    """Create a 400m square AOI centered on the point."""
    point = ee.Geometry.Point([lon, lat])
    return point.buffer(BUFFER_M).bounds()


def get_date_range(row: pd.Series) -> tuple[str, str]:
    """Get date range: 6 months before signature through today."""
    sig = row["contract_signature_date"]
    if pd.isna(sig):
        sig = pd.Timestamp("2022-01-01", tz="UTC")
    start = (sig - pd.DateOffset(months=6)).strftime("%Y-%m-%d")
    end = "2026-04-01"
    return start, end


def pull_s1_stack(aoi: ee.Geometry, start: str, end: str) -> dict:
    """Pull monthly Sentinel-1 GRD VV/VH stack."""
    s1 = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filterBounds(aoi)
        .filterDate(start, end)
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VH"))
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .select(["VV", "VH"])
    )

    # Determine dominant orbit pass
    asc_count = s1.filter(ee.Filter.eq("orbitProperties_pass", "ASCENDING")).size().getInfo()
    desc_count = s1.filter(ee.Filter.eq("orbitProperties_pass", "DESCENDING")).size().getInfo()
    orbit = "ASCENDING" if asc_count >= desc_count else "DESCENDING"
    s1 = s1.filter(ee.Filter.eq("orbitProperties_pass", orbit))

    # Monthly median composites
    start_date = ee.Date(start)
    end_date = ee.Date(end)
    n_months = end_date.difference(start_date, "month").round().getInfo()

    dates = []
    vv_stack = []
    vh_stack = []

    for i in range(int(n_months)):
        m_start = start_date.advance(i, "month")
        m_end = start_date.advance(i + 1, "month")
        monthly = s1.filterDate(m_start, m_end)

        count = monthly.size().getInfo()
        if count == 0:
            continue

        composite = monthly.median().clip(aoi)

        # Sample as array
        arr = composite.sampleRectangle(region=aoi, defaultValue=0)
        try:
            vv = np.array(arr.get("VV").getInfo(), dtype=np.float32)
            vh = np.array(arr.get("VH").getInfo(), dtype=np.float32)
        except Exception:
            continue

        # Pad/crop to CHIP_PX x CHIP_PX
        vv = _resize_chip(vv)
        vh = _resize_chip(vh)

        dates.append(m_start.format("YYYY-MM").getInfo())
        vv_stack.append(vv)
        vh_stack.append(vh)

    return {
        "s1_vv": np.array(vv_stack, dtype=np.float32) if vv_stack else np.empty((0, CHIP_PX, CHIP_PX), dtype=np.float32),
        "s1_vh": np.array(vh_stack, dtype=np.float32) if vh_stack else np.empty((0, CHIP_PX, CHIP_PX), dtype=np.float32),
        "s1_dates": dates,
        "s1_orbit": orbit,
    }


def pull_s2_stack(aoi: ee.Geometry, start: str, end: str) -> dict:
    """Pull monthly Sentinel-2 L2A stack with SCL cloud masking."""
    s2 = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(aoi)
        .filterDate(start, end)
        .select(S2_BANDS + ["SCL"])
    )

    start_date = ee.Date(start)
    end_date = ee.Date(end)
    n_months = end_date.difference(start_date, "month").round().getInfo()

    dates = []
    rgb_stack = []
    nir_stack = []
    valid_frac_list = []

    for i in range(int(n_months)):
        m_start = start_date.advance(i, "month")
        m_end = start_date.advance(i + 1, "month")
        monthly = s2.filterDate(m_start, m_end)

        count = monthly.size().getInfo()
        if count == 0:
            dates.append(m_start.format("YYYY-MM").getInfo())
            rgb_stack.append(np.full((CHIP_PX, CHIP_PX, 3), np.nan, dtype=np.float32))
            nir_stack.append(np.full((CHIP_PX, CHIP_PX), np.nan, dtype=np.float32))
            valid_frac_list.append(0.0)
            continue

        # Apply SCL cloud mask
        def mask_clouds(img):
            scl = img.select("SCL")
            mask = scl.eq(4).Or(scl.eq(5)).Or(scl.eq(6))
            return img.updateMask(mask)

        masked = monthly.map(mask_clouds)
        composite = masked.select(S2_BANDS).median().clip(aoi)

        # Valid fraction
        valid_count = masked.select("B2").count().clip(aoi)
        total_count = monthly.select("B2").count().clip(aoi)

        try:
            arr = composite.sampleRectangle(region=aoi, defaultValue=0)
            b2 = np.array(arr.get("B2").getInfo(), dtype=np.uint16)
            b3 = np.array(arr.get("B3").getInfo(), dtype=np.uint16)
            b4 = np.array(arr.get("B4").getInfo(), dtype=np.uint16)
            b8 = np.array(arr.get("B8").getInfo(), dtype=np.uint16)

            # Valid fraction
            vf_arr = valid_count.sampleRectangle(region=aoi, defaultValue=0)
            tf_arr = total_count.sampleRectangle(region=aoi, defaultValue=1)
            vf = np.array(vf_arr.get("B2").getInfo(), dtype=np.float32)
            tf = np.array(tf_arr.get("B2").getInfo(), dtype=np.float32)
            tf = np.where(tf == 0, 1, tf)
            valid_frac = float(np.mean(vf / tf))
        except Exception:
            dates.append(m_start.format("YYYY-MM").getInfo())
            rgb_stack.append(np.full((CHIP_PX, CHIP_PX, 3), np.nan, dtype=np.float32))
            nir_stack.append(np.full((CHIP_PX, CHIP_PX), np.nan, dtype=np.float32))
            valid_frac_list.append(0.0)
            continue

        rgb = np.stack([_resize_chip(b4), _resize_chip(b3), _resize_chip(b2)], axis=-1)  # R,G,B order
        nir = _resize_chip(b8)

        dates.append(m_start.format("YYYY-MM").getInfo())
        rgb_stack.append(rgb.astype(np.uint16))
        nir_stack.append(nir.astype(np.uint16))
        valid_frac_list.append(valid_frac)

    return {
        "s2_rgb": np.array(rgb_stack, dtype=np.uint16) if rgb_stack else np.empty((0, CHIP_PX, CHIP_PX, 3), dtype=np.uint16),
        "s2_nir": np.array(nir_stack, dtype=np.uint16) if nir_stack else np.empty((0, CHIP_PX, CHIP_PX), dtype=np.uint16),
        "s2_dates": dates,
        "s2_valid_frac": np.array(valid_frac_list, dtype=np.float32),
    }


def _resize_chip(arr: np.ndarray) -> np.ndarray:
    """Pad or crop a 2D array to CHIP_PX x CHIP_PX."""
    if arr.ndim != 2:
        return np.zeros((CHIP_PX, CHIP_PX), dtype=arr.dtype)
    h, w = arr.shape
    out = np.zeros((CHIP_PX, CHIP_PX), dtype=arr.dtype)
    copy_h = min(h, CHIP_PX)
    copy_w = min(w, CHIP_PX)
    out[:copy_h, :copy_w] = arr[:copy_h, :copy_w]
    return out


def log_error(contract_id: str, date_range: str, error: str):
    """Append error to fetch_errors.jsonl."""
    entry = {
        "contract_id": contract_id,
        "date_range": date_range,
        "error": str(error),
        "timestamp": datetime.utcnow().isoformat(),
    }
    with open(ERROR_LOG, "a") as f:
        f.write(json.dumps(entry) + "\n")


def process_site(row: pd.Series) -> dict:
    """Pull satellite stacks for a single site."""
    cid = row["contract_id"]
    lat, lon = row["lat"], row["lon"]
    start, end = get_date_range(row)

    out_path = STACKS_DIR / f"site_{cid}.npz"

    # Checkpoint: skip if already done
    if out_path.exists():
        try:
            data = np.load(out_path)
            if "s1_vv" in data and "s2_rgb" in data:
                return {"contract_id": cid, "status": "skipped", "s1_months": len(data["s1_vv"]), "s2_months": len(data["s2_rgb"])}
        except Exception:
            pass  # Corrupted, re-download

    aoi = get_aoi(lat, lon)

    # Pull Sentinel-1
    try:
        s1 = pull_s1_stack(aoi, start, end)
    except Exception as e:
        log_error(cid, f"{start}/{end}", f"S1: {e}")
        s1 = {"s1_vv": np.empty((0, CHIP_PX, CHIP_PX), dtype=np.float32),
              "s1_vh": np.empty((0, CHIP_PX, CHIP_PX), dtype=np.float32),
              "s1_dates": [], "s1_orbit": "UNKNOWN"}

    # Pull Sentinel-2
    try:
        s2 = pull_s2_stack(aoi, start, end)
    except Exception as e:
        log_error(cid, f"{start}/{end}", f"S2: {e}")
        s2 = {"s2_rgb": np.empty((0, CHIP_PX, CHIP_PX, 3), dtype=np.uint16),
              "s2_nir": np.empty((0, CHIP_PX, CHIP_PX), dtype=np.uint16),
              "s2_dates": [], "s2_valid_frac": np.array([], dtype=np.float32)}

    # Unify dates (use S1 dates as primary since SAR is weather-independent)
    all_dates = sorted(set(s1["s1_dates"]) | set(s2["s2_dates"]))

    # Save
    np.savez_compressed(
        out_path,
        s1_vv=s1["s1_vv"],
        s1_vh=s1["s1_vh"],
        s2_rgb=s2["s2_rgb"],
        s2_nir=s2["s2_nir"],
        dates=np.array(all_dates),
        s2_valid_frac=s2["s2_valid_frac"],
        metadata=json.dumps({
            "contract_id": cid,
            "lat": lat, "lon": lon,
            "date_range": [start, end],
            "s1_orbit": s1.get("s1_orbit", "UNKNOWN"),
            "s1_months": len(s1["s1_dates"]),
            "s2_months": len(s2["s2_dates"]),
        }),
    )

    return {
        "contract_id": cid,
        "status": "ok",
        "s1_months": len(s1["s1_dates"]),
        "s2_months": len(s2["s2_dates"]),
        "s2_mean_valid_frac": float(s2["s2_valid_frac"].mean()) if len(s2["s2_valid_frac"]) > 0 else 0.0,
    }


def main():
    parser = argparse.ArgumentParser(description="Phase 4: Satellite stack pull via GEE")
    parser.add_argument("--project", required=True, help="Google Cloud project ID for Earth Engine")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent workers (default: 4)")
    args = parser.parse_args()

    print("=" * 60, flush=True)
    print("PHASE 4 — SATELLITE STACK PULL", flush=True)
    print("=" * 60, flush=True)

    init_ee(args.project)

    STACKS_DIR.mkdir(parents=True, exist_ok=True)

    cohort = pd.read_parquet(DATA_DIR / "cohort_geolocated.parquet")
    print(f"Loaded {len(cohort)} geolocated sites", flush=True)

    # Process sites sequentially (GEE has its own rate limiting)
    # Using threads for I/O overlap but keeping it conservative
    results = []
    failed = 0

    for i, (_, row) in enumerate(cohort.iterrows()):
        cid = row["contract_id"]
        print(f"[{i + 1:3d}/{len(cohort)}] {cid} ({row['lat']:.3f}, {row['lon']:.3f})...", end=" ", flush=True)

        try:
            result = process_site(row)
            results.append(result)
            print(f"{result['status']} — S1:{result['s1_months']}mo, S2:{result['s2_months']}mo", flush=True)
        except Exception as e:
            print(f"FAILED: {e}", flush=True)
            log_error(cid, "all", str(e))
            failed += 1

    # --- Phase Report ---
    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 4 REPORT", flush=True)
    print(f"{'=' * 60}", flush=True)

    ok_results = [r for r in results if r["status"] == "ok"]
    skipped = [r for r in results if r["status"] == "skipped"]

    print(f"\nTotal sites: {len(cohort)}", flush=True)
    print(f"Successfully stacked: {len(ok_results)}", flush=True)
    print(f"Skipped (already done): {len(skipped)}", flush=True)
    print(f"Failed: {failed}", flush=True)

    if ok_results or skipped:
        all_ok = ok_results + skipped
        s1_months = [r["s1_months"] for r in all_ok]
        s2_months = [r["s2_months"] for r in all_ok]
        print(f"\nMean S1 timesteps/site: {np.mean(s1_months):.1f}", flush=True)
        print(f"Mean S2 timesteps/site: {np.mean(s2_months):.1f}", flush=True)

        if ok_results:
            valid_fracs = [r.get("s2_mean_valid_frac", 0) for r in ok_results]
            print(f"Mean S2 valid fraction: {np.mean(valid_fracs):.2f}", flush=True)
            low_valid = [r for r in ok_results if r.get("s2_mean_valid_frac", 0) < 0.3]
            if low_valid:
                print(f"\nWARNING: {len(low_valid)} sites with S2 valid fraction < 0.3:", flush=True)
                for r in low_valid:
                    print(f"  {r['contract_id']}: {r['s2_mean_valid_frac']:.2f}", flush=True)

    # Disk footprint
    total_bytes = sum(f.stat().st_size for f in STACKS_DIR.glob("site_*.npz"))
    print(f"\nTotal disk footprint: {total_bytes / 1e6:.1f} MB", flush=True)

    if ERROR_LOG.exists():
        n_errors = sum(1 for _ in open(ERROR_LOG))
        print(f"Errors logged: {n_errors} (see {ERROR_LOG})", flush=True)


if __name__ == "__main__":
    main()
