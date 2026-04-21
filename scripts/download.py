#!/usr/bin/env python3
"""
Download SECOP II datasets from datos.gov.co (Socrata SODA API).

Downloads year-by-year (2019-2024) with 50K-row pages.
Three datasets in parallel, sequential paging within each.
Idempotent: skips batches whose manifest already exists.
"""
import argparse
import json
import hashlib
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.datos.gov.co/resource"
RAW_DIR = Path("data/raw")
PAGE_SIZE = 50_000
YEARS = range(2019, 2025)

DATASETS = {
    "secop2_procesos": {
        "id": "p6dx-8zbt",
        "date_field": "fecha_de_publicacion_del",
        "order_field": "fecha_de_publicacion_del,id_del_proceso",
    },
    "secop2_contratos": {
        "id": "jbjy-vk9h",
        "date_field": "fecha_de_firma",
        "order_field": "fecha_de_firma,id_contrato",
    },
    "secop2_adiciones": {
        "id": "cb9c-h8sn",
        "date_field": "fecharegistro",
        "order_field": "fecharegistro,identificador",
    },
}


def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=8,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=4)
    session.mount("https://", adapter)
    return session


def download_dataset(name: str, cfg: dict, years: range) -> dict:
    dataset_id = cfg["id"]
    date_field = cfg["date_field"]
    order_field = cfg["order_field"]
    out_dir = RAW_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)

    session = create_session()
    url = f"{BASE_URL}/{dataset_id}.json"

    total_records = 0
    total_bytes = 0
    stats_per_year = {}

    for year in years:
        date_from = f"{year}-01-01T00:00:00"
        date_to = f"{year + 1}-01-01T00:00:00"
        where = f"{date_field} >= '{date_from}' AND {date_field} < '{date_to}'"
        year_records = 0
        offset = 0

        while True:
            batch_name = f"batch_{year}_{offset:08d}"
            batch_file = out_dir / f"{batch_name}.json"
            manifest_file = out_dir / f"{batch_name}.manifest.json"

            # Resume: skip completed batches
            if manifest_file.exists():
                m = json.loads(manifest_file.read_text())
                n = m["record_count"]
                year_records += n
                total_records += n
                total_bytes += m["byte_count"]
                if n < PAGE_SIZE:
                    break  # last batch of this year
                offset += PAGE_SIZE
                continue

            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$where": where,
                "$order": order_field,
            }

            t0 = time.monotonic()
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    resp = session.get(url, params=params, timeout=600)
                    resp.raise_for_status()
                    break
                except requests.exceptions.RequestException as exc:
                    if attempt == max_retries - 1:
                        print(f"[{name}] FATAL {year} offset={offset}: {exc}", file=sys.stderr)
                        raise
                    wait = min(2 ** attempt * 5, 120)
                    print(
                        f"[{name}] RETRY {attempt+1}/{max_retries} {year} offset={offset}: {exc} — waiting {wait}s",
                        flush=True,
                    )
                    time.sleep(wait)
                    session = create_session()  # fresh connection pool

            elapsed = time.monotonic() - t0
            raw_bytes = resp.content
            data = resp.json()
            n = len(data)

            # Save batch
            batch_file.write_bytes(raw_bytes)

            # Save manifest
            sha = hashlib.sha256(raw_bytes).hexdigest()
            manifest = {
                "url": str(resp.url),
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "byte_count": len(raw_bytes),
                "sha256": sha,
                "record_count": n,
                "dataset": name,
                "year": year,
                "offset": offset,
            }
            manifest_file.write_text(json.dumps(manifest, indent=2))

            year_records += n
            total_records += n
            total_bytes += len(raw_bytes)

            print(
                f"[{name}] {year} offset={offset:>8d}: "
                f"{n:>6,} rows  {len(raw_bytes)/1e6:>6.1f}MB  "
                f"{elapsed:>5.1f}s  cumul={total_records:>10,}",
                flush=True,
            )

            if n < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            time.sleep(0.3)

        stats_per_year[year] = year_records
        print(f"[{name}] year {year} done: {year_records:,} records", flush=True)

    print(
        f"[{name}] COMPLETE: {total_records:,} records, "
        f"{total_bytes/1e9:.1f} GB",
        flush=True,
    )
    return {"name": name, "total_records": total_records, "total_bytes": total_bytes, "per_year": stats_per_year}


def main():
    parser = argparse.ArgumentParser(description="Download SECOP II data")
    parser.add_argument("--dataset", choices=list(DATASETS.keys()), help="Download only this dataset")
    parser.add_argument("--year", type=int, help="Download only this year")
    args = parser.parse_args()

    datasets = {args.dataset: DATASETS[args.dataset]} if args.dataset else DATASETS
    years = range(args.year, args.year + 1) if args.year else YEARS

    print(f"Downloading {len(datasets)} dataset(s) for years {list(years)}")
    print(f"Page size: {PAGE_SIZE:,}, output dir: {RAW_DIR}")
    print()

    results = {}

    if len(datasets) == 1:
        name, cfg = next(iter(datasets.items()))
        results[name] = download_dataset(name, cfg, years)
    else:
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(download_dataset, name, cfg, years): name
                for name, cfg in datasets.items()
            }
            for fut in as_completed(futures):
                name = futures[fut]
                try:
                    results[name] = fut.result()
                except Exception as exc:
                    print(f"[{name}] FAILED: {exc}", file=sys.stderr)
                    raise

    print("\n" + "=" * 60)
    print("DOWNLOAD SUMMARY")
    print("=" * 60)
    for name, r in results.items():
        print(f"  {name}: {r['total_records']:,} records, {r['total_bytes']/1e9:.1f} GB")
        for y, c in sorted(r["per_year"].items()):
            print(f"    {y}: {c:>10,}")


if __name__ == "__main__":
    main()
