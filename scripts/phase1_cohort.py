"""Phase 1 — Cohort Selection.

Filters SECOP II contracts for large infrastructure (Obra) projects,
joins amendments for scoring, and outputs top 200 candidates.
"""

import math
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

DATA_DIR = Path("data/parquet")
OUT_DIR = Path("data")

# --- Target departments (usable optical satellite skies) ---
TARGET_DEPARTMENTS = {
    "ANTIOQUIA",
    "CUNDINAMARCA",
    "BOYACA",
    "SANTANDER",
    "ATLANTICO",
    "VALLE DEL CAUCA",
    "NORTE DE SANTANDER",
    "TOLIMA",
    "HUILA",
    "META",
}

# --- Exclusion statuses (cancelled/annulled/draft) ---
EXCLUDED_STATUSES = {"Cancelado", "Borrador"}

# --- Exclusion regex for non-construction objects ---
EXCLUDE_OBJECT_RE = r"(?i)estudios\s+y\s+dise[ñn]os|interventor[ií]a|consultor[ií]a|supervisi[oó]n|dise[ñn]o"


def read_parquet_dir(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a hive-partitioned parquet directory with mixed schemas."""
    files = sorted(path.glob("**/data.parquet"))
    tables = []
    for f in files:
        t = pq.ParquetFile(f).read(columns=columns)
        tables.append(t)
    combined = pa.concat_tables(tables, promote_options="default")
    return combined.to_pandas()


def main():
    print("=" * 60)
    print("PHASE 1 — COHORT SELECTION")
    print("=" * 60)

    # --- 1. Load contracts ---
    print("\n[1/5] Loading contracts...")
    contract_cols = [
        "contract_id", "process_id", "contract_type_raw", "status_raw",
        "department_norm", "municipality_norm", "entity_name", "entity_level",
        "object_description", "awarded_value_cop", "contract_signature_date",
        "contract_start_date", "contract_end_date", "supplier_id",
        "supplier_name", "valor_pagado", "valor_facturado",
        "valor_pendiente_pago", "dias_adicionados", "source_record_uri",
    ]
    contracts = read_parquet_dir(DATA_DIR / "contracts", columns=contract_cols)
    print(f"  Loaded {len(contracts):,} contracts")

    # --- 2. Apply filters ---
    print("\n[2/5] Filtering...")

    # Obra only
    mask = contracts["contract_type_raw"] == "Obra"
    print(f"  After Obra filter: {mask.sum():,}")

    # Value > 5B COP
    mask &= contracts["awarded_value_cop"] > 5_000_000_000
    print(f"  After value > 5B COP: {mask.sum():,}")

    # Signature date range
    sig_date = contracts["contract_signature_date"]
    mask &= (sig_date >= "2021-06-01") & (sig_date <= "2023-12-31")
    print(f"  After signature date 2021-06 to 2023-12: {mask.sum():,}")

    # Exclude cancelled/annulled
    mask &= ~contracts["status_raw"].isin(EXCLUDED_STATUSES)
    print(f"  After excluding {EXCLUDED_STATUSES}: {mask.sum():,}")

    # Target departments
    mask &= contracts["department_norm"].isin(TARGET_DEPARTMENTS)
    print(f"  After department filter: {mask.sum():,}")

    # Exclude non-construction objects
    obj_exclude = contracts["object_description"].str.contains(
        EXCLUDE_OBJECT_RE, regex=True, na=False
    )
    mask &= ~obj_exclude
    print(f"  After excluding design/consultancy objects: {mask.sum():,}")

    cohort = contracts[mask].copy()
    print(f"\n  Filtered cohort: {len(cohort):,} contracts")

    # --- 3. Join amendments ---
    print("\n[3/5] Loading and joining amendments...")
    amendment_cols = ["contract_id", "amendment_type_raw", "amendment_type_norm"]
    amendments = read_parquet_dir(DATA_DIR / "amendments", columns=amendment_cols)
    print(f"  Loaded {len(amendments):,} amendments total")

    # Filter to cohort contract IDs
    cohort_ids = set(cohort["contract_id"])
    amend_cohort = amendments[amendments["contract_id"].isin(cohort_ids)].copy()
    print(f"  Amendments matching cohort: {len(amend_cohort):,}")

    # Compute per-contract amendment stats
    # Normalized amendment types: MODIFICACION_GENERAL, CONCLUSION, NO_DEFINIDO,
    # ADICION_VALOR, REACTIVACION, SUSPENSION, CESION
    amend_stats = amend_cohort.groupby("contract_id").agg(
        n_amendments=("amendment_type_raw", "count"),
        had_suspension=("amendment_type_norm", lambda x: (x == "SUSPENSION").any()),
        n_extensions=("amendment_type_norm", lambda x: 0),  # No separate EXTENSION type in data
        n_value_additions=("amendment_type_norm", lambda x: (x == "ADICION_VALOR").sum()),
        n_value_reductions=("amendment_type_norm", lambda x: 0),  # Not present in data
    ).reset_index()

    cohort = cohort.merge(amend_stats, on="contract_id", how="left")

    # Fill NaN for contracts with no amendments
    cohort["n_amendments"] = cohort["n_amendments"].fillna(0).astype(int)
    cohort["had_suspension"] = cohort["had_suspension"].fillna(False)
    cohort["n_extensions"] = cohort["n_extensions"].fillna(0).astype(int)
    cohort["n_value_additions"] = cohort["n_value_additions"].fillna(0).astype(int)
    cohort["n_value_reductions"] = cohort["n_value_reductions"].fillna(0).astype(int)

    # Note: amendments table does NOT have structured value deltas (see inventory 8.5)
    # total_value_delta cannot be computed from amendments alone
    # Using n_value_additions as a proxy
    cohort["total_value_delta"] = 0  # Placeholder — no structured value data in amendments

    # --- 4. Score and rank ---
    print("\n[4/5] Computing demo suitability scores...")

    cohort["score"] = (
        cohort["awarded_value_cop"].apply(lambda v: math.log(max(v, 1)))
        * (cohort["n_amendments"] + 1)
        * cohort["had_suspension"].apply(lambda s: 1.5 if s else 1.0)
    )

    cohort = cohort.sort_values("score", ascending=False).reset_index(drop=True)
    top200 = cohort.head(200).copy()

    # --- 5. Save ---
    print("\n[5/5] Saving cohort_candidates.parquet...")
    out_path = OUT_DIR / "cohort_candidates.parquet"
    top200.to_parquet(out_path, index=False, compression="zstd")
    print(f"  Saved {len(top200)} rows to {out_path}")

    # --- Phase Report ---
    print("\n" + "=" * 60)
    print("PHASE 1 REPORT")
    print("=" * 60)

    # Count by department
    print("\n--- Count by Department ---")
    dept_counts = top200["department_norm"].value_counts()
    for dept, count in dept_counts.items():
        print(f"  {dept}: {count}")

    # Count by entity level
    print("\n--- Count by Entity Level ---")
    level_counts = top200["entity_level"].value_counts()
    for level, count in level_counts.items():
        print(f"  {level}: {count}")

    # Value stats
    usd_rate = 4200  # Approximate COP/USD
    median_cop = top200["awarded_value_cop"].median()
    p90_cop = top200["awarded_value_cop"].quantile(0.9)
    print(f"\n--- Contract Value (top 200) ---")
    print(f"  Median: {median_cop:,.0f} COP (~${median_cop / usd_rate:,.0f} USD)")
    print(f"  P90:    {p90_cop:,.0f} COP (~${p90_cop / usd_rate:,.0f} USD)")
    print(f"  Min:    {top200['awarded_value_cop'].min():,.0f} COP")
    print(f"  Max:    {top200['awarded_value_cop'].max():,.0f} COP")

    # Amendment distribution
    print(f"\n--- Amendment Count Distribution (top 200) ---")
    amend_dist = top200["n_amendments"].value_counts().sort_index()
    for n_amend, count in amend_dist.items():
        bar = "#" * count
        print(f"  {n_amend:3d} amendments: {count:3d} {bar}")

    # Suspensions
    n_suspended = top200["had_suspension"].sum()
    print(f"\n  Contracts with suspension: {n_suspended}")
    print(f"  Contracts with extensions: {(top200['n_extensions'] > 0).sum()}")

    # Top 10 contractors
    print(f"\n--- Top 10 Contractors by Frequency ---")
    contractor_counts = top200["supplier_name"].value_counts().head(10)
    for name, count in contractor_counts.items():
        print(f"  {count:3d}  {name[:80]}")

    # Score range
    print(f"\n--- Score Range ---")
    print(f"  Max: {top200['score'].iloc[0]:.1f}")
    print(f"  Min: {top200['score'].iloc[-1]:.1f}")
    print(f"  Median: {top200['score'].median():.1f}")


if __name__ == "__main__":
    main()
