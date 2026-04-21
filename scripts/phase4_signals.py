"""Phase 4 — Signal Construction.

Computes 8 anomaly signals against the reference population.
Each signal is saved as a separate parquet under data/signals/.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path("data")
PARQUET_DIR = DATA_DIR / "parquet"
SIGNALS_DIR = DATA_DIR / "signals"
NOW = pd.Timestamp("2026-04-01", tz="UTC")


def read_parquet_dir(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    files = sorted(path.glob("**/data.parquet"))
    tables = [pq.ParquetFile(f).read(columns=columns) for f in files]
    return pa.concat_tables(tables, promote_options="default").to_pandas()


def percentile_rank(series: pd.Series) -> pd.Series:
    """Compute percentile rank (0-1) within the series."""
    return series.rank(pct=True, na_option="keep")


def report_signal(name: str, df: pd.DataFrame, score_col: str, threshold: float | None = None):
    """Print summary stats for a signal."""
    vals = df[score_col].dropna()
    print(f"\n  --- {name} ---", flush=True)
    print(f"  N total: {len(df):,}, N with score: {len(vals):,}", flush=True)
    if len(vals) > 0:
        print(f"  Mean: {vals.mean():.3f}, Median: {vals.median():.3f}", flush=True)
        print(f"  P95: {vals.quantile(0.95):.3f}, P99: {vals.quantile(0.99):.3f}, Max: {vals.max():.3f}", flush=True)
        if threshold is not None:
            flagged = (vals > threshold).sum()
            print(f"  Flagged (>{threshold}): {flagged:,} ({flagged / len(vals) * 100:.1f}%)", flush=True)
    print(f"  Top 10:", flush=True)
    for _, r in df.nlargest(10, score_col).iterrows():
        print(f"    {r.get('contract_id', r.get('entity_nit', '?'))}: {r[score_col]:.3f}", flush=True)


# ===================================================================
# Signal 4.1 — Payment Stall
# ===================================================================
def signal_stall(refpop: pd.DataFrame, progress: pd.DataFrame) -> pd.DataFrame:
    print("\n[S1] Payment Stall", flush=True)

    # For the 182 cohort contracts: use detailed timeline
    # For the rest of refpop: use simplified stall metric

    # --- Detailed stall for cohort contracts ---
    cohort_stall = []
    for cid, group in progress.sort_values("month").groupby("contract_id"):
        pcts = group["declared_progress_pct"].values
        statuses = group["active_status"].values

        if len(pcts) == 0:
            continue

        current_pct = pcts[-1]
        if current_pct >= 0.95:
            continue  # Near-complete, skip

        # Count trailing months where progress changed < 2pp while active
        months_flat = 0
        months_flat_active = 0
        for i in range(len(pcts) - 1, 0, -1):
            delta = abs(pcts[i] - pcts[i - 1])
            if delta < 0.02:
                months_flat += 1
                if statuses[i] == "active":
                    months_flat_active += 1
            else:
                break

        cohort_stall.append({
            "contract_id": cid,
            "months_flat_while_active": months_flat_active,
            "declared_progress_pct_current": current_pct,
            "stall_source": "detailed",
        })

    detailed = pd.DataFrame(cohort_stall)

    # --- Simplified stall for all refpop ---
    # Active contracts with low payment ratio relative to elapsed time
    active_mask = ~refpop["status_raw"].isin(["Cerrado", "terminado", "Cancelado"])
    active = refpop[active_mask & (refpop["progress_pct"] < 0.95)].copy()

    # Months since start with no/low payment = simplified "months_flat"
    # If progress is < 10% and contract has been running > 12 months, flag
    active["months_flat_while_active"] = np.where(
        active["progress_pct"] < 0.10,
        active["months_since_start"].clip(upper=60),
        np.where(
            active["progress_pct"] < 0.30,
            (active["months_since_start"] * (1 - active["progress_pct"])).clip(upper=40),
            0,
        ),
    ).astype(int)

    simple = active[["contract_id", "months_flat_while_active", "progress_pct"]].rename(
        columns={"progress_pct": "declared_progress_pct_current"}
    )
    simple["stall_source"] = "simplified"

    # Merge: prefer detailed where available
    detailed_ids = set(detailed["contract_id"])
    simple_only = simple[~simple["contract_id"].isin(detailed_ids)]
    stall = pd.concat([detailed, simple_only], ignore_index=True)

    # Compute stall score
    val_map = refpop.set_index("contract_id")["awarded_value_cop"]
    stall["valor_contrato"] = stall["contract_id"].map(val_map)
    stall["stall_score"] = (
        np.log(stall["valor_contrato"].clip(lower=1))
        * stall["months_flat_while_active"]
    )

    # Percentile within all stall-eligible contracts
    stall["stall_percentile"] = percentile_rank(stall["stall_score"])

    out = stall[["contract_id", "months_flat_while_active",
                 "declared_progress_pct_current", "stall_score", "stall_percentile"]]
    out.to_parquet(SIGNALS_DIR / "s1_stall.parquet", index=False, compression="zstd")
    report_signal("S1 Stall", out, "stall_score", threshold=100)
    return out


# ===================================================================
# Signal 4.2 — Value Creep
# ===================================================================
def signal_value_creep(refpop: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n[S2] Value Creep", flush=True)

    # Get process estimated values (deduplicated)
    procs = read_parquet_dir(PARQUET_DIR / "processes", columns=["process_id", "estimated_value_cop"])
    procs = procs.groupby("process_id")["estimated_value_cop"].max().reset_index()

    # Count contracts per process (skip multi-lot)
    cpc = refpop.groupby("process_id")["contract_id"].count().rename("n_in_process")
    rp = refpop.merge(cpc, on="process_id", how="left")
    rp = rp[rp["n_in_process"] == 1]  # Single-contract processes only

    rp = rp.merge(procs.rename(columns={"estimated_value_cop": "process_estimated_cop"}),
                   on="process_id", how="left")
    valid = rp["process_estimated_cop"].notna() & (rp["process_estimated_cop"] > 0)
    rp = rp[valid].copy()

    rp["value_creep_ratio"] = (
        (rp["awarded_value_cop"] - rp["process_estimated_cop"]) / rp["process_estimated_cop"]
    )

    # Contract-level
    contract_creep = rp[["contract_id", "supplier_id", "value_creep_ratio"]].copy()
    contract_creep["creep_percentile"] = percentile_rank(contract_creep["value_creep_ratio"])

    contract_creep.to_parquet(
        SIGNALS_DIR / "s2_value_creep.parquet", index=False, compression="zstd"
    )
    report_signal("S2 Value Creep (contract)", contract_creep, "value_creep_ratio", threshold=0.3)

    # Contractor-level: value-weighted mean
    rp["weighted_creep"] = rp["value_creep_ratio"] * rp["awarded_value_cop"]
    contractor = rp.groupby("supplier_id").agg(
        portfolio_creep_ratio_num=("weighted_creep", "sum"),
        portfolio_total_value=("awarded_value_cop", "sum"),
        n_contracts_weighted=("contract_id", "count"),
    ).reset_index()
    contractor["portfolio_creep_ratio"] = (
        contractor["portfolio_creep_ratio_num"] / contractor["portfolio_total_value"]
    )
    contractor = contractor[contractor["n_contracts_weighted"] >= 2]
    contractor["creep_percentile"] = percentile_rank(contractor["portfolio_creep_ratio"])

    contractor[["supplier_id", "portfolio_creep_ratio", "n_contracts_weighted", "creep_percentile"]].to_parquet(
        SIGNALS_DIR / "s2_value_creep_contractor.parquet", index=False, compression="zstd"
    )
    report_signal("S2 Value Creep (contractor)", contractor, "portfolio_creep_ratio", threshold=0.3)

    return contract_creep, contractor


# ===================================================================
# Signal 4.3 — Schedule Slippage
# ===================================================================
def signal_slippage(refpop: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n[S3] Schedule Slippage", flush=True)

    rp = refpop[refpop["original_duration_days"] > 30].copy()  # Skip very short contracts

    # Contract-level
    contract_slip = rp[["contract_id", "supplier_id", "slippage_ratio",
                         "dias_adicionados", "original_duration_days"]].copy()
    contract_slip["extension_days_total"] = contract_slip["dias_adicionados"].fillna(0)
    contract_slip["slippage_percentile"] = percentile_rank(contract_slip["slippage_ratio"])

    contract_slip[["contract_id", "extension_days_total", "original_duration_days",
                    "slippage_ratio", "slippage_percentile"]].to_parquet(
        SIGNALS_DIR / "s3_slippage.parquet", index=False, compression="zstd"
    )
    report_signal("S3 Slippage (contract)", contract_slip, "slippage_ratio", threshold=0.5)

    # Contractor-level: value-weighted mean
    rp_valid = rp[rp["awarded_value_cop"] > 0].copy()
    rp_valid["weighted_slip"] = rp_valid["slippage_ratio"] * rp_valid["awarded_value_cop"]
    contractor = rp_valid.groupby("supplier_id").agg(
        portfolio_slip_num=("weighted_slip", "sum"),
        portfolio_total_value=("awarded_value_cop", "sum"),
        n_contracts=("contract_id", "count"),
    ).reset_index()
    contractor["portfolio_slippage_ratio"] = (
        contractor["portfolio_slip_num"] / contractor["portfolio_total_value"]
    )
    contractor = contractor[contractor["n_contracts"] >= 2]
    contractor["slippage_percentile"] = percentile_rank(contractor["portfolio_slippage_ratio"])

    contractor[["supplier_id", "portfolio_slippage_ratio", "n_contracts", "slippage_percentile"]].to_parquet(
        SIGNALS_DIR / "s3_slippage_contractor.parquet", index=False, compression="zstd"
    )
    report_signal("S3 Slippage (contractor)", contractor, "portfolio_slippage_ratio", threshold=0.5)

    return contract_slip, contractor


# ===================================================================
# Signal 4.4 — Threshold Bunching
# ===================================================================
def signal_bunching(refpop: pd.DataFrame) -> pd.DataFrame:
    print("\n[S4] Threshold Bunching", flush=True)

    # Simplified approach: use national SMMLV-based thresholds
    # 2024 SMMLV = 1,300,000 COP
    # Menor cuantía upper: ~280 SMMLV = 364M COP (varies by entity size)
    # Mínima cuantía upper: 10% of menor cuantía = 36.4M COP
    # Using fixed thresholds for simplicity (documented in methodology)
    SMMLV = 1_300_000
    thresholds = {
        "minima_cuantia": int(28 * SMMLV),       # ~36.4M COP
        "menor_cuantia_low": int(280 * SMMLV),    # ~364M COP
        "menor_cuantia_high": int(1000 * SMMLV),  # ~1.3B COP
    }

    # Use ALL contract types for bunching (not just Obra), per entity per year
    all_contracts = read_parquet_dir(
        PARQUET_DIR / "contracts",
        columns=["entity_nit", "contract_signature_date", "awarded_value_cop"],
    )
    all_contracts["year"] = all_contracts["contract_signature_date"].dt.year
    all_contracts = all_contracts[
        (all_contracts["year"] >= 2020) & (all_contracts["year"] <= 2024)
    ]

    rows = []
    for (entity, year), group in all_contracts.groupby(["entity_nit", "year"]):
        values = group["awarded_value_cop"].values
        n = len(values)
        if n < 5:  # Need enough contracts for meaningful bunching
            continue

        for tname, T in thresholds.items():
            below = ((values >= 0.85 * T) & (values < T)).sum()
            above = ((values > T) & (values <= 1.15 * T)).sum()
            bunching_ratio = below / max(above, 1)

            rows.append({
                "entity_nit": entity,
                "year": year,
                "threshold_name": tname,
                "threshold_value": T,
                "bunching_ratio": bunching_ratio,
                "n_below": int(below),
                "n_above": int(above),
                "n_contracts_in_window": n,
            })

    bunching = pd.DataFrame(rows)

    bunching.to_parquet(SIGNALS_DIR / "s4_bunching.parquet", index=False, compression="zstd")

    # Report on suspicious bunching (ratio > 1.5)
    flagged = bunching[bunching["bunching_ratio"] > 1.5]
    print(f"  Total entity-year-threshold rows: {len(bunching):,}", flush=True)
    print(f"  Flagged (ratio > 1.5): {len(flagged):,}", flush=True)
    print(f"  Unique entities flagged: {flagged['entity_nit'].nunique():,}", flush=True)
    if len(flagged) > 0:
        print(f"  By threshold:", flush=True)
        print(flagged["threshold_name"].value_counts().to_string(), flush=True)
        print(f"  Top 10 by bunching ratio:", flush=True)
        for _, r in flagged.nlargest(10, "bunching_ratio").iterrows():
            print(f"    {r['entity_nit']} ({r['year']}) {r['threshold_name']}: "
                  f"ratio={r['bunching_ratio']:.1f} (below={r['n_below']}, above={r['n_above']})", flush=True)

    return bunching


# ===================================================================
# Signal 4.5 — Contractor Concentration (HHI)
# ===================================================================
def signal_concentration(refpop: pd.DataFrame) -> pd.DataFrame:
    print("\n[S5] Contractor Concentration", flush=True)

    # Use ALL contract types for concentration
    all_contracts = read_parquet_dir(
        PARQUET_DIR / "contracts",
        columns=["entity_nit", "supplier_id", "awarded_value_cop",
                 "contract_signature_date"],
    )

    # Rolling 12-month windows ending each quarter-end
    windows = pd.date_range("2020-03-31", "2024-12-31", freq="QE", tz="UTC")

    rows = []
    for w_end in windows:
        w_start = w_end - pd.DateOffset(months=12)
        mask = (
            (all_contracts["contract_signature_date"] >= w_start)
            & (all_contracts["contract_signature_date"] <= w_end)
        )
        window_df = all_contracts[mask]

        for entity, egroup in window_df.groupby("entity_nit"):
            total_val = egroup["awarded_value_cop"].sum()
            if total_val <= 0 or len(egroup) < 3:
                continue

            shares = egroup.groupby("supplier_id")["awarded_value_cop"].sum() / total_val
            hhi = (shares ** 2).sum()
            top_contractor = shares.idxmax()
            top_share = shares.max()

            rows.append({
                "entity_nit": entity,
                "window_end": w_end.tz_localize(None),
                "hhi": hhi,
                "top_contractor_nit": top_contractor,
                "top_contractor_share": top_share,
                "n_contracts": len(egroup),
                "n_contractors": len(shares),
            })

    conc = pd.DataFrame(rows)

    # Compute streak: consecutive windows where same contractor is top
    conc = conc.sort_values(["entity_nit", "window_end"])
    streaks = []
    for entity, egroup in conc.groupby("entity_nit"):
        tops = egroup["top_contractor_nit"].values
        streak = 1
        for i in range(1, len(tops)):
            if tops[i] == tops[i - 1]:
                streak += 1
            else:
                streak = 1
            streaks.append(streak)
        if len(tops) > 0:
            streaks.insert(len(streaks) - len(tops) + 1, 1)  # First entry = 1

    # Simpler streak computation
    conc["streak_length"] = 1
    prev_entity = None
    prev_top = None
    streak = 1
    streak_vals = []
    for _, r in conc.iterrows():
        if r["entity_nit"] == prev_entity and r["top_contractor_nit"] == prev_top:
            streak += 1
        else:
            streak = 1
        streak_vals.append(streak)
        prev_entity = r["entity_nit"]
        prev_top = r["top_contractor_nit"]
    conc["streak_length"] = streak_vals

    conc.to_parquet(SIGNALS_DIR / "s5_concentration.parquet", index=False, compression="zstd")

    flagged = conc[(conc["hhi"] > 0.25) & (conc["streak_length"] >= 3)]
    print(f"  Total entity-window rows: {len(conc):,}", flush=True)
    print(f"  Flagged (HHI>0.25 & streak>=3): {len(flagged):,}", flush=True)
    print(f"  Unique entities flagged: {flagged['entity_nit'].nunique():,}", flush=True)
    print(f"  HHI distribution: mean={conc['hhi'].mean():.3f}, median={conc['hhi'].median():.3f}, "
          f"P95={conc['hhi'].quantile(0.95):.3f}", flush=True)

    return conc


# ===================================================================
# Signal 4.6 — Single-Bidder Rate
# ===================================================================
def signal_single_bidder() -> pd.DataFrame:
    print("\n[S6] Single-Bidder Rate", flush=True)

    bids = pd.read_parquet(DATA_DIR / "obra_bid_counts.parquet")
    bids = bids[bids["publication_year"].isin(["2020", "2021", "2022", "2023", "2024"])]
    bids["year"] = bids["publication_year"].astype(int)

    # Exclude contratación directa (single-source by design)
    competitive_methods = [
        "Licitación pública", "Licitación pública Obra Publica",
        "Selección Abreviada de Menor Cuantía", "Mínima cuantía",
        "Selección abreviada subasta inversa",
        "Contratación régimen especial (con ofertas)",
        "Contratación Directa (con ofertas)",
        "Concurso de méritos abierto",
    ]

    result_rows = []
    for method_filter, method_label in [("all_competitive", "all_competitive")] + [
        (m, m) for m in competitive_methods
    ]:
        if method_filter == "all_competitive":
            subset = bids[bids["procurement_method_raw"].isin(competitive_methods)]
        else:
            subset = bids[bids["procurement_method_raw"] == method_filter]

        if len(subset) == 0:
            continue

        for (entity, year), group in subset.groupby(["entity_nit", "year"]):
            n = len(group)
            if n < 2:
                continue
            n_single = (group["bid_count"] <= 1).sum()
            rate = n_single / n

            result_rows.append({
                "entity_nit": entity,
                "year": year,
                "procurement_method": method_label,
                "n_processes": n,
                "n_single_bidder": n_single,
                "single_bidder_rate": rate,
            })

    sb = pd.DataFrame(result_rows)

    # Add national median for comparison
    if len(sb) > 0:
        national_medians = sb.groupby(["year", "procurement_method"])["single_bidder_rate"].median()
        sb["single_bidder_rate_national_median"] = sb.apply(
            lambda r: national_medians.get((r["year"], r["procurement_method"]), np.nan), axis=1
        )

    sb.to_parquet(SIGNALS_DIR / "s6_single_bidder.parquet", index=False, compression="zstd")

    # Report on all_competitive
    comp = sb[sb["procurement_method"] == "all_competitive"]
    print(f"  Total entity-year rows: {len(sb):,}", flush=True)
    print(f"  Competitive-only rows: {len(comp):,}", flush=True)
    if len(comp) > 0:
        print(f"  Single-bidder rate (competitive): mean={comp['single_bidder_rate'].mean():.3f}, "
              f"median={comp['single_bidder_rate'].median():.3f}, P95={comp['single_bidder_rate'].quantile(0.95):.3f}", flush=True)
        flagged = comp[comp["single_bidder_rate"] > 0.5]
        print(f"  Entities with >50% single-bidder: {flagged['entity_nit'].nunique():,}", flush=True)

    return sb


# ===================================================================
# Signal 4.7 — Time-to-Award Residual
# ===================================================================
def signal_award_speed() -> pd.DataFrame:
    print("\n[S7] Time-to-Award Residual", flush=True)

    procs = read_parquet_dir(PARQUET_DIR / "processes", columns=[
        "process_id", "estimated_value_cop", "procurement_method_norm",
        "publication_date", "award_date", "category_code", "contract_type_raw",
    ])

    # Filter: Obra, with both dates, 2020-2024
    procs = procs[
        (procs["contract_type_raw"] == "Obra")
        & procs["publication_date"].notna()
        & procs["award_date"].notna()
    ].copy()

    procs["days_to_award"] = (procs["award_date"] - procs["publication_date"]).dt.days
    procs = procs[(procs["days_to_award"] > 0) & (procs["days_to_award"] < 3650)]  # Sanity: 0-10 years
    procs["pub_year"] = procs["publication_date"].dt.year
    procs = procs[(procs["pub_year"] >= 2020) & (procs["pub_year"] <= 2024)]

    # Deduplicate by process_id (take first)
    procs = procs.drop_duplicates(subset="process_id", keep="first")

    procs["log_days"] = np.log(procs["days_to_award"] + 1)
    procs["log_value"] = np.log(procs["estimated_value_cop"].clip(lower=1))

    # UNSPSC 4-digit category
    procs["cat4"] = procs["category_code"].str.extract(r"V1\.(\d{4})", expand=False).fillna("0000")

    # Simple OLS: log(days) ~ log(value) + method + year
    # Using dummy encoding
    method_dummies = pd.get_dummies(procs["procurement_method_norm"], prefix="method", dtype=float)
    year_dummies = pd.get_dummies(procs["pub_year"], prefix="year", dtype=float)

    X = pd.concat([
        procs[["log_value"]],
        method_dummies.iloc[:, :-1],  # Drop one for intercept
        year_dummies.iloc[:, :-1],
    ], axis=1).values

    # Add intercept
    X = np.column_stack([np.ones(len(X)), X])
    y = procs["log_days"].values

    # Remove NaN/inf
    valid = np.isfinite(X).all(axis=1) & np.isfinite(y)
    X_v, y_v = X[valid], y[valid]

    # OLS via numpy lstsq
    beta, residuals, rank, sv = np.linalg.lstsq(X_v, y_v, rcond=None)
    y_hat = X_v @ beta
    resid = y_v - y_hat
    resid_std = resid.std()

    # Map back
    procs_valid = procs[valid].copy()
    procs_valid["expected_log_days"] = y_hat
    procs_valid["expected_days"] = np.exp(y_hat) - 1
    procs_valid["residual_z"] = resid / resid_std
    procs_valid["flag_direction"] = np.where(
        procs_valid["residual_z"] > 2, "slow",
        np.where(procs_valid["residual_z"] < -2, "fast", "normal")
    )

    out = procs_valid[["process_id", "days_to_award", "expected_days",
                        "residual_z", "flag_direction"]].copy()

    out.to_parquet(SIGNALS_DIR / "s7_award_speed.parquet", index=False, compression="zstd")

    print(f"  N processes: {len(out):,}", flush=True)
    print(f"  R²: {1 - (resid ** 2).sum() / ((y_v - y_v.mean()) ** 2).sum():.3f}", flush=True)
    print(f"  Flagged fast (z<-2): {(out['flag_direction'] == 'fast').sum():,}", flush=True)
    print(f"  Flagged slow (z>2): {(out['flag_direction'] == 'slow').sum():,}", flush=True)
    print(f"  Residual z: mean={out['residual_z'].mean():.3f}, std={out['residual_z'].std():.3f}", flush=True)
    print(f"  Top 10 fastest (most negative z):", flush=True)
    for _, r in out.nsmallest(10, "residual_z").iterrows():
        print(f"    {r['process_id']}: {r['days_to_award']}d (expected {r['expected_days']:.0f}d), z={r['residual_z']:.2f}", flush=True)

    return out


# ===================================================================
# Signal 4.8 — Contractor-Entity Relationship Intensity
# ===================================================================
def signal_relationships() -> pd.DataFrame:
    print("\n[S8] Contractor-Entity Relationship Intensity", flush=True)

    contracts = read_parquet_dir(PARQUET_DIR / "contracts", columns=[
        "entity_nit", "supplier_id", "awarded_value_cop", "contract_signature_date",
        "contract_type_raw",
    ])

    # Restrict to Obra contracts 2022-2024
    mask = (
        (contracts["contract_type_raw"] == "Obra")
        & (contracts["contract_signature_date"] >= "2022-01-01")
        & (contracts["contract_signature_date"] <= "2024-12-31")
    )
    c = contracts[mask].copy()

    # Remove "No Definido" and empty supplier IDs
    c = c[c["supplier_id"].notna() & (c["supplier_id"] != "No Definido") & (c["supplier_id"] != "")]
    # Remove negative/zero values
    c = c[c["awarded_value_cop"] > 0]
    print(f"  Obra contracts 2022-2024 with valid supplier: {len(c):,}", flush=True)

    # Build edge weights
    edges = c.groupby(["entity_nit", "supplier_id"]).agg(
        total_awarded_cop=("awarded_value_cop", "sum"),
        n_contracts=("awarded_value_cop", "count"),
    ).reset_index()

    # Filter: at least 3 contracts per edge
    edges = edges[edges["n_contracts"] >= 3]

    # Compute marginals
    entity_totals = c.groupby("entity_nit")["awarded_value_cop"].sum().rename("w_e_total")
    contractor_totals = c.groupby("supplier_id")["awarded_value_cop"].sum().rename("w_c_total")
    w_total = c["awarded_value_cop"].sum()

    edges = edges.merge(entity_totals, on="entity_nit", how="left")
    edges = edges.merge(contractor_totals, on="supplier_id", how="left")

    # Expected weight under gravity null
    edges["expected_cop"] = (edges["w_e_total"] * edges["w_c_total"]) / w_total

    # Z-score: log-ratio approach for heavy-tailed value distributions
    # Use log(observed/expected) standardized by its std deviation across all edges
    edges["log_ratio"] = np.log(
        (edges["total_awarded_cop"] + 1) / (edges["expected_cop"].clip(lower=1) + 1)
    )
    lr_mean = edges["log_ratio"].mean()
    lr_std = edges["log_ratio"].std()
    edges["z_score"] = (edges["log_ratio"] - lr_mean) / lr_std if lr_std > 0 else 0.0

    out = edges[["entity_nit", "supplier_id", "total_awarded_cop",
                  "expected_cop", "z_score", "n_contracts"]].copy()

    out.to_parquet(SIGNALS_DIR / "s8_relationships.parquet", index=False, compression="zstd")

    print(f"  Total edges (>=3 contracts): {len(out):,}", flush=True)
    print(f"  Z-score: mean={out['z_score'].mean():.2f}, median={out['z_score'].median():.2f}, "
          f"P95={out['z_score'].quantile(0.95):.2f}, max={out['z_score'].max():.2f}", flush=True)
    flagged = out[out["z_score"] > 3]
    print(f"  Flagged (z>3): {len(flagged):,}", flush=True)
    print(f"  Top 10 by z-score:", flush=True)
    for _, r in out.nlargest(10, "z_score").iterrows():
        print(f"    {r['entity_nit']} × {r['supplier_id']}: z={r['z_score']:.1f}, "
              f"actual={r['total_awarded_cop']/1e9:.1f}B vs expected={r['expected_cop']/1e9:.1f}B, "
              f"n={r['n_contracts']}", flush=True)

    return out


def main():
    print("=" * 60, flush=True)
    print("PHASE 4 — SIGNAL CONSTRUCTION", flush=True)
    print("=" * 60, flush=True)

    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    refpop = pd.read_parquet(DATA_DIR / "reference_population.parquet")
    print(f"Reference population: {len(refpop):,} contracts", flush=True)

    # Load declared progress (for s1)
    progress = pd.read_parquet(DATA_DIR / "declared_progress.parquet")

    # Run all signals
    s1 = signal_stall(refpop, progress)
    s2_c, s2_k = signal_value_creep(refpop)
    s3_c, s3_k = signal_slippage(refpop)
    s4 = signal_bunching(refpop)
    s5 = signal_concentration(refpop)
    s6 = signal_single_bidder()
    s7 = signal_award_speed()
    s8 = signal_relationships()

    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 4 COMPLETE", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"Signals saved to {SIGNALS_DIR}/", flush=True)
    for f in sorted(SIGNALS_DIR.glob("*.parquet")):
        size = f.stat().st_size / 1024
        print(f"  {f.name}: {size:.0f} KB", flush=True)


if __name__ == "__main__":
    main()
