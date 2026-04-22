"""Phase 5 — Composite Score & Demo Cohort.

Combines all 8 signals into a composite anomaly score,
selects the demo cohort, and builds the contractor league table.
"""

from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path("data")
SIGNALS_DIR = DATA_DIR / "signals"

# Composite weights — defensible first guesses, NOT optimized
WEIGHTS = {
    "z_stall": 1.0,
    "z_creep_contract": 1.0,
    "z_creep_contractor": 0.5,
    "z_slip_contract": 0.7,
    "z_slip_contractor": 0.5,
    "z_bunching_entity": 0.7,
    "z_hhi_entity": 0.5,
    "z_single_bidder_entity": 0.7,
    "z_award_speed_abs": 0.5,
    "z_relationship": 0.8,
    "z_fragmentation": 0.7,
}

# Signal keys (without z_ prefix) that map to short dashboard names
SIGNAL_KEYS = [
    "stall", "creep_contract", "creep_contractor",
    "slip_contract", "slip_contractor", "bunching_entity",
    "hhi_entity", "single_bidder_entity", "award_speed_abs",
    "relationship", "fragmentation",
]

# Cohorts exempt from certain signals (structurally meaningless)
# Includes regimen especial sub-cohorts
EXEMPT_BUNCHING = {
    "mandato", "directa", "eice", "especial",
    "especial_ese", "especial_universidad", "especial_d092",
    "especial_convenio", "especial_otro",
}
EXEMPT_SINGLE = {"mandato", "directa"}

COP_TO_USD = 4000  # Approximate, prominently documented

# ── Data-quality plausibility thresholds ─────────────────────────
NATURAL_PERSON_DOC_TYPES = {"Cedula Ciudadania", "Cedula Extranjeria"}
NATURAL_PERSON_VALUE_THRESHOLD = 500_000_000       # 500M COP
ENTITY_MEDIAN_FACTOR = 100                          # 100x entity median
ENTITY_MEDIAN_MIN_CONTRACTS = 10                    # need >=10 contracts
VALUE_PAYMENT_FACTOR = 10                           # awarded > 10x max(pagado, facturado)
PAYMENT_ELIGIBLE_STATUSES = {"En ejecucion", "Modificado", "Cerrado", "terminado"}


def apply_dq_filters(refpop: pd.DataFrame) -> pd.DataFrame:
    """Flag implausible contracts before scoring.

    Returns refpop with two new columns:
      dq_excluded (bool) — True if any check fires
      dq_flags   (str)   — comma-separated flag names
    """
    n = len(refpop)
    flags = pd.DataFrame(index=refpop.index)

    # Check 1: Natural-person mega-contract
    is_natural = refpop["supplier_doc_type"].isin(NATURAL_PERSON_DOC_TYPES)
    is_mega = refpop["awarded_value_cop"] > NATURAL_PERSON_VALUE_THRESHOLD
    flags["natural_person_mega"] = is_natural & is_mega

    # Check 2: Entity median deviation (skip entities with < 10 contracts)
    entity_stats = refpop.groupby("entity_nit")["awarded_value_cop"].agg(["median", "count"])
    entity_stats.columns = ["entity_median", "entity_n"]
    refpop_idx = refpop.merge(entity_stats, left_on="entity_nit", right_index=True, how="left")
    enough_contracts = refpop_idx["entity_n"] >= ENTITY_MEDIAN_MIN_CONTRACTS
    over_median = refpop_idx["awarded_value_cop"] > ENTITY_MEDIAN_FACTOR * refpop_idx["entity_median"]
    flags["entity_median_outlier"] = enough_contracts & over_median

    # Check 3: Value-vs-payment mismatch
    pagado = refpop["valor_pagado"].fillna(0)
    facturado = refpop["valor_facturado"].fillna(0)
    max_payment = np.maximum(pagado, facturado)
    has_payment = pagado > 0
    over_payment = refpop["awarded_value_cop"] > VALUE_PAYMENT_FACTOR * max_payment
    eligible_status = refpop["status_raw"].isin(PAYMENT_ELIGIBLE_STATUSES)
    flags["value_payment_mismatch"] = has_payment & over_payment & eligible_status

    # Combine
    flag_cols = ["natural_person_mega", "entity_median_outlier", "value_payment_mismatch"]
    refpop["dq_excluded"] = flags[flag_cols].any(axis=1)
    refpop["dq_flags"] = flags[flag_cols].apply(
        lambda row: ",".join(c for c in flag_cols if row[c]), axis=1
    )

    n_excluded = refpop["dq_excluded"].sum()
    print(f"  DQ filter: {n_excluded:,} of {n:,} contracts flagged ({n_excluded/n*100:.1f}%)", flush=True)
    for col in flag_cols:
        print(f"    {col}: {flags[col].sum():,}", flush=True)

    return refpop


def zscore_clip(series: pd.Series) -> pd.Series:
    """Standardize to z-score, clip [-5,5], NaN → 0."""
    mean = series.mean()
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    z = ((series - mean) / std).clip(-5, 5).fillna(0)
    return z


def zscore_clip_cohort(series: pd.Series, cohort_series: pd.Series):
    """Compute z_global and z_cohort for each value."""
    z_global = zscore_clip(series)

    z_cohort = pd.Series(0.0, index=series.index)
    for cohort in cohort_series.unique():
        mask = cohort_series == cohort
        z_cohort[mask] = zscore_clip(series[mask])

    return z_global, z_cohort


def main():
    print("=" * 60, flush=True)
    print("PHASE 5 — COMPOSITE SCORE & DEMO COHORT", flush=True)
    print("=" * 60, flush=True)

    # Load reference population
    refpop = pd.read_parquet(DATA_DIR / "reference_population.parquet")
    print(f"Reference population: {len(refpop):,} contracts", flush=True)

    # Apply data-quality plausibility filters
    refpop = apply_dq_filters(refpop)
    clean = refpop[~refpop["dq_excluded"]].copy()
    excluded = refpop[refpop["dq_excluded"]].copy()
    print(f"  Clean: {len(clean):,}, DQ-excluded: {len(excluded):,}", flush=True)

    # =============================================
    # Step 5.1 — Contract-level composite
    # =============================================
    print("\n[Step 5.1] Building contract-level composite...", flush=True)

    features = clean[["contract_id", "process_id", "entity_nit", "supplier_id",
                       "awarded_value_cop", "signature_year",
                       "cohort_key", "is_mandato", "is_eice"]].copy()

    # --- S1: Stall (direct per-contract) ---
    s1 = pd.read_parquet(SIGNALS_DIR / "s1_stall.parquet")
    features = features.merge(
        s1[["contract_id", "stall_score"]].rename(columns={"stall_score": "raw_stall"}),
        on="contract_id", how="left"
    )
    # Flag contracts not evaluated by S1 (vp=0)
    s1_ids = set(s1["contract_id"])
    features["s1_not_evaluated"] = ~features["contract_id"].isin(s1_ids)

    # --- S2: Value creep (contract-level + contractor-level) ---
    s2c = pd.read_parquet(SIGNALS_DIR / "s2_value_creep.parquet")
    features = features.merge(
        s2c[["contract_id", "value_creep_ratio"]].rename(columns={"value_creep_ratio": "raw_creep_contract"}),
        on="contract_id", how="left"
    )

    s2k = pd.read_parquet(SIGNALS_DIR / "s2_value_creep_contractor.parquet")
    features = features.merge(
        s2k[["supplier_id", "portfolio_creep_ratio", "portfolio_creep_ratio_count"]].rename(columns={
            "portfolio_creep_ratio": "raw_creep_contractor",
            "portfolio_creep_ratio_count": "raw_creep_contractor_count",
        }),
        on="supplier_id", how="left"
    )

    # --- S3: Slippage (contract + contractor) ---
    s3c = pd.read_parquet(SIGNALS_DIR / "s3_slippage.parquet")
    features = features.merge(
        s3c[["contract_id", "slippage_ratio"]].rename(columns={"slippage_ratio": "raw_slip_contract"}),
        on="contract_id", how="left"
    )

    s3k = pd.read_parquet(SIGNALS_DIR / "s3_slippage_contractor.parquet")
    features = features.merge(
        s3k[["supplier_id", "portfolio_slippage_ratio", "portfolio_slippage_ratio_count"]].rename(columns={
            "portfolio_slippage_ratio": "raw_slip_contractor",
            "portfolio_slippage_ratio_count": "raw_slip_contractor_count",
        }),
        on="supplier_id", how="left"
    )

    # --- S4: Bunching (entity inherits for contract's year) ---
    s4 = pd.read_parquet(SIGNALS_DIR / "s4_bunching.parquet")
    # Zero out non-significant bunching ratios (permutation test p >= 0.05)
    s4["bunching_ratio_adj"] = np.where(s4["bunching_significant"], s4["bunching_ratio"], 0.0)
    # Take max adjusted bunching ratio across thresholds for each entity-year
    s4_max = s4.groupby(["entity_nit", "year"])["bunching_ratio_adj"].max().reset_index()
    s4_max.rename(columns={"bunching_ratio_adj": "raw_bunching_entity", "year": "signature_year"}, inplace=True)
    features = features.merge(s4_max, on=["entity_nit", "signature_year"], how="left")

    # --- S5: Concentration (entity inherits — use latest window before contract signing) ---
    s5 = pd.read_parquet(SIGNALS_DIR / "s5_concentration.parquet")
    # Take the maximum HHI per entity across all windows as a simple proxy
    s5_max = s5.groupby("entity_nit")["hhi"].max().reset_index()
    s5_max.rename(columns={"hhi": "raw_hhi_entity"}, inplace=True)
    features = features.merge(s5_max, on="entity_nit", how="left")

    # --- S6: Single-bidder rate (entity inherits for contract's year, competitive methods) ---
    s6 = pd.read_parquet(SIGNALS_DIR / "s6_single_bidder.parquet")
    s6_comp = s6[s6["procurement_method"] == "all_competitive"]
    s6_agg = s6_comp.groupby(["entity_nit", "year"])["single_bidder_rate"].max().reset_index()
    s6_agg.rename(columns={"single_bidder_rate": "raw_single_bidder_entity", "year": "signature_year"}, inplace=True)
    features = features.merge(s6_agg, on=["entity_nit", "signature_year"], how="left")

    # --- S7: Award speed (contract inherits from process) ---
    s7 = pd.read_parquet(SIGNALS_DIR / "s7_award_speed.parquet")
    features = features.merge(
        s7[["process_id", "residual_z"]].rename(columns={"residual_z": "raw_award_speed"}),
        on="process_id", how="left"
    )

    # --- S8: Relationship (contract inherits from edge) ---
    s8 = pd.read_parquet(SIGNALS_DIR / "s8_relationships.parquet")
    features = features.merge(
        s8[["entity_nit", "supplier_id", "z_score"]].rename(columns={"z_score": "raw_relationship"}),
        on=["entity_nit", "supplier_id"], how="left"
    )

    # --- S9: Fragmentation (entity-supplier-year → contract inherits max) ---
    s9 = pd.read_parquet(SIGNALS_DIR / "s9_fragmentation.parquet")
    s9_max = s9.groupby(["entity_nit", "supplier_id"])["fragmentation_score"].max().reset_index()
    s9_max.rename(columns={"fragmentation_score": "raw_fragmentation"}, inplace=True)
    features = features.merge(s9_max, on=["entity_nit", "supplier_id"], how="left")

    # --- Dual z-scores: global + cohort-conditioned ---
    cohort = features["cohort_key"]
    raw_signals = {
        "stall": features["raw_stall"],
        "creep_contract": features["raw_creep_contract"],
        "slip_contract": features["raw_slip_contract"],
        "bunching_entity": features["raw_bunching_entity"],
        "hhi_entity": features["raw_hhi_entity"],
        "single_bidder_entity": features["raw_single_bidder_entity"],
        "award_speed_abs": features["raw_award_speed"].abs(),
        "relationship": features["raw_relationship"],
        "fragmentation": features["raw_fragmentation"],
    }

    for sig_name, raw in raw_signals.items():
        z_global, z_cohort = zscore_clip_cohort(raw, cohort)
        features[f"z_{sig_name}_global"] = z_global
        features[f"z_{sig_name}_cohort"] = z_cohort
        # Legacy column (used by WEIGHTS dict): now points to cohort
        features[f"z_{sig_name}"] = z_cohort

    # Dual contractor weighting: max(value-weighted, count-weighted) z-scores
    for sig, vw_col, cw_col in [
        ("creep_contractor", "raw_creep_contractor", "raw_creep_contractor_count"),
        ("slip_contractor", "raw_slip_contractor", "raw_slip_contractor_count"),
    ]:
        z_vw_g, z_vw_c = zscore_clip_cohort(features[vw_col].fillna(0), cohort)
        z_cw_g, z_cw_c = zscore_clip_cohort(features[cw_col].fillna(0), cohort)
        features[f"z_{sig}_global"] = np.maximum(z_vw_g, z_cw_g)
        features[f"z_{sig}_cohort"] = np.maximum(z_vw_c, z_cw_c)
        features[f"z_{sig}"] = features[f"z_{sig}_cohort"]

    # --- Signal exemptions: zero out structurally meaningless signals ---
    exempt_bunching = features["cohort_key"].isin(EXEMPT_BUNCHING)
    features.loc[exempt_bunching, "z_bunching_entity_cohort"] = 0
    features.loc[exempt_bunching, "z_bunching_entity"] = 0

    exempt_single = features["cohort_key"].isin(EXEMPT_SINGLE)
    features.loc[exempt_single, "z_single_bidder_entity_cohort"] = 0
    features.loc[exempt_single, "z_single_bidder_entity"] = 0

    print(f"  Exempted bunching for {exempt_bunching.sum():,} contracts", flush=True)
    print(f"  Exempted single-bidder for {exempt_single.sum():,} contracts", flush=True)

    # --- Compute composite (using cohort z-scores) ---
    z_cols = list(WEIGHTS.keys())
    composite = sum(features[col] * weight for col, weight in WEIGHTS.items())
    features["composite"] = composite
    features["composite_percentile"] = features["composite"].rank(pct=True)

    # Also compute composite_global for comparison
    composite_global = sum(
        features[f"z_{sig}_global"] * weight
        for sig, weight in [
            ("stall", 1.0), ("creep_contract", 1.0), ("creep_contractor", 0.5),
            ("slip_contract", 0.7), ("slip_contractor", 0.5), ("bunching_entity", 0.7),
            ("hhi_entity", 0.5), ("single_bidder_entity", 0.7), ("award_speed_abs", 0.5),
            ("relationship", 0.8), ("fragmentation", 0.7),
        ]
    )
    features["composite_global"] = composite_global

    # Placeholder for context-adjusted composite (populated by phase5_context.py)
    features["composite_adjusted"] = features["composite"]
    features["composite_adjusted_percentile"] = features["composite_percentile"]

    # Add DQ columns to clean scored features
    features["dq_excluded"] = False
    features["dq_flags"] = ""

    # Build stub rows for DQ-excluded contracts
    if len(excluded) > 0:
        excl_rows = excluded[["contract_id", "process_id", "entity_nit", "supplier_id",
                               "awarded_value_cop", "signature_year",
                               "cohort_key", "is_mandato", "is_eice",
                               "dq_excluded", "dq_flags"]].copy()
        excl_rows["composite"] = np.nan
        excl_rows["composite_percentile"] = np.nan
        excl_rows["composite_global"] = np.nan
        excl_rows["composite_adjusted"] = np.nan
        excl_rows["composite_adjusted_percentile"] = np.nan
        excl_rows["s1_not_evaluated"] = True
        # Zero out all z/raw columns
        for col in features.columns:
            if col.startswith(("z_", "raw_")) and col not in excl_rows.columns:
                excl_rows[col] = 0.0
        features = pd.concat([features, excl_rows], ignore_index=True)

    # Save full scored population
    scored_path = DATA_DIR / "anomaly_scored.parquet"
    features.to_parquet(scored_path, index=False, compression="zstd")
    print(f"  Saved {len(features):,} scored contracts to {scored_path}", flush=True)

    # Report cohort distribution of top decile
    top_decile = features[features["composite_percentile"] >= 0.9]
    print(f"\n  Top-decile cohort distribution:", flush=True)
    print(top_decile["cohort_key"].value_counts().to_string(), flush=True)

    # =============================================
    # Step 5.2 — Select demo cohort
    # =============================================
    print("\n[Step 5.2] Selecting demo cohort...", flush=True)

    # Start with geolocated cohort
    geo = pd.read_parquet(DATA_DIR / "cohort_geolocated.parquet")
    geo_ids = set(geo["contract_id"])

    # Merge scores into geolocated cohort
    geo_scored = geo.merge(
        features[["contract_id", "composite", "composite_percentile"] + z_cols],
        on="contract_id", how="left"
    )

    # Take top 150 by composite from the geolocated set
    geo_top = geo_scored.nlargest(min(150, len(geo_scored)), "composite")

    # Check if we have enough in the top decile
    top_decile_threshold = features["composite"].quantile(0.9)
    n_geo_top_decile = (geo_top["composite"] >= top_decile_threshold).sum()
    print(f"  Geolocated contracts in refpop top decile: {n_geo_top_decile}", flush=True)

    # If fewer than 100, supplement with top-scoring nationwide
    if n_geo_top_decile < 100:
        n_supplement = 150 - len(geo_top)
        nationwide = features[
            (~features["contract_id"].isin(geo_ids))
            & (features["composite"] >= top_decile_threshold)
        ].nlargest(n_supplement, "composite")

        # Add municipality centroid coordinates with jitter
        nationwide_info = refpop[refpop["contract_id"].isin(nationwide["contract_id"])].copy()
        nationwide = nationwide.merge(
            nationwide_info[["contract_id", "department_norm", "municipality_norm",
                             "entity_name", "supplier_name", "object_description",
                             "status_raw", "source_record_uri"]],
            on="contract_id", how="left"
        )
        # Use municipality_norm for approximate geocoding (just labels, no coords needed for demo)
        nationwide["lat"] = np.nan
        nationwide["lon"] = np.nan
        nationwide["geocode_tier"] = "none"
        nationwide["geocode_confidence"] = 0.0

        # Combine
        demo = pd.concat([geo_top, nationwide], ignore_index=True)
        print(f"  Supplemented: {len(nationwide)} nationwide contracts added", flush=True)
    else:
        demo = geo_top

    print(f"  Demo cohort size: {len(demo)}", flush=True)

    # Resolve display fields
    demo["value_usd"] = (demo["awarded_value_cop"] / COP_TO_USD).astype(int)
    demo["object_short"] = demo["object_description"].str[:200]
    demo["coords_type"] = np.where(demo["geocode_tier"] == "none", "centroid_jitter", "geocoded")

    # Top 3 firing signals per contract
    signal_names = {
        "z_stall": "Payment stall",
        "z_creep_contract": "Value creep",
        "z_creep_contractor": "Contractor creep",
        "z_slip_contract": "Schedule slip",
        "z_slip_contractor": "Contractor slip",
        "z_bunching_entity": "Threshold bunching",
        "z_hhi_entity": "Contractor concentration",
        "z_single_bidder_entity": "Single bidder",
        "z_award_speed_abs": "Award speed anomaly",
        "z_relationship": "Relationship intensity",
        "z_fragmentation": "Fragmentation",
    }

    def top_signals(row):
        vals = {name: abs(row[col]) for col, name in signal_names.items()}
        top3 = sorted(vals.items(), key=lambda x: x[1], reverse=True)[:3]
        return "; ".join(f"{name} ({v:.1f}σ)" for name, v in top3 if v > 0.5)

    demo["top_3_signals"] = demo.apply(top_signals, axis=1)

    demo_path = DATA_DIR / "demo_cohort.parquet"
    demo.to_parquet(demo_path, index=False, compression="zstd")
    print(f"  Saved demo_cohort.parquet", flush=True)

    # =============================================
    # Step 5.3 — Contractor league table
    # =============================================
    print("\n[Step 5.3] Building contractor league table...", flush=True)

    # Contractors with >=3 contracts in demo OR >=10 in refpop
    # Filter out DQ-excluded contracts for league computation
    features_clean = features[~features["dq_excluded"]].copy()

    demo_contractors = demo.groupby("supplier_id")["contract_id"].count()
    demo_contractors_3plus = set(demo_contractors[demo_contractors >= 3].index)

    refpop_contractors = features_clean.groupby("supplier_id")["contract_id"].count()
    refpop_contractors_10plus = set(refpop_contractors[refpop_contractors >= 10].index)

    eligible = demo_contractors_3plus | refpop_contractors_10plus

    contractor_data = features_clean[features_clean["supplier_id"].isin(eligible)].copy()

    league = contractor_data.groupby("supplier_id").agg(
        portfolio_composite_num=("composite", lambda x: (x * contractor_data.loc[x.index, "awarded_value_cop"]).sum()),
        portfolio_total_value=("awarded_value_cop", "sum"),
        n_contracts_active=("contract_id", "count"),
        n_flagged=("composite_percentile", lambda x: (x >= 0.9).sum()),
    ).reset_index()

    league["portfolio_composite"] = league["portfolio_composite_num"] / league["portfolio_total_value"].clip(lower=1)
    league["total_exposure_cop"] = league["portfolio_total_value"]

    # Get contractor names
    name_map = refpop.groupby("supplier_id")["supplier_name"].first()
    league["supplier_name"] = league["supplier_id"].map(name_map)

    # Top 2 signals across portfolio
    def portfolio_top_signals(supplier_id):
        sub = contractor_data[contractor_data["supplier_id"] == supplier_id]
        signal_means = {}
        for col, name in signal_names.items():
            val = sub[col].abs().mean()
            if not pd.isna(val):
                signal_means[name] = val
        top2 = sorted(signal_means.items(), key=lambda x: x[1], reverse=True)[:2]
        return "; ".join(f"{name}" for name, _ in top2)

    league["top_2_signals"] = league["supplier_id"].apply(portfolio_top_signals)

    league_out = league[["supplier_id", "supplier_name", "portfolio_composite",
                          "n_contracts_active", "total_exposure_cop", "n_flagged",
                          "top_2_signals"]].sort_values("portfolio_composite", ascending=False)

    league_path = DATA_DIR / "demo_contractors.parquet"
    league_out.to_parquet(league_path, index=False, compression="zstd")
    print(f"  Saved {len(league_out)} contractors to demo_contractors.parquet", flush=True)

    # =============================================
    # Phase 5 Report
    # =============================================
    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 5 REPORT", flush=True)
    print(f"{'=' * 60}", flush=True)

    # Score distribution
    print("\n--- Composite Score Distribution (full refpop) ---", flush=True)
    c = features["composite"]
    bins = np.percentile(c.dropna(), [0, 10, 25, 50, 75, 90, 95, 99, 100])
    print(f"  Min: {c.min():.2f}", flush=True)
    print(f"  P10: {bins[1]:.2f}", flush=True)
    print(f"  P25: {bins[2]:.2f}", flush=True)
    print(f"  Median: {bins[3]:.2f}", flush=True)
    print(f"  P75: {bins[4]:.2f}", flush=True)
    print(f"  P90: {bins[5]:.2f}", flush=True)
    print(f"  P95: {bins[6]:.2f}", flush=True)
    print(f"  P99: {bins[7]:.2f}", flush=True)
    print(f"  Max: {c.max():.2f}", flush=True)

    # Demo cohort composition
    n_geo = (demo["coords_type"] == "geocoded").sum()
    n_supp = (demo["coords_type"] == "centroid_jitter").sum()
    print(f"\n--- Demo Cohort ---", flush=True)
    print(f"  Total: {len(demo)}", flush=True)
    print(f"  From geolocated: {n_geo}", flush=True)
    print(f"  Supplemented nationwide: {n_supp}", flush=True)

    # Top 20 contracts
    print(f"\n--- Top 20 Contracts by Composite ---", flush=True)
    for _, r in demo.nlargest(20, "composite").iterrows():
        val = r["awarded_value_cop"] / 1e9
        pct = r["composite_percentile"] * 100
        dept = r.get("department_norm", "?")
        print(f"  {r['contract_id']}: composite={r['composite']:.2f} (P{pct:.0f}), "
              f"{val:.1f}B COP, {dept}", flush=True)
        print(f"    Signals: {r['top_3_signals']}", flush=True)

    # Top 20 contractors
    print(f"\n--- Top 20 Contractors ---", flush=True)
    for _, r in league_out.head(20).iterrows():
        exp = r["total_exposure_cop"] / 1e9
        name = str(r["supplier_name"])[:50]
        print(f"  {name}: composite={r['portfolio_composite']:.2f}, "
              f"{r['n_contracts_active']} contracts, {exp:.1f}B COP, "
              f"{r['n_flagged']} flagged", flush=True)
        print(f"    Top signals: {r['top_2_signals']}", flush=True)

    # Sanity check: any top-10 driven by single signal?
    print(f"\n--- Sanity Check: Signal Concentration in Top 10 ---", flush=True)
    for _, r in demo.nlargest(10, "composite").iterrows():
        z_vals = {col: abs(r[col]) for col in z_cols}
        max_z = max(z_vals.values())
        total_z = sum(z_vals.values())
        dominant_pct = max_z / total_z * 100 if total_z > 0 else 0
        dominant_name = max(z_vals, key=z_vals.get)
        if dominant_pct > 60:
            print(f"  WARNING: {r['contract_id']} — {dominant_name} contributes "
                  f"{dominant_pct:.0f}% of total |z|", flush=True)
        else:
            print(f"  OK: {r['contract_id']} — max signal {dominant_name} = {dominant_pct:.0f}%", flush=True)


if __name__ == "__main__":
    main()
