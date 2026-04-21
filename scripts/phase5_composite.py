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
}

COP_TO_USD = 4000  # Approximate, prominently documented


def zscore_clip(series: pd.Series) -> pd.Series:
    """Standardize to z-score, clip [-5,5], NaN → 0."""
    mean = series.mean()
    std = series.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    z = ((series - mean) / std).clip(-5, 5).fillna(0)
    return z


def main():
    print("=" * 60, flush=True)
    print("PHASE 5 — COMPOSITE SCORE & DEMO COHORT", flush=True)
    print("=" * 60, flush=True)

    # Load reference population
    refpop = pd.read_parquet(DATA_DIR / "reference_population.parquet")
    print(f"Reference population: {len(refpop):,} contracts", flush=True)

    # =============================================
    # Step 5.1 — Contract-level composite
    # =============================================
    print("\n[Step 5.1] Building contract-level composite...", flush=True)

    features = refpop[["contract_id", "process_id", "entity_nit", "supplier_id",
                        "awarded_value_cop", "signature_year"]].copy()

    # --- S1: Stall (direct per-contract) ---
    s1 = pd.read_parquet(SIGNALS_DIR / "s1_stall.parquet")
    features = features.merge(
        s1[["contract_id", "stall_score"]].rename(columns={"stall_score": "raw_stall"}),
        on="contract_id", how="left"
    )

    # --- S2: Value creep (contract-level + contractor-level) ---
    s2c = pd.read_parquet(SIGNALS_DIR / "s2_value_creep.parquet")
    features = features.merge(
        s2c[["contract_id", "value_creep_ratio"]].rename(columns={"value_creep_ratio": "raw_creep_contract"}),
        on="contract_id", how="left"
    )

    s2k = pd.read_parquet(SIGNALS_DIR / "s2_value_creep_contractor.parquet")
    features = features.merge(
        s2k[["supplier_id", "portfolio_creep_ratio"]].rename(columns={"portfolio_creep_ratio": "raw_creep_contractor"}),
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
        s3k[["supplier_id", "portfolio_slippage_ratio"]].rename(columns={"portfolio_slippage_ratio": "raw_slip_contractor"}),
        on="supplier_id", how="left"
    )

    # --- S4: Bunching (entity inherits for contract's year) ---
    s4 = pd.read_parquet(SIGNALS_DIR / "s4_bunching.parquet")
    # Take max bunching ratio across thresholds for each entity-year
    s4_max = s4.groupby(["entity_nit", "year"])["bunching_ratio"].max().reset_index()
    s4_max.rename(columns={"bunching_ratio": "raw_bunching_entity", "year": "signature_year"}, inplace=True)
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

    # --- Standardize all signals ---
    features["z_stall"] = zscore_clip(features["raw_stall"])
    features["z_creep_contract"] = zscore_clip(features["raw_creep_contract"])
    features["z_creep_contractor"] = zscore_clip(features["raw_creep_contractor"])
    features["z_slip_contract"] = zscore_clip(features["raw_slip_contract"])
    features["z_slip_contractor"] = zscore_clip(features["raw_slip_contractor"])
    features["z_bunching_entity"] = zscore_clip(features["raw_bunching_entity"])
    features["z_hhi_entity"] = zscore_clip(features["raw_hhi_entity"])
    features["z_single_bidder_entity"] = zscore_clip(features["raw_single_bidder_entity"])
    features["z_award_speed_abs"] = zscore_clip(features["raw_award_speed"].abs())
    features["z_relationship"] = zscore_clip(features["raw_relationship"])

    # --- Compute composite ---
    z_cols = list(WEIGHTS.keys())
    composite = sum(features[col] * weight for col, weight in WEIGHTS.items())
    features["composite"] = composite
    features["composite_percentile"] = features["composite"].rank(pct=True)

    # Save full scored population
    scored_path = DATA_DIR / "anomaly_scored.parquet"
    features.to_parquet(scored_path, index=False, compression="zstd")
    print(f"  Saved {len(features):,} scored contracts to {scored_path}", flush=True)

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
    demo_contractors = demo.groupby("supplier_id")["contract_id"].count()
    demo_contractors_3plus = set(demo_contractors[demo_contractors >= 3].index)

    refpop_contractors = features.groupby("supplier_id")["contract_id"].count()
    refpop_contractors_10plus = set(refpop_contractors[refpop_contractors >= 10].index)

    eligible = demo_contractors_3plus | refpop_contractors_10plus

    contractor_data = features[features["supplier_id"].isin(eligible)].copy()

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
