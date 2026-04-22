"""Phase 6 — Out-of-Sample Validation.

Splits scored contracts by signature_year: train=2020-2022, test=2023-2024.
Computes z-scores on train-only statistics, applies to test.
Reports AUC and Spearman rho for outcome proxies.
"""

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

DATA_DIR = Path("data")

SIGNAL_MAP = [
    ("raw_stall", 1.0),
    ("raw_creep_contract", 1.0),
    ("raw_creep_contractor", 0.5),
    ("raw_slip_contract", 0.7),
    ("raw_slip_contractor", 0.5),
    ("raw_bunching_entity", 0.7),
    ("raw_hhi_entity", 0.5),
    ("raw_single_bidder_entity", 0.7),
    ("raw_award_speed", 0.5),  # uses abs
    ("raw_relationship", 0.8),
    ("raw_fragmentation", 0.7),
]


def simple_auc(y_true, y_score):
    """Compute AUC using Mann-Whitney U statistic."""
    pos = y_score[y_true == 1]
    neg = y_score[y_true == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    u_stat = 0
    for p in pos:
        u_stat += (neg < p).sum() + 0.5 * (neg == p).sum()
    return u_stat / (len(pos) * len(neg))


def main():
    print("=" * 60, flush=True)
    print("PHASE 6 — OUT-OF-SAMPLE VALIDATION", flush=True)
    print("=" * 60, flush=True)

    scored = pd.read_parquet(DATA_DIR / "anomaly_scored.parquet")
    clean = scored[~scored["dq_excluded"].fillna(False)].copy()

    # Split by signature year
    train = clean[clean["signature_year"].isin([2020, 2021, 2022])].copy()
    test = clean[clean["signature_year"].isin([2023, 2024])].copy()

    print(f"Train (2020-2022): {len(train):,} contracts", flush=True)
    print(f"Test  (2023-2024): {len(test):,} contracts", flush=True)

    if len(test) < 100:
        print("  Too few test contracts, skipping validation.", flush=True)
        return

    # Compute z-scores on train, apply to test
    composite_test = np.zeros(len(test), dtype=np.float64)
    for raw_col, weight in SIGNAL_MAP:
        train_vals = train[raw_col].fillna(0)
        test_vals = test[raw_col].fillna(0).values.copy()
        if "award_speed" in raw_col:
            train_vals = train_vals.abs()
            test_vals = np.abs(test_vals)
        else:
            train_vals = train_vals.values

        mu = train_vals.mean()
        std = train_vals.std()
        if std > 0:
            z = np.clip((test_vals - mu) / std, -5, 5)
        else:
            z = np.zeros(len(test))
        composite_test += z * weight

    test["composite_oos"] = composite_test

    # Load reference population for outcome proxies
    refpop = pd.read_parquet(DATA_DIR / "reference_population.parquet")
    rp_cols = ["contract_id", "status_raw", "dias_adicionados"]
    if "value_creep_ratio" in refpop.columns:
        rp_cols.append("value_creep_ratio")

    test = test.merge(refpop[rp_cols], on="contract_id", how="left", suffixes=("", "_rp"))

    # Define outcome proxies
    proxies = {}

    # Proxy 1: Terminated/cancelled
    terminated_statuses = {"terminado", "Cancelado", "Terminado anormalmente despues de convocado"}
    test["terminated"] = test["status_raw"].isin(terminated_statuses).astype(int)
    if test["terminated"].sum() > 5:
        proxies["terminated"] = test["terminated"]

    # Proxy 2: Has amendments (extension days > 0)
    test["has_amendments"] = (test["dias_adicionados"].fillna(0) > 0).astype(int)
    if test["has_amendments"].sum() > 5:
        proxies["has_amendments"] = test["has_amendments"]

    # Proxy 3: Large value creep (> 0.3)
    if "raw_creep_contract" in test.columns:
        test["large_creep"] = (test["raw_creep_contract"].fillna(0) > 0.3).astype(int)
        if test["large_creep"].sum() > 5:
            proxies["large_creep"] = test["large_creep"]

    print(f"\n--- Validation Results ---", flush=True)
    for name, y_true in proxies.items():
        y_score = test["composite_oos"].values
        valid = np.isfinite(y_score) & np.isfinite(y_true.values)
        y_s = y_score[valid]
        y_t = y_true.values[valid]

        auc = simple_auc(y_t, y_s)
        rho, p_val = sp_stats.spearmanr(y_s, y_t)
        prevalence = y_t.mean()

        print(f"  {name}:", flush=True)
        print(f"    Prevalence: {prevalence:.3f} ({int(y_t.sum())}/{len(y_t)})", flush=True)
        print(f"    AUC: {auc:.3f}", flush=True)
        print(f"    Spearman rho: {rho:.3f} (p={p_val:.4f})", flush=True)

    # Save test predictions
    test[["contract_id", "composite_oos"]].to_parquet(
        DATA_DIR / "validation_oos.parquet", index=False, compression="zstd"
    )

    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 6 COMPLETE", flush=True)
    print(f"{'=' * 60}", flush=True)


if __name__ == "__main__":
    main()
