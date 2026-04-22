"""Phase 5c — Bootstrap Stability Analysis.

Runs 1,000 bootstrap resamples to assess which contracts' top-50/top-100
membership is robust to sampling variation in the reference population.
"""

from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path("data")

# Raw column -> z column mapping with composite weights
SIGNAL_MAP = [
    ("raw_stall", "z_stall", 1.0),
    ("raw_creep_contract", "z_creep_contract", 1.0),
    ("raw_creep_contractor", "z_creep_contractor", 0.5),
    ("raw_slip_contract", "z_slip_contract", 0.7),
    ("raw_slip_contractor", "z_slip_contractor", 0.5),
    ("raw_bunching_entity", "z_bunching_entity", 0.7),
    ("raw_hhi_entity", "z_hhi_entity", 0.5),
    ("raw_single_bidder_entity", "z_single_bidder_entity", 0.7),
    ("raw_award_speed", "z_award_speed_abs", 0.5),  # uses abs
    ("raw_relationship", "z_relationship", 0.8),
    ("raw_fragmentation", "z_fragmentation", 0.7),
]

N_BOOT = 1_000
RNG_SEED = 42


def main():
    print("=" * 60, flush=True)
    print("PHASE 5c — BOOTSTRAP STABILITY", flush=True)
    print("=" * 60, flush=True)

    scored = pd.read_parquet(DATA_DIR / "anomaly_scored.parquet")
    clean = scored[~scored["dq_excluded"].fillna(False)].copy()
    n = len(clean)
    print(f"Clean contracts: {n:,}", flush=True)

    # Extract raw signal arrays
    raw_arrays = {}
    for raw_col, z_col, weight in SIGNAL_MAP:
        vals = clean[raw_col].fillna(0).values.copy()
        if "award_speed" in raw_col:
            vals = np.abs(vals)
        raw_arrays[z_col] = (vals, weight)

    contract_ids = clean["contract_id"].values
    rng = np.random.default_rng(RNG_SEED)

    # Track top-50 and top-100 membership across resamples
    top50_counts = np.zeros(n, dtype=np.int32)
    top100_counts = np.zeros(n, dtype=np.int32)
    composite_samples = np.zeros((n, N_BOOT), dtype=np.float32)

    print(f"Running {N_BOOT} bootstrap resamples...", flush=True)
    for b in range(N_BOOT):
        idx = rng.integers(0, n, size=n)

        composite = np.zeros(n, dtype=np.float64)
        for z_col, (vals, weight) in raw_arrays.items():
            sample = vals[idx]
            mu = sample.mean()
            std = sample.std()
            if std > 0:
                z = np.clip((vals - mu) / std, -5, 5)
            else:
                z = np.zeros(n)
            composite += z * weight

        composite_samples[:, b] = composite

        # Track top-50 and top-100
        top100_idx = np.argpartition(composite, -100)[-100:]
        top100_counts[top100_idx] += 1

        top50_idx = np.argpartition(composite, -50)[-50:]
        top50_counts[top50_idx] += 1

        if (b + 1) % 200 == 0:
            print(f"  {b + 1}/{N_BOOT} resamples done", flush=True)

    # Build output
    top50_freq = top50_counts / N_BOOT
    top100_freq = top100_counts / N_BOOT
    composite_mean = composite_samples.mean(axis=1)
    composite_std = composite_samples.std(axis=1)

    # Flag unstable: contracts that appeared in top-50 at least once but < 90% of the time
    ever_top50 = top50_counts > 0
    ranking_unstable = ever_top50 & (top50_freq < 0.90)

    result = pd.DataFrame({
        "contract_id": contract_ids,
        "top50_frequency": top50_freq,
        "top100_frequency": top100_freq,
        "composite_mean": composite_mean,
        "composite_std": composite_std,
        "ranking_unstable": ranking_unstable,
    })

    out_path = DATA_DIR / "bootstrap_stability.parquet"
    result.to_parquet(out_path, index=False, compression="zstd")

    # Report
    n_ever_top50 = ever_top50.sum()
    n_stable = (ever_top50 & ~ranking_unstable).sum()
    n_unstable = ranking_unstable.sum()
    print(f"\n  Contracts ever in top-50: {n_ever_top50}", flush=True)
    print(f"  Stable (>=90% frequency): {n_stable}", flush=True)
    print(f"  Unstable (<90% frequency): {n_unstable}", flush=True)

    # Show top-10 most stable
    top_stable = result.nlargest(10, "top50_frequency")
    print(f"\n  Top 10 most stable in top-50:", flush=True)
    for _, r in top_stable.iterrows():
        print(f"    {r['contract_id']}: top50={r['top50_frequency']:.1%}, "
              f"composite={r['composite_mean']:.2f} +/- {r['composite_std']:.2f}", flush=True)

    # Show borderline contracts
    borderline = result[(top50_freq > 0.3) & (top50_freq < 0.9)]
    if len(borderline) > 0:
        print(f"\n  Borderline contracts (30-90% top-50 frequency): {len(borderline)}", flush=True)
        for _, r in borderline.nlargest(5, "top50_frequency").iterrows():
            print(f"    {r['contract_id']}: top50={r['top50_frequency']:.1%}", flush=True)

    print(f"\n  Saved {out_path}", flush=True)
    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 5c COMPLETE", flush=True)
    print(f"{'=' * 60}", flush=True)


if __name__ == "__main__":
    main()
