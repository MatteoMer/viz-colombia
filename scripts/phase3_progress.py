"""Phase 3 — Declared-Progress Timeline.

Constructs monthly declared-progress time series for each geolocated contract
using valor_pagado snapshots and amendment events.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path("data")
PARQUET_DIR = DATA_DIR / "parquet"
TODAY = pd.Timestamp("2026-04-01", tz="UTC")  # First of current month


def read_parquet_dir(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    files = sorted(path.glob("**/data.parquet"))
    tables = [pq.ParquetFile(f).read(columns=columns) for f in files]
    return pa.concat_tables(tables, promote_options="default").to_pandas()


def build_suspension_periods(amendments: pd.DataFrame) -> dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]]:
    """Build (start, end) suspension periods per contract from SUSPENSION/REACTIVACION pairs."""
    relevant = amendments[
        amendments["amendment_type_norm"].isin(["SUSPENSION", "REACTIVACION"])
    ].sort_values(["contract_id", "registration_date"])

    periods: dict[str, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}

    for cid, group in relevant.groupby("contract_id"):
        events = list(zip(group["amendment_type_norm"], group["registration_date"]))
        cid_periods = []
        susp_start = None

        for typ, dt in events:
            if typ == "SUSPENSION" and susp_start is None:
                susp_start = dt
            elif typ == "REACTIVACION" and susp_start is not None:
                cid_periods.append((susp_start, dt))
                susp_start = None

        # If suspension without reactivation, assume still suspended
        if susp_start is not None:
            cid_periods.append((susp_start, TODAY))

        if cid_periods:
            periods[cid] = cid_periods

    return periods


def months_in_range(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    """Generate first-of-month timestamps from start to end (inclusive)."""
    start_month = start.to_period("M").to_timestamp().tz_localize("UTC")
    end_month = end.to_period("M").to_timestamp().tz_localize("UTC")
    return list(pd.date_range(start_month, end_month, freq="MS", tz="UTC"))


def is_suspended(month: pd.Timestamp, periods: list[tuple[pd.Timestamp, pd.Timestamp]]) -> bool:
    """Check if a month falls within any suspension period."""
    month_end = month + pd.offsets.MonthEnd(0)
    for s, e in periods:
        if s <= month_end and e >= month:
            return True
    return False


def main():
    print("=" * 60, flush=True)
    print("PHASE 3 — DECLARED-PROGRESS TIMELINE", flush=True)
    print("=" * 60, flush=True)

    # Load geolocated cohort
    cohort = pd.read_parquet(DATA_DIR / "cohort_geolocated.parquet")
    print(f"Loaded {len(cohort)} geolocated contracts", flush=True)
    cids = set(cohort["contract_id"])

    # Load amendments for cohort
    print("Loading amendments...", flush=True)
    amendments = read_parquet_dir(PARQUET_DIR / "amendments")
    amendments = amendments[amendments["contract_id"].isin(cids)]
    print(f"  {len(amendments)} amendments for cohort", flush=True)

    # Build suspension periods
    susp_periods = build_suspension_periods(amendments)
    print(f"  {len(susp_periods)} contracts have suspension periods", flush=True)

    # Build amendment events per contract for the events column
    amend_events: dict[str, list[dict]] = {}
    for _, row in amendments.iterrows():
        cid = row["contract_id"]
        event = {
            "date": row["registration_date"].isoformat(),
            "type": row["amendment_type_norm"],
            "is_suspension": row["amendment_type_norm"] == "SUSPENSION",
            "raw_amendment_type": row["amendment_type_raw"],
        }
        amend_events.setdefault(cid, []).append(event)

    # Count value additions per contract (denominator may have changed)
    value_additions = amendments[amendments["amendment_type_norm"] == "ADICION_VALOR"].groupby("contract_id").size()

    # Build time series
    print("\nBuilding monthly time series...", flush=True)
    rows = []

    for _, c in cohort.iterrows():
        cid = c["contract_id"]
        awarded = c["awarded_value_cop"]
        pagado = c["valor_pagado"] if pd.notna(c["valor_pagado"]) else 0
        status = c["status_raw"]

        # Determine start date
        start = c["contract_start_date"] if pd.notna(c["contract_start_date"]) else c["contract_signature_date"]
        if pd.isna(start):
            continue

        # Determine end date
        end = c["contract_end_date"] if pd.notna(c["contract_end_date"]) else None
        # Add dias_adicionados if available
        if end is not None and pd.notna(c.get("dias_adicionados")) and c["dias_adicionados"] > 0:
            end = end + pd.Timedelta(days=int(c["dias_adicionados"]))

        # Cap at today
        if end is None or end > TODAY:
            end = TODAY

        if start >= end:
            continue

        # Determine current declared progress
        if awarded > 0:
            current_pct = min(pagado / awarded, 1.0)
        else:
            current_pct = 0.0

        # For closed/terminated contracts with 0 pagado, check status
        if current_pct == 0.0 and status in ("Cerrado", "terminado"):
            # Assume completed — valor_pagado just wasn't recorded
            current_pct = 1.0

        # Get suspension periods
        contract_suspensions = susp_periods.get(cid, [])

        # Calculate total suspended months
        susp_months_total = 0
        for s, e in contract_suspensions:
            susp_months_total += max(0, (e - s).days / 30)

        # Generate monthly series
        month_list = months_in_range(start, end)
        if not month_list:
            continue

        # Compute active months (excluding suspensions)
        active_months = []
        for m in month_list:
            if not is_suspended(m, contract_suspensions):
                active_months.append(m)

        n_active = len(active_months)

        # Linear interpolation: 0% at start → current_pct at end, across active months only
        for m in month_list:
            suspended = is_suspended(m, contract_suspensions)

            if m < start.to_period("M").to_timestamp().tz_localize("UTC"):
                pct = 0.0
                active_status = "pre-start"
            elif suspended:
                # During suspension, hold the last active value
                # Find last active month before this one
                prior_active = [am for am in active_months if am < m]
                if prior_active and n_active > 1:
                    idx = active_months.index(prior_active[-1])
                    pct = current_pct * (idx + 1) / n_active
                else:
                    pct = 0.0
                active_status = "suspended"
            elif status in ("Cerrado", "terminado") and m > end - pd.Timedelta(days=31):
                pct = current_pct
                active_status = "closed"
            else:
                # Linear interpolation across active months
                if m in active_months and n_active > 0:
                    idx = active_months.index(m)
                    pct = current_pct * (idx + 1) / n_active
                else:
                    pct = current_pct
                active_status = "active"

            pct = min(pct, 1.0)

            # Compute current contract value (base + any value additions)
            # Since we don't have structured value deltas, we use awarded_value_cop
            current_valor = awarded

            # Estimate pagado at this month
            pagado_est = int(pct * current_valor)

            # Events this month
            month_events = []
            for evt in amend_events.get(cid, []):
                evt_date = pd.Timestamp(evt["date"])
                if evt_date.to_period("M") == m.to_period("M"):
                    month_events.append(evt)

            rows.append({
                "contract_id": cid,
                "month": m.tz_localize(None),  # Store as naive date
                "declared_progress_pct": round(pct, 4),
                "valor_pagado_month_end_cop": pagado_est,
                "valor_contrato_current_cop": int(current_valor),
                "active_status": active_status,
                "events_this_month": json.dumps(month_events) if month_events else "[]",
            })

    progress_df = pd.DataFrame(rows)

    # Save
    out_path = DATA_DIR / "declared_progress.parquet"
    progress_df.to_parquet(out_path, index=False, compression="zstd")
    print(f"\nSaved {len(progress_df)} rows to {out_path}", flush=True)

    # --- Phase Report ---
    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 3 REPORT", flush=True)
    print(f"{'=' * 60}", flush=True)

    n_contracts = progress_df["contract_id"].nunique()
    print(f"\nContract-month rows: {len(progress_df):,}", flush=True)
    print(f"Contracts with timelines: {n_contracts}", flush=True)

    # Duration stats
    durations = progress_df.groupby("contract_id")["month"].agg(["min", "max"])
    durations["months"] = ((durations["max"] - durations["min"]).dt.days / 30).round().astype(int)
    print(f"\n--- Contract Duration (months) ---", flush=True)
    print(f"  Min: {durations['months'].min()}", flush=True)
    print(f"  Median: {durations['months'].median():.0f}", flush=True)
    print(f"  Max: {durations['months'].max()}", flush=True)

    # Suspensions
    has_suspension = progress_df[progress_df["active_status"] == "suspended"]["contract_id"].nunique()
    print(f"\nContracts with at least one suspended month: {has_suspension}", flush=True)

    # Contracts at 100%
    final_pct = progress_df.sort_values("month").groupby("contract_id")["declared_progress_pct"].last()
    at_100 = (final_pct >= 0.99).sum()
    print(f"Contracts at 100% declared progress: {at_100}", flush=True)

    # Status distribution at end
    final_status = progress_df.sort_values("month").groupby("contract_id")["active_status"].last()
    print(f"\n--- Final Active Status ---", flush=True)
    for status, count in final_status.value_counts().items():
        print(f"  {status}: {count}", flush=True)

    # ASCII sparklines for 3 example contracts
    print(f"\n--- Example Timelines (ASCII sparkline) ---", flush=True)
    sparkline_chars = " ▁▂▃▄▅▆▇█"

    sample_cids = progress_df["contract_id"].unique()[:3]
    for cid in sample_cids:
        ts = progress_df[progress_df["contract_id"] == cid].sort_values("month")
        pcts = ts["declared_progress_pct"].values
        statuses = ts["active_status"].values

        # Build sparkline
        spark = ""
        for p, s in zip(pcts, statuses):
            if s == "suspended":
                spark += "×"
            else:
                idx = min(int(p * 8), 8)
                spark += sparkline_chars[idx]

        # Get contract value
        val = ts["valor_contrato_current_cop"].iloc[0] / 1e9
        final = pcts[-1] * 100

        print(f"\n  {cid} ({val:.1f}B COP, {len(pcts)} months)", flush=True)
        print(f"  [{spark}] {final:.0f}%", flush=True)
        print(f"  Status: {statuses[0]} → {statuses[-1]}", flush=True)


if __name__ == "__main__":
    main()
