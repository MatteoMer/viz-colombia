#!/usr/bin/env python3
"""
Phase 7: Campaign Finance Cross-Reference.

Joins Cuentas Claras 2019 campaign donors with SECOP II contractors
on donor identification (NIT or Cédula) = supplier_id.

Produces:
  - data/campaign_donors.parquet: per-contractor donation summary
  - data/campaign_donor_links.parquet: per-contract donation links

Usage:
    uv run python scripts/phase7_campaign_finance.py
"""
import json
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path("data")
CF_DIR = DATA_DIR / "campaign_finance"


JUNK_IDS = {
    "NO APLICA", "NO DEFINIDO", "SIN INFORMACION", "0", "1",
    "999999999", "9999999999", "123456789", "00000000000001",
}


def normalize_id(s: pd.Series) -> pd.Series:
    """Strip whitespace, remove leading zeros, lowercase."""
    return s.str.strip().str.lstrip("0").replace("", pd.NA)


def main():
    # ── Load data ──
    print("=" * 50)
    print("PHASE 7: CAMPAIGN FINANCE CROSS-REFERENCE")
    print("=" * 50)

    cf = pd.read_parquet(CF_DIR / "cuentas_claras_2019.parquet")
    rp = pd.read_parquet(DATA_DIR / "reference_population.parquet")
    scored = pd.read_parquet(DATA_DIR / "anomaly_scored.parquet")

    print(f"  Cuentas Claras: {len(cf):,} donation records")
    print(f"  Reference population: {len(rp):,} contracts")
    print(f"  Scored contracts: {len(scored):,}")

    # ── Normalize IDs for matching ──
    cf["donor_id_norm"] = normalize_id(cf["ing_identificacion"].astype(str))
    rp["supplier_id_norm"] = normalize_id(rp["supplier_id"].astype(str))

    # Drop rows without usable IDs and filter junk placeholders
    cf = cf.dropna(subset=["donor_id_norm"])
    cf = cf[~cf["ing_identificacion"].str.upper().str.strip().isin(JUNK_IDS)]

    rp_valid = rp[rp["supplier_id"] != "No Definido"].copy()
    rp_valid = rp_valid[~rp_valid["supplier_id"].str.upper().str.strip().isin(JUNK_IDS)]
    rp_valid = rp_valid.dropna(subset=["supplier_id_norm"])
    # Require IDs of at least 5 digits to avoid spurious matches
    rp_valid = rp_valid[rp_valid["supplier_id_norm"].str.len() >= 5]
    cf = cf[cf["donor_id_norm"].str.len() >= 5]

    # ── Aggregate donors ──
    # For each donor, summarize: total donated, candidates funded, positions
    donor_agg = (
        cf.groupby("donor_id_norm")
        .agg(
            donor_name=("nombre_persona", "first"),
            donor_type=("tpe_nombre", "first"),
            id_type=("tid_nombre", "first"),
            total_donated_cop=("ing_valor", "sum"),
            n_donations=("ing_valor", "count"),
            n_candidates=("nombre_candidato", "nunique"),
            candidates_funded=("nombre_candidato", lambda x: "; ".join(sorted(x.unique())[:5])),
            positions_funded=("cnd_nombre", lambda x: ", ".join(sorted(x.unique()))),
            parties=("org_nombre", lambda x: ", ".join(sorted(x.unique())[:3])),
            departments=("dep_nombre", lambda x: ", ".join(sorted(x.unique())[:3])),
        )
        .reset_index()
    )
    print(f"  Unique donors (aggregated): {len(donor_agg):,}")

    # ── Find SECOP contractors who are also campaign donors ──
    supplier_ids = set(rp_valid["supplier_id_norm"].unique())
    donor_ids = set(donor_agg["donor_id_norm"].unique())
    overlap = supplier_ids & donor_ids

    print(f"\n  SECOP supplier IDs: {len(supplier_ids):,}")
    print(f"  Campaign donor IDs: {len(donor_ids):,}")
    print(f"  OVERLAP (donors who are also contractors): {len(overlap):,}")

    # ── Build per-contractor donor summary ──
    matched_donors = donor_agg[donor_agg["donor_id_norm"].isin(overlap)].copy()

    # Map back to original supplier_id (un-normalized)
    id_map = rp_valid.drop_duplicates("supplier_id_norm").set_index("supplier_id_norm")["supplier_id"]
    matched_donors["supplier_id"] = matched_donors["donor_id_norm"].map(id_map)

    # Add supplier name from SECOP
    supplier_names = rp_valid.drop_duplicates("supplier_id").set_index("supplier_id")["supplier_name"]
    matched_donors["supplier_name_secop"] = matched_donors["supplier_id"].map(supplier_names)

    matched_donors.to_parquet(DATA_DIR / "campaign_donors.parquet", index=False)
    print(f"\n  Saved campaign_donors.parquet: {len(matched_donors):,} contractor-donors")

    # ── Build per-donor per-candidate detail ──
    # Structured records for the relationship graph (donor → candidate)
    candidate_detail = (
        cf[cf["donor_id_norm"].isin(overlap)]
        .groupby(["donor_id_norm", "nombre_candidato"])
        .agg(
            position=("cnd_nombre", "first"),
            party=("org_nombre", "first"),
            donated=("ing_valor", "sum"),
        )
        .reset_index()
        .rename(columns={"nombre_candidato": "candidate_name"})
    )
    candidate_detail.to_parquet(DATA_DIR / "campaign_candidates_detail.parquet", index=False)
    print(f"  Saved campaign_candidates_detail.parquet: {len(candidate_detail):,} donor-candidate pairs")

    # ── Build per-contract donor links ──
    # For each contract, check if its supplier is a campaign donor
    contracts_with_donors = rp_valid[rp_valid["supplier_id_norm"].isin(overlap)][
        ["contract_id", "supplier_id", "supplier_id_norm"]
    ].copy()

    # Merge donor details
    contract_links = contracts_with_donors.merge(
        matched_donors[["donor_id_norm", "donor_name", "donor_type",
                        "total_donated_cop", "n_candidates", "candidates_funded",
                        "positions_funded", "parties"]],
        left_on="supplier_id_norm",
        right_on="donor_id_norm",
        how="left",
    )
    contract_links = contract_links.drop(columns=["supplier_id_norm", "donor_id_norm"])
    contract_links.to_parquet(DATA_DIR / "campaign_donor_links.parquet", index=False)
    print(f"  Saved campaign_donor_links.parquet: {len(contract_links):,} contract-donor links")

    # ── Stats ──
    print(f"\n{'='*50}")
    print("CROSS-REFERENCE SUMMARY")
    print(f"{'='*50}")

    # How many scored contracts have a donor link?
    scored_ids = set(scored["contract_id"])
    linked_contract_ids = set(contract_links["contract_id"])
    scored_with_donor = scored_ids & linked_contract_ids
    print(f"  Scored contracts with campaign donor link: {len(scored_with_donor):,} / {len(scored_ids):,}")

    # Among flagged contracts (P90+), how many have donor links?
    flagged = scored[scored["composite_percentile"] >= 0.9]
    flagged_with_donor = set(flagged["contract_id"]) & linked_contract_ids
    print(f"  Flagged (P90+) with donor link: {len(flagged_with_donor):,} / {len(flagged):,}")

    # Top donor-contractors by total donated
    print(f"\n  Top 10 contractor-donors by total donated:")
    top = matched_donors.nlargest(10, "total_donated_cop")
    for _, r in top.iterrows():
        secop_name = r.get("supplier_name_secop", "")
        donor_name = r["donor_name"]
        name = secop_name if secop_name else donor_name
        print(
            f"    {r['supplier_id']:>12s}  {name[:50]:<50s}  "
            f"${r['total_donated_cop']:>15,.0f} COP  "
            f"-> {r['n_candidates']} candidate(s)"
        )

    # By donor type
    print(f"\n  Matched by donor type:")
    for dt, g in matched_donors.groupby("donor_type"):
        print(f"    {dt}: {len(g):,} contractors")

    # Contracts by position funded
    print(f"\n  Contracts linked to donors who funded:")
    for pos in ["Alcaldía", "Gobernación", "Concejo", "Asamblea"]:
        n = contract_links[contract_links["positions_funded"].str.contains(pos, na=False)].shape[0]
        print(f"    {pos}: {n:,} contracts")


if __name__ == "__main__":
    main()
