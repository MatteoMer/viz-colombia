"""Phase 5b — Context Card Generation & Adjusted Composite.

Runs after phase5_composite.py, before build_dashboard.py.
Generates per-contract context cards that explain why certain signals fire,
then computes a context-adjusted composite score.

Card types:
  thin_market   — HHI/single-bidder firing in PDET/ZOMAC/fiscal cat 5-6 area
  consortium    — Supplier is a consortium; members listed
  regimen_subtype — Contract is regimen especial sub-cohort
  value_plausibility — Value >P90 or <P10 for (object_category, fiscal_category)
  no_explanation — Red signals with no benign explanation card

Confidence levels:
  high   → 0.25x multiplier on affected signals
  moderate → 0.50x
  low    → 0.85x
  (no card) → 1.0x
"""

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path("data")
COVARIATES_DIR = DATA_DIR / "covariates"
SIGNALS_DIR = DATA_DIR / "signals"

# Composite weights (must match phase5_composite.py)
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

# Map signal z-columns to card-relevant short names
SIGNAL_SHORT = {
    "z_hhi_entity": "hhi",
    "z_single_bidder_entity": "single",
    "z_relationship": "rel",
    "z_fragmentation": "frag",
    "z_bunching_entity": "bunch",
    "z_award_speed_abs": "speed",
    "z_creep_contract": "creep_c",
    "z_creep_contractor": "creep_k",
    "z_slip_contract": "slip_c",
    "z_slip_contractor": "slip_k",
    "z_stall": "stall",
}

# Confidence multipliers
CONFIDENCE_MULTIPLIER = {
    "high": 0.25,
    "moderate": 0.50,
    "low": 0.85,
}


def _load_if_exists(path: Path) -> pd.DataFrame | None:
    if path.exists():
        return pd.read_parquet(path)
    return None


# ── Card generators ───────────────────────────────────────────────


def generate_thin_market_cards(
    scored: pd.DataFrame,
    covariates: pd.DataFrame | None,
    s5: pd.DataFrame | None,
) -> dict[str, list[dict]]:
    """Thin-market card: HHI/single-bidder firing + remote/conflict-affected municipality."""
    cards: dict[str, list[dict]] = {}

    if covariates is None or covariates.empty:
        return cards

    # Build muni context lookup: codigo_divipola -> covariates
    cov_lookup = covariates.set_index("codigo_divipola").to_dict("index")

    # Entity HHI lookup from s5
    entity_hhi: dict[str, float] = {}
    if s5 is not None and not s5.empty:
        hhi_max = s5.groupby("entity_nit")["hhi"].max()
        entity_hhi = hhi_max.to_dict()

    # Peer HHI: precompute per fiscal category
    # Group entities by fiscal category via scored + covariates
    entity_fiscal: dict[str, int | None] = {}
    for _, r in scored.iterrows():
        code = r.get("codigo_divipola")
        if pd.notna(code) and code in cov_lookup:
            fc = cov_lookup[code].get("fiscal_category")
            entity_fiscal[r["entity_nit"]] = fc

    # Build fiscal_category -> list of entity HHIs
    fiscal_hhis: dict[int, list[float]] = {}
    for ent, hhi_val in entity_hhi.items():
        fc = entity_fiscal.get(ent)
        if fc is not None and pd.notna(fc):
            fiscal_hhis.setdefault(int(fc), []).append(hhi_val)

    for _, r in scored.iterrows():
        cid = r["contract_id"]
        code = r.get("codigo_divipola")
        if pd.isna(code) or code not in cov_lookup:
            continue

        ctx = cov_lookup[code]
        is_pdet = ctx.get("is_pdet", False)
        is_zomac = ctx.get("is_zomac", False)
        fc = ctx.get("fiscal_category")
        is_remote = (is_pdet or is_zomac or
                     (pd.notna(fc) and int(fc) >= 5))

        if not is_remote:
            continue

        # Check if HHI or single-bidder signal is firing (z > 1.0)
        hhi_z = abs(r.get("z_hhi_entity", 0))
        single_z = abs(r.get("z_single_bidder_entity", 0))
        if hhi_z <= 1.0 and single_z <= 1.0:
            continue

        # Determine confidence
        # High if peer entities in same fiscal category have similar HHI
        confidence = "moderate"
        ent_hhi = entity_hhi.get(r["entity_nit"])
        if ent_hhi is not None and fc is not None and pd.notna(fc):
            peer_list = fiscal_hhis.get(int(fc), [])
            if len(peer_list) >= 5:
                peer_median = np.median(peer_list)
                # If entity HHI is within 1.5x of peer median, high confidence
                if ent_hhi <= peer_median * 1.5:
                    confidence = "high"

        reasons = []
        if is_pdet:
            reasons.append("PDET municipality (conflict-affected)")
        if is_zomac:
            reasons.append("ZOMAC municipality (post-conflict special zone)")
        if pd.notna(fc) and int(fc) >= 5:
            reasons.append(f"Fiscal category {int(fc)} (small rural municipality)")
        dist = ctx.get("distance_to_capital_km")
        if pd.notna(dist) and dist > 100:
            reasons.append(f"{dist:.0f} km from department capital")

        affected = []
        if hhi_z > 1.0:
            affected.append("hhi")
        if single_z > 1.0:
            affected.append("single")

        card = {
            "type": "thin_market",
            "confidence": confidence,
            "headline": "Geographic thin-market municipality",
            "explanation": (
                f"This entity operates in a structurally thin market. "
                f"{'; '.join(reasons)}. "
                f"High HHI/single-bidder rates are expected in isolated areas with few qualified contractors."
            ),
            "affected_signals": affected,
        }
        cards.setdefault(cid, []).append(card)

    return cards


def generate_consortium_cards(
    scored: pd.DataFrame,
    consortium_members: pd.DataFrame | None,
) -> dict[str, list[dict]]:
    """Informational card listing consortium members."""
    cards: dict[str, list[dict]] = {}

    if consortium_members is None or consortium_members.empty:
        return cards

    # Build consortium name -> members lookup (normalized)
    import unicodedata
    def _norm(s):
        if not isinstance(s, str):
            return ""
        nfkd = unicodedata.normalize("NFKD", s)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()

    name_col = "consortium_name" if "consortium_name" in consortium_members.columns else None
    cons_lookup: dict[str, list[dict]] = {}
    if name_col:
        for _, r in consortium_members.iterrows():
            key = _norm(str(r[name_col]))
            if not key:
                continue
            cons_lookup.setdefault(key, []).append({
                "nit": str(r["member_nit"]),
                "name": str(r.get("member_name", "")),
                "pct": round(float(r.get("participation_pct", 0)) * 100, 1),
            })

    for _, r in scored.iterrows():
        if not r.get("is_consortium", False):
            continue
        cid = r["contract_id"]
        supplier_name_norm = _norm(str(r.get("supplier_name", "")))
        members = cons_lookup.get(supplier_name_norm, [])
        if not members:
            continue

        member_text = "; ".join(
            f"{m['name'][:40]} ({m['pct']}%)" for m in members[:5]
        )
        card = {
            "type": "consortium",
            "confidence": "low",
            "headline": f"Consortium with {len(members)} member firms",
            "explanation": (
                f"This supplier is a consortium/UT. Underlying firms: {member_text}. "
                f"Concentration and relationship signals have been recalculated using "
                f"member-level decomposition."
            ),
            "affected_signals": ["hhi", "rel", "frag"],
            "members": members,
        }
        cards.setdefault(cid, []).append(card)

    return cards


def generate_regimen_subtype_cards(scored: pd.DataFrame) -> dict[str, list[dict]]:
    """Note on regimen especial sub-cohort baseline."""
    cards: dict[str, list[dict]] = {}

    SUBTYPE_LABELS = {
        "especial_ese": "E.S.E. (public health entity)",
        "especial_universidad": "Public university",
        "especial_d092": "Decreto 092 solidarity contract",
        "especial_convenio": "Convenio interadministrativo",
        "especial_otro": "Regimen especial (unclassified)",
    }

    for _, r in scored.iterrows():
        cohort = str(r.get("cohort_key", ""))
        if not cohort.startswith("especial_"):
            continue
        label = SUBTYPE_LABELS.get(cohort, cohort)
        card = {
            "type": "regimen_subtype",
            "confidence": "moderate",
            "headline": f"Regimen especial: {label}",
            "explanation": (
                f"This contract was procured under regimen especial and classified as {label}. "
                f"Z-scores are conditioned on peer contracts of the same sub-type, reducing "
                f"false positives from structural differences in procurement timing and competition."
            ),
            "affected_signals": ["bunch", "hhi", "speed"],
        }
        cards.setdefault(r["contract_id"], []).append(card)

    return cards


def generate_value_plausibility_cards(
    scored: pd.DataFrame,
    refpop: pd.DataFrame,
    covariates: pd.DataFrame | None,
) -> dict[str, list[dict]]:
    """Flag contracts whose value is unusual for (object_category, fiscal_category)."""
    cards: dict[str, list[dict]] = {}

    if covariates is None or covariates.empty:
        return cards
    if "object_category" not in refpop.columns:
        return cards

    # Build fiscal category lookup
    cov_lookup = {}
    if "codigo_divipola" in covariates.columns:
        cov_lookup = covariates.set_index("codigo_divipola")["fiscal_category"].to_dict()

    # Add fiscal category to refpop
    rp = refpop[["contract_id", "awarded_value_cop", "object_category"]].copy()
    if "codigo_divipola" in refpop.columns:
        rp["fiscal_category"] = refpop["codigo_divipola"].map(cov_lookup)
    else:
        return cards

    rp = rp.dropna(subset=["fiscal_category", "object_category"])
    rp["fiscal_category"] = rp["fiscal_category"].astype(int)

    # Compute value distribution per (object_category, fiscal_category)
    group_stats = rp.groupby(["object_category", "fiscal_category"])["awarded_value_cop"].agg(
        p10=lambda x: x.quantile(0.10),
        p90=lambda x: x.quantile(0.90),
        median="median",
        count="count",
    ).reset_index()
    group_stats = group_stats[group_stats["count"] >= 10]  # Need enough data

    stats_lookup = {}
    for _, gs in group_stats.iterrows():
        key = (gs["object_category"], int(gs["fiscal_category"]))
        stats_lookup[key] = {
            "p10": gs["p10"], "p90": gs["p90"],
            "median": gs["median"], "count": int(gs["count"]),
        }

    # Score contracts
    scored_ids = set(scored["contract_id"])
    for _, r in rp.iterrows():
        cid = r["contract_id"]
        if cid not in scored_ids:
            continue
        key = (r["object_category"], int(r["fiscal_category"]))
        if key not in stats_lookup:
            continue

        stats = stats_lookup[key]
        value = r["awarded_value_cop"]
        if value > stats["p90"]:
            direction = "above P90"
            ratio = value / stats["median"] if stats["median"] > 0 else 0
        elif value < stats["p10"]:
            direction = "below P10"
            ratio = stats["median"] / value if value > 0 else 0
        else:
            continue

        card = {
            "type": "value_plausibility",
            "confidence": "low",
            "headline": f"Value {direction} for {r['object_category']} in fiscal cat {int(r['fiscal_category'])}",
            "explanation": (
                f"This contract's value is {direction} compared to {stats['count']} peer contracts "
                f"of the same type ({r['object_category']}) in similar-sized municipalities "
                f"(fiscal category {int(r['fiscal_category'])}). "
                f"Peer median: COP {stats['median']:,.0f}."
            ),
            "affected_signals": ["creep_c"],
        }
        cards.setdefault(cid, []).append(card)

    return cards


def generate_no_explanation_cards(
    scored: pd.DataFrame,
    all_cards: dict[str, list[dict]],
) -> dict[str, list[dict]]:
    """Generate alert card for contracts with red signals but no benign explanation."""
    cards: dict[str, list[dict]] = {}
    RED_THRESHOLD = 1.5  # z-score threshold for "red"

    signal_cols = [c for c in scored.columns if c.startswith("z_") and not c.endswith(("_global", "_cohort"))]

    for _, r in scored.iterrows():
        cid = r["contract_id"]
        if r.get("dq_excluded", False):
            continue

        # Find red signals
        red_signals = []
        for col in signal_cols:
            if abs(r.get(col, 0)) >= RED_THRESHOLD:
                short = SIGNAL_SHORT.get(col, col)
                red_signals.append(short)

        if not red_signals:
            continue

        # Check if existing cards explain any of them
        existing = all_cards.get(cid, [])
        explained = set()
        for card in existing:
            for sig in card.get("affected_signals", []):
                explained.add(sig)

        unexplained = [s for s in red_signals if s not in explained]
        if not unexplained:
            continue

        signal_names = {
            "stall": "Payment Stall", "creep_c": "Value Creep",
            "creep_k": "Contractor Creep", "slip_c": "Schedule Slip",
            "slip_k": "Contractor Slip", "bunch": "Threshold Bunching",
            "hhi": "HHI Concentration", "single": "Single Bidder",
            "speed": "Award Speed", "rel": "Relationship Intensity",
            "frag": "Fragmentation",
        }
        unexplained_names = [signal_names.get(s, s) for s in unexplained]
        card = {
            "type": "no_explanation",
            "confidence": None,
            "headline": f"{len(unexplained)} red signal(s) without contextual explanation",
            "explanation": (
                f"The following signals fire above {RED_THRESHOLD}σ and have no benign contextual "
                f"explanation: {', '.join(unexplained_names)}. These warrant closer investigation."
            ),
            "affected_signals": unexplained,
        }
        cards.setdefault(cid, []).append(card)

    return cards


# ── Adjusted composite ────────────────────────────────────────────


def compute_adjusted_composite(
    scored: pd.DataFrame,
    all_cards: dict[str, list[dict]],
) -> pd.DataFrame:
    """Compute context-adjusted composite using per-signal multipliers.

    For each signal, find the highest-confidence card that covers it,
    and apply the corresponding multiplier.
    """
    # Build per-contract, per-signal multiplier
    signal_cols = list(WEIGHTS.keys())

    multipliers = pd.DataFrame(1.0, index=scored.index, columns=signal_cols)

    cid_to_idx = dict(zip(scored["contract_id"], scored.index))

    for cid, card_list in all_cards.items():
        if cid not in cid_to_idx:
            continue
        idx = cid_to_idx[cid]

        # For each signal, find best (lowest) multiplier from cards that cover it
        sig_best: dict[str, float] = {}
        for card in card_list:
            conf = card.get("confidence")
            if conf not in CONFIDENCE_MULTIPLIER:
                continue
            mult = CONFIDENCE_MULTIPLIER[conf]
            for sig_short in card.get("affected_signals", []):
                # Map short name back to z-column
                for z_col, short in SIGNAL_SHORT.items():
                    if short == sig_short and z_col in signal_cols:
                        current = sig_best.get(z_col, 1.0)
                        sig_best[z_col] = min(current, mult)

        for z_col, mult in sig_best.items():
            multipliers.at[idx, z_col] = mult

    # Compute adjusted composite
    adjusted = sum(
        scored[col] * weight * multipliers[col]
        for col, weight in WEIGHTS.items()
    )
    scored["composite_adjusted"] = adjusted
    scored["composite_adjusted_percentile"] = scored["composite_adjusted"].rank(pct=True)

    # Report impact
    clean = scored[~scored.get("dq_excluded", pd.Series(False, index=scored.index))]
    if len(clean) > 0:
        raw_pctl = clean["composite_percentile"]
        adj_pctl = clean["composite_adjusted_percentile"]
        delta = abs(raw_pctl - adj_pctl)
        n_shifted = (delta > 0.10).sum()
        corr = clean[["composite", "composite_adjusted"]].corr().iloc[0, 1]
        print(f"  Adjustment impact: {n_shifted:,} contracts shifted >10 percentile points", flush=True)
        print(f"  Raw-adjusted correlation: {corr:.4f}", flush=True)

    return scored


# ── Main ──────────────────────────────────────────────────────────


def main():
    print("=" * 60, flush=True)
    print("PHASE 5b — CONTEXT CARDS & ADJUSTED COMPOSITE", flush=True)
    print("=" * 60, flush=True)

    # Load data
    scored = pd.read_parquet(DATA_DIR / "anomaly_scored.parquet")
    print(f"Loaded {len(scored):,} scored contracts", flush=True)

    refpop = pd.read_parquet(DATA_DIR / "reference_population.parquet")

    # Load covariates (graceful if absent)
    covariates = _load_if_exists(COVARIATES_DIR / "municipality_covariates.parquet")
    consortium = _load_if_exists(COVARIATES_DIR / "consortium_members.parquet")
    s5 = _load_if_exists(SIGNALS_DIR / "s5_concentration.parquet")

    # Merge fields from refpop into scored
    merge_cols = ["contract_id"]
    extra_cols = []
    for col in ["is_consortium", "codigo_divipola", "object_category", "supplier_name"]:
        if col in refpop.columns and col not in scored.columns:
            extra_cols.append(col)
    if extra_cols:
        scored = scored.merge(
            refpop[merge_cols + extra_cols],
            on="contract_id", how="left",
        )

    # Generate cards
    all_cards: dict[str, list[dict]] = {}

    print("\n[1] Generating thin-market cards...", flush=True)
    thin_cards = generate_thin_market_cards(scored, covariates, s5)
    for cid, cl in thin_cards.items():
        all_cards.setdefault(cid, []).extend(cl)
    print(f"  {sum(len(v) for v in thin_cards.values())} thin-market cards", flush=True)

    print("[2] Generating consortium cards...", flush=True)
    cons_cards = generate_consortium_cards(scored, consortium)
    for cid, cl in cons_cards.items():
        all_cards.setdefault(cid, []).extend(cl)
    print(f"  {sum(len(v) for v in cons_cards.values())} consortium cards", flush=True)

    print("[3] Generating regimen subtype cards...", flush=True)
    reg_cards = generate_regimen_subtype_cards(scored)
    for cid, cl in reg_cards.items():
        all_cards.setdefault(cid, []).extend(cl)
    print(f"  {sum(len(v) for v in reg_cards.values())} regimen subtype cards", flush=True)

    print("[4] Generating value plausibility cards...", flush=True)
    val_cards = generate_value_plausibility_cards(scored, refpop, covariates)
    for cid, cl in val_cards.items():
        all_cards.setdefault(cid, []).extend(cl)
    print(f"  {sum(len(v) for v in val_cards.values())} value plausibility cards", flush=True)

    print("[5] Generating no-explanation cards...", flush=True)
    no_cards = generate_no_explanation_cards(scored, all_cards)
    for cid, cl in no_cards.items():
        all_cards.setdefault(cid, []).extend(cl)
    print(f"  {sum(len(v) for v in no_cards.values())} no-explanation cards", flush=True)

    # Card distribution
    n_with_cards = len(all_cards)
    card_counts = [len(v) for v in all_cards.values()]
    print(f"\n  Contracts with cards: {n_with_cards:,}", flush=True)
    if card_counts:
        print(f"  Card count distribution: 1={sum(1 for c in card_counts if c==1)}, "
              f"2={sum(1 for c in card_counts if c==2)}, "
              f"3+={sum(1 for c in card_counts if c>=3)}", flush=True)

    # Compute adjusted composite
    print("\n[6] Computing adjusted composite...", flush=True)
    scored = compute_adjusted_composite(scored, all_cards)

    # Save outputs
    # Context cards (sparse JSON — only contracts with cards)
    cards_path = DATA_DIR / "context_cards.json"
    # Convert to serializable format
    cards_json = {cid: cards for cid, cards in all_cards.items()}
    cards_path.write_text(
        json.dumps(cards_json, ensure_ascii=False, indent=None),
        encoding="utf-8",
    )
    print(f"\n  Saved context_cards.json ({len(cards_json):,} contracts)", flush=True)

    # Update anomaly_scored.parquet with adjusted composite
    scored.to_parquet(DATA_DIR / "anomaly_scored.parquet", index=False, compression="zstd")
    print(f"  Updated anomaly_scored.parquet with composite_adjusted", flush=True)

    # Fix 3 diagnostic: thin-market card coverage for vp=0 + HHI contracts
    print("\n[7] Fix 3 diagnostic: thin-market coverage...", flush=True)
    top100 = scored.nlargest(100, "composite_adjusted")
    s1_not_eval = top100.get("s1_not_evaluated", pd.Series(False, index=top100.index))
    hhi_high = top100["z_hhi_entity"].abs() > 1.0
    vp0_hhi = top100[s1_not_eval & hhi_high]
    n_covered = sum(
        1 for _, r in vp0_hhi.iterrows()
        if any(c["type"] == "thin_market" and "hhi" in c.get("affected_signals", [])
               for c in all_cards.get(r["contract_id"], []))
    )
    coverage = n_covered / max(len(vp0_hhi), 1)
    print(f"  Fix 3: vp=0+HHI>1.0 in top-100: {len(vp0_hhi)}, covered: {n_covered} ({coverage:.0%})", flush=True)

    print(f"\n{'=' * 60}", flush=True)
    print("PHASE 5b COMPLETE", flush=True)
    print(f"{'=' * 60}", flush=True)


if __name__ == "__main__":
    main()
