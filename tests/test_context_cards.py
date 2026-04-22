"""Tests for context card generation and adjusted composite."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from phase5_context import (
    generate_thin_market_cards,
    generate_consortium_cards,
    generate_regimen_subtype_cards,
    generate_value_plausibility_cards,
    generate_no_explanation_cards,
    compute_adjusted_composite,
    WEIGHTS,
    CONFIDENCE_MULTIPLIER,
)


def _make_scored(n=3, **overrides):
    """Create minimal scored DataFrame."""
    rows = []
    for i in range(n):
        row = {
            "contract_id": f"C{i}",
            "entity_nit": f"ENT_{i}",
            "supplier_id": f"SUP_{i}",
            "cohort_key": "competitive",
            "is_consortium": False,
            "codigo_divipola": f"0{i}001",
            "dq_excluded": False,
            "composite": 2.0,
            "composite_percentile": 0.8,
        }
        # Add all z-score columns
        for zcol in WEIGHTS:
            row[zcol] = 0.5
        row.update(overrides)
        rows.append(row)
    df = pd.DataFrame(rows)
    # Override per-row if overrides has lists
    return df


# ── Thin-market cards ─────────────────────────────────────────────

def test_thin_market_pdet_high_hhi():
    """PDET municipality with high HHI gets thin_market card."""
    scored = _make_scored(1, z_hhi_entity=2.5, codigo_divipola="52001")
    covariates = pd.DataFrame([{
        "codigo_divipola": "52001",
        "is_pdet": True,
        "is_zomac": False,
        "fiscal_category": 6,
        "distance_to_capital_km": 200,
    }])
    s5 = pd.DataFrame([{"entity_nit": "ENT_0", "hhi": 0.4, "window_end": "2024-12-31"}])

    cards = generate_thin_market_cards(scored, covariates, s5)
    assert "C0" in cards
    assert cards["C0"][0]["type"] == "thin_market"
    assert "PDET" in cards["C0"][0]["explanation"]


def test_thin_market_not_generated_for_urban():
    """Non-PDET, non-ZOMAC, fiscal cat 1 should not get thin_market card."""
    scored = _make_scored(1, z_hhi_entity=2.5, codigo_divipola="05001")
    covariates = pd.DataFrame([{
        "codigo_divipola": "05001",
        "is_pdet": False,
        "is_zomac": False,
        "fiscal_category": 1,
        "distance_to_capital_km": 10,
    }])
    s5 = pd.DataFrame([{"entity_nit": "ENT_0", "hhi": 0.3, "window_end": "2024-12-31"}])

    cards = generate_thin_market_cards(scored, covariates, s5)
    assert "C0" not in cards


def test_thin_market_not_generated_for_low_hhi():
    """PDET but low HHI z-score should not trigger thin_market."""
    scored = _make_scored(1, z_hhi_entity=0.5, z_single_bidder_entity=0.3, codigo_divipola="52001")
    covariates = pd.DataFrame([{
        "codigo_divipola": "52001",
        "is_pdet": True,
        "is_zomac": False,
        "fiscal_category": 6,
    }])
    cards = generate_thin_market_cards(scored, covariates, None)
    assert "C0" not in cards


# ── Consortium cards ──────────────────────────────────────────────

def test_consortium_card_generated():
    """Consortium supplier gets informational card with members."""
    scored = _make_scored(1, is_consortium=True, supplier_id="CONS_1", supplier_name="CONSORCIO ABC")
    members = pd.DataFrame([
        {"consortium_name": "CONSORCIO ABC", "member_nit": "M_A", "member_name": "Firm A", "participation_pct": 0.6},
        {"consortium_name": "CONSORCIO ABC", "member_nit": "M_B", "member_name": "Firm B", "participation_pct": 0.4},
    ])

    cards = generate_consortium_cards(scored, members)
    assert "C0" in cards
    card = cards["C0"][0]
    assert card["type"] == "consortium"
    assert card["confidence"] == "low"
    assert len(card["members"]) == 2


def test_consortium_card_not_for_non_consortium():
    """Non-consortium supplier should not get consortium card."""
    scored = _make_scored(1, is_consortium=False, supplier_name="EMPRESA XYZ")
    members = pd.DataFrame([
        {"consortium_name": "OTHER", "member_nit": "M_A", "member_name": "A", "participation_pct": 1.0},
    ])
    cards = generate_consortium_cards(scored, members)
    assert "C0" not in cards


# ── Regimen subtype cards ─────────────────────────────────────────

def test_regimen_subtype_ese():
    """ESE sub-cohort gets regimen subtype card."""
    scored = _make_scored(1, cohort_key="especial_ese")
    cards = generate_regimen_subtype_cards(scored)
    assert "C0" in cards
    assert cards["C0"][0]["type"] == "regimen_subtype"
    assert "E.S.E." in cards["C0"][0]["headline"]


def test_regimen_subtype_not_for_competitive():
    """Competitive cohort should not get regimen subtype card."""
    scored = _make_scored(1, cohort_key="competitive")
    cards = generate_regimen_subtype_cards(scored)
    assert "C0" not in cards


# ── No-explanation cards ──────────────────────────────────────────

def test_no_explanation_card_when_red_unexplained():
    """Red signals without benign explanation get no_explanation card."""
    scored = _make_scored(1, z_stall=2.0, z_creep_contract=2.5)
    existing_cards: dict[str, list[dict]] = {}  # No existing cards

    cards = generate_no_explanation_cards(scored, existing_cards)
    assert "C0" in cards
    assert cards["C0"][0]["type"] == "no_explanation"
    assert len(cards["C0"][0]["affected_signals"]) >= 2


def test_no_explanation_not_when_explained():
    """Red signals with benign cards should not get no_explanation."""
    scored = _make_scored(1, z_hhi_entity=2.0)
    existing_cards = {
        "C0": [{"type": "thin_market", "confidence": "high",
                 "affected_signals": ["hhi", "single"],
                 "headline": "test", "explanation": "test"}]
    }

    cards = generate_no_explanation_cards(scored, existing_cards)
    # Only the hhi signal was red, and it's explained by thin_market
    # Other signals are at 0.5 (below 1.5 threshold), so no no_explanation card
    assert "C0" not in cards


# ── Adjusted composite ────────────────────────────────────────────

def test_adjusted_composite_applies_multipliers():
    """Adjusted composite should be lower when high-confidence card covers a signal."""
    scored = _make_scored(1)
    # Set all z-scores to 1.0 for simplicity
    for col in WEIGHTS:
        scored[col] = 1.0
    scored["composite"] = sum(WEIGHTS.values())
    scored["composite_percentile"] = 0.9

    # High-confidence card covering hhi signal
    all_cards = {
        "C0": [{"type": "thin_market", "confidence": "high",
                 "affected_signals": ["hhi", "single"],
                 "headline": "test", "explanation": "test"}]
    }

    result = compute_adjusted_composite(scored, all_cards)
    adj = result.iloc[0]["composite_adjusted"]
    raw = result.iloc[0]["composite"]

    # HHI weight=0.5, single weight=0.7 -> both multiplied by 0.25
    expected_reduction = 0.5 * (1 - 0.25) + 0.7 * (1 - 0.25)
    assert adj < raw
    assert abs((raw - adj) - expected_reduction) < 0.01


def test_adjusted_composite_no_cards_unchanged():
    """Without cards, adjusted composite equals raw."""
    scored = _make_scored(1)
    for col in WEIGHTS:
        scored[col] = 1.0
    scored["composite"] = sum(WEIGHTS.values())
    scored["composite_percentile"] = 0.9

    result = compute_adjusted_composite(scored, {})
    assert abs(result.iloc[0]["composite_adjusted"] - result.iloc[0]["composite"]) < 0.01
