"""Tests for data-quality plausibility filters."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from phase5_composite import apply_dq_filters


def _make_refpop(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal refpop DataFrame from a list of row overrides."""
    defaults = {
        "contract_id": "C001",
        "entity_nit": "ENT1",
        "supplier_doc_type": "NIT",
        "awarded_value_cop": 100_000_000,
        "valor_pagado": 0.0,
        "valor_facturado": 0.0,
        "status_raw": "En ejecucion",
    }
    records = [{**defaults, **r} for r in rows]
    return pd.DataFrame(records)


# ── Check 1: Natural-person mega-contract ────────────────────────

def test_check1_cedula_over_threshold_flagged():
    df = _make_refpop([{"supplier_doc_type": "Cedula Ciudadania", "awarded_value_cop": 600_000_000}])
    result = apply_dq_filters(df)
    assert result.iloc[0]["dq_excluded"]
    assert "natural_person_mega" in result.iloc[0]["dq_flags"]


def test_check1_nit_over_threshold_not_flagged():
    df = _make_refpop([{"supplier_doc_type": "NIT", "awarded_value_cop": 600_000_000}])
    result = apply_dq_filters(df)
    assert not result.iloc[0]["dq_excluded"]


def test_check1_cedula_under_threshold_not_flagged():
    df = _make_refpop([{"supplier_doc_type": "Cedula Ciudadania", "awarded_value_cop": 400_000_000}])
    result = apply_dq_filters(df)
    assert not result.iloc[0]["dq_excluded"]


def test_check1_cedula_extranjeria_flagged():
    df = _make_refpop([{"supplier_doc_type": "Cedula Extranjeria", "awarded_value_cop": 600_000_000}])
    result = apply_dq_filters(df)
    assert result.iloc[0]["dq_excluded"]
    assert "natural_person_mega" in result.iloc[0]["dq_flags"]


# ── Check 2: Entity median deviation ────────────────────────────

def test_check2_entity_with_enough_contracts_outlier_flagged():
    """Entity with 15 contracts median ~100M, one at 15B -> flagged."""
    rows = [{"contract_id": f"C{i:03d}", "entity_nit": "ENT1", "awarded_value_cop": 100_000_000}
            for i in range(15)]
    rows.append({"contract_id": "C_OUTLIER", "entity_nit": "ENT1", "awarded_value_cop": 15_000_000_000})
    df = _make_refpop(rows)
    result = apply_dq_filters(df)
    outlier = result[result["contract_id"] == "C_OUTLIER"].iloc[0]
    assert outlier["dq_excluded"]
    assert "entity_median_outlier" in outlier["dq_flags"]


def test_check2_entity_too_few_contracts_not_flagged():
    """Entity with 8 contracts — below minimum, not flagged even if outlier."""
    rows = [{"contract_id": f"C{i:03d}", "entity_nit": "ENT2", "awarded_value_cop": 100_000_000}
            for i in range(8)]
    rows.append({"contract_id": "C_OUTLIER", "entity_nit": "ENT2", "awarded_value_cop": 15_000_000_000})
    df = _make_refpop(rows)
    result = apply_dq_filters(df)
    outlier = result[result["contract_id"] == "C_OUTLIER"].iloc[0]
    assert "entity_median_outlier" not in outlier["dq_flags"]


def test_check2_value_at_50x_median_not_flagged():
    """Value at 50x median (below 100x threshold) — not flagged."""
    rows = [{"contract_id": f"C{i:03d}", "entity_nit": "ENT1", "awarded_value_cop": 100_000_000}
            for i in range(15)]
    rows.append({"contract_id": "C_EDGE", "entity_nit": "ENT1", "awarded_value_cop": 5_000_000_000})
    df = _make_refpop(rows)
    result = apply_dq_filters(df)
    edge = result[result["contract_id"] == "C_EDGE"].iloc[0]
    assert "entity_median_outlier" not in edge["dq_flags"]


# ── Check 3: Value-vs-payment mismatch ──────────────────────────

def test_check3_high_ratio_eligible_status_flagged():
    """awarded=10B, pagado=100M, status 'En ejecucion' -> flagged."""
    df = _make_refpop([{
        "awarded_value_cop": 10_000_000_000,
        "valor_pagado": 100_000_000,
        "valor_facturado": 50_000_000,
        "status_raw": "En ejecucion",
    }])
    result = apply_dq_filters(df)
    assert result.iloc[0]["dq_excluded"]
    assert "value_payment_mismatch" in result.iloc[0]["dq_flags"]


def test_check3_ineligible_status_not_flagged():
    """Same ratio but status 'Cancelado' -> not flagged."""
    df = _make_refpop([{
        "awarded_value_cop": 10_000_000_000,
        "valor_pagado": 100_000_000,
        "status_raw": "Cancelado",
    }])
    result = apply_dq_filters(df)
    assert "value_payment_mismatch" not in result.iloc[0]["dq_flags"]


def test_check3_ratio_below_threshold_not_flagged():
    """awarded=500M, pagado=100M (5x, below 10x) -> not flagged."""
    df = _make_refpop([{
        "awarded_value_cop": 500_000_000,
        "valor_pagado": 100_000_000,
        "status_raw": "En ejecucion",
    }])
    result = apply_dq_filters(df)
    assert "value_payment_mismatch" not in result.iloc[0]["dq_flags"]


def test_check3_zero_pagado_not_flagged():
    """No payment yet (pagado=0) -> not flagged regardless of value."""
    df = _make_refpop([{
        "awarded_value_cop": 10_000_000_000,
        "valor_pagado": 0,
        "valor_facturado": 0,
        "status_raw": "En ejecucion",
    }])
    result = apply_dq_filters(df)
    assert "value_payment_mismatch" not in result.iloc[0]["dq_flags"]


# ── Combination: multiple flags ──────────────────────────────────

def test_multiple_flags_combined():
    """Contract triggering checks 1+2 should have both flags."""
    # 15 contracts for entity to satisfy min-contracts, plus the outlier
    rows = [{"contract_id": f"C{i:03d}", "entity_nit": "ENT1", "awarded_value_cop": 100_000_000}
            for i in range(15)]
    rows.append({
        "contract_id": "C_MULTI",
        "entity_nit": "ENT1",
        "supplier_doc_type": "Cedula Ciudadania",
        "awarded_value_cop": 15_000_000_000,
    })
    df = _make_refpop(rows)
    result = apply_dq_filters(df)
    multi = result[result["contract_id"] == "C_MULTI"].iloc[0]
    assert multi["dq_excluded"]
    assert "natural_person_mega" in multi["dq_flags"]
    assert "entity_median_outlier" in multi["dq_flags"]


# ── Clean contract ───────────────────────────────────────────────

def test_clean_contract_not_excluded():
    """Normal contract should not be flagged."""
    df = _make_refpop([{
        "supplier_doc_type": "NIT",
        "awarded_value_cop": 200_000_000,
        "valor_pagado": 50_000_000,
        "status_raw": "En ejecucion",
    }])
    result = apply_dq_filters(df)
    assert not result.iloc[0]["dq_excluded"]
    assert result.iloc[0]["dq_flags"] == ""
