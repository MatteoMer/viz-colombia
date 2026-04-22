"""Tests for consortium decomposition."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from consortium_decompose import build_decomposed_view


def _make_refpop(n=5, consortium_ids=None):
    """Create a minimal refpop DataFrame for testing."""
    if consortium_ids is None:
        consortium_ids = set()
    rows = []
    for i in range(n):
        cid = f"C{i}"
        is_cons = i in consortium_ids
        rows.append({
            "contract_id": cid,
            "supplier_id": f"CONS_{i}" if is_cons else f"SUP_{i}",
            "supplier_name": f"CONSORCIO TEST {i}" if is_cons else f"Supplier {i}",
            "awarded_value_cop": 1_000_000 * (i + 1),
            "entity_nit": "ENT_1",
            "signature_year": 2023,
            "object_description": f"Obra de prueba {i}",
            "is_consortium": is_cons,
        })
    return pd.DataFrame(rows)


def _make_members():
    """Create consortium member data."""
    return pd.DataFrame([
        {"consortium_nit": "CONS_0", "member_nit": "M_A", "member_name": "Firm A", "participation_pct": 0.6},
        {"consortium_nit": "CONS_0", "member_nit": "M_B", "member_name": "Firm B", "participation_pct": 0.4},
        {"consortium_nit": "CONS_2", "member_nit": "M_C", "member_name": "Firm C", "participation_pct": 0.5},
        {"consortium_nit": "CONS_2", "member_nit": "M_D", "member_name": "Firm D", "participation_pct": 0.5},
    ])


# ── Row count conservation ────────────────────────────────────────

def test_no_consortium_passthrough():
    """Without consortium data, all contracts pass through unchanged."""
    refpop = _make_refpop(5)
    result = build_decomposed_view(refpop, None)
    assert len(result) == 5
    assert (result["effective_supplier_id"] == result["supplier_id"]).all()
    assert (result["effective_value_cop"] == result["awarded_value_cop"]).all()


def test_decomposed_row_expansion():
    """Consortium contracts expand into member rows."""
    refpop = _make_refpop(5, consortium_ids={0, 2})
    members = _make_members()
    result = build_decomposed_view(refpop, members)

    # 3 non-consortium pass through, 2 consortium x 2 members each = 7
    assert len(result) == 7


# ── Value conservation ────────────────────────────────────────────

def test_value_conservation():
    """Sum of effective_value_cop for consortium contracts equals original awarded."""
    refpop = _make_refpop(5, consortium_ids={0, 2})
    members = _make_members()
    result = build_decomposed_view(refpop, members)

    # Total value should be conserved
    original_total = refpop["awarded_value_cop"].sum()
    decomposed_total = result["effective_value_cop"].sum()
    assert abs(original_total - decomposed_total) < 1  # float precision


def test_value_conservation_per_contract():
    """Each consortium contract's member values sum to original."""
    refpop = _make_refpop(3, consortium_ids={0})
    members = _make_members()
    result = build_decomposed_view(refpop, members)

    c0_original = refpop[refpop["contract_id"] == "C0"]["awarded_value_cop"].iloc[0]
    c0_expanded = result[result["contract_id"] == "C0"]["effective_value_cop"].sum()
    assert abs(c0_original - c0_expanded) < 1


# ── Non-consortium passthrough ────────────────────────────────────

def test_non_consortium_unchanged():
    """Non-consortium contracts are identical in decomposed view."""
    refpop = _make_refpop(5, consortium_ids={0})
    members = _make_members()
    result = build_decomposed_view(refpop, members)

    non_cons = result[result["contract_id"] == "C1"]
    assert len(non_cons) == 1
    assert non_cons.iloc[0]["effective_supplier_id"] == "SUP_1"
    assert non_cons.iloc[0]["effective_value_cop"] == 2_000_000


# ── Graceful degradation ─────────────────────────────────────────

def test_missing_consortium_data_graceful():
    """Consortium contracts without member data pass through as-is."""
    refpop = _make_refpop(3, consortium_ids={0, 1})
    # Only members for CONS_0, not CONS_1
    members = pd.DataFrame([
        {"consortium_nit": "CONS_0", "member_nit": "M_A", "member_name": "A", "participation_pct": 0.5},
        {"consortium_nit": "CONS_0", "member_nit": "M_B", "member_name": "B", "participation_pct": 0.5},
    ])
    result = build_decomposed_view(refpop, members)

    # CONS_0: 2 member rows; CONS_1: 1 passthrough; C2: 1 passthrough = 4
    assert len(result) == 4

    # CONS_1 should pass through with original supplier_id
    cons1 = result[result["contract_id"] == "C1"]
    assert len(cons1) == 1
    assert cons1.iloc[0]["effective_supplier_id"] == "CONS_1"


def test_empty_consortium_members():
    """Empty consortium members DataFrame treated same as None."""
    refpop = _make_refpop(3, consortium_ids={0})
    members = pd.DataFrame(columns=["consortium_nit", "member_nit", "member_name", "participation_pct"])
    result = build_decomposed_view(refpop, members)
    assert len(result) == 3
