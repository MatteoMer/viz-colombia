"""Tests for covariate building (DIVIPOLA crosswalk, municipality covariates, consortium lookup)."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from build_covariates import (
    build_divipola_crosswalk,
    build_municipality_covariates,
    build_consortium_lookup,
    _strip_accents,
    MANUAL_OVERRIDES,
)


# ── Normalization ─────────────────────────────────────────────────

def test_strip_accents_basic():
    assert _strip_accents("Bogotá") == "BOGOTA"


def test_strip_accents_tilde():
    assert _strip_accents("Ñariño") == "NARINO"


def test_strip_accents_spaces():
    assert _strip_accents("  Río Negro  ") == "RIO NEGRO"


# ── DIVIPOLA crosswalk ────────────────────────────────────────────

def test_crosswalk_exact_match(tmp_path, monkeypatch):
    """Exact match returns correct codigo_divipola."""
    import json
    raw_dir = tmp_path / "data" / "covariates" / "raw"
    raw_dir.mkdir(parents=True)

    divipola_data = [
        {"nombre_departamento": "ANTIOQUIA", "nombre_municipio": "MEDELLIN",
         "codigo_municipio": "05001", "codigo_departamento": "05"},
        {"nombre_departamento": "CUNDINAMARCA", "nombre_municipio": "SOACHA",
         "codigo_municipio": "25754", "codigo_departamento": "25"},
    ]
    (raw_dir / "divipola.json").write_text(json.dumps(divipola_data))

    monkeypatch.setattr("build_covariates.RAW_DIR", raw_dir)

    munis = pd.DataFrame({
        "department_norm": ["ANTIOQUIA", "CUNDINAMARCA"],
        "municipality_norm": ["MEDELLIN", "SOACHA"],
    })
    result = build_divipola_crosswalk(munis)
    assert len(result) == 2
    assert result.iloc[0]["codigo_divipola"] == "05001"
    assert result.iloc[1]["codigo_divipola"] == "25754"


def test_crosswalk_substring_match(tmp_path, monkeypatch):
    """Substring match for 'GUADALAJARA DE BUGA' -> 'BUGA'."""
    import json
    raw_dir = tmp_path / "data" / "covariates" / "raw"
    raw_dir.mkdir(parents=True)

    divipola_data = [
        {"nombre_departamento": "VALLE DEL CAUCA", "nombre_municipio": "BUGA",
         "codigo_municipio": "76111", "codigo_departamento": "76"},
    ]
    (raw_dir / "divipola.json").write_text(json.dumps(divipola_data))

    monkeypatch.setattr("build_covariates.RAW_DIR", raw_dir)

    munis = pd.DataFrame({
        "department_norm": ["VALLE DEL CAUCA"],
        "municipality_norm": ["GUADALAJARA DE BUGA"],
    })
    result = build_divipola_crosswalk(munis)
    assert len(result) == 1
    # Should match via manual override or substring
    assert result.iloc[0]["codigo_divipola"] is not None


def test_crosswalk_manual_override(tmp_path, monkeypatch):
    """Manual overrides take precedence."""
    import json
    raw_dir = tmp_path / "data" / "covariates" / "raw"
    raw_dir.mkdir(parents=True)
    (raw_dir / "divipola.json").write_text(json.dumps([]))

    monkeypatch.setattr("build_covariates.RAW_DIR", raw_dir)

    munis = pd.DataFrame({
        "department_norm": ["DISTRITO CAPITAL DE BOGOTA"],
        "municipality_norm": ["BOGOTA"],
    })
    result = build_divipola_crosswalk(munis)
    assert len(result) == 1
    assert result.iloc[0]["codigo_divipola"] == "11001"


def test_crosswalk_missing_graceful(tmp_path, monkeypatch):
    """Missing divipola.json returns empty crosswalk."""
    raw_dir = tmp_path / "data" / "covariates" / "raw"
    raw_dir.mkdir(parents=True)
    # No divipola.json

    monkeypatch.setattr("build_covariates.RAW_DIR", raw_dir)

    munis = pd.DataFrame({
        "department_norm": ["ANTIOQUIA"],
        "municipality_norm": ["MEDELLIN"],
    })
    result = build_divipola_crosswalk(munis)
    assert "codigo_divipola" in result.columns


# ── Consortium lookup ─────────────────────────────────────────────

def test_consortium_participation_sum(tmp_path, monkeypatch):
    """Valid consortium groups should sum participation to ~1.0."""
    import json
    raw_dir = tmp_path / "data" / "covariates" / "raw"
    raw_dir.mkdir(parents=True)

    members = [
        {"codigo_grupo": "NIT1", "nit_participante": "A", "nombre_participante": "Firm A",
         "participacion": "50", "es_lider": "true"},
        {"codigo_grupo": "NIT1", "nit_participante": "B", "nombre_participante": "Firm B",
         "participacion": "50", "es_lider": "false"},
        {"codigo_grupo": "NIT2", "nit_participante": "C", "nombre_participante": "Firm C",
         "participacion": "100", "es_lider": "true"},
    ]
    (raw_dir / "consortium_members.json").write_text(json.dumps(members))
    monkeypatch.setattr("build_covariates.RAW_DIR", raw_dir)

    result = build_consortium_lookup()
    assert len(result) == 3

    # Group NIT1 should sum to 1.0
    grp1 = result[result["consortium_id"] == "NIT1"]
    assert abs(grp1["participation_pct"].sum() - 1.0) < 0.01


def test_consortium_deduplication(tmp_path, monkeypatch):
    """Duplicate (consortium_nit, member_nit) rows should be deduplicated."""
    import json
    raw_dir = tmp_path / "data" / "covariates" / "raw"
    raw_dir.mkdir(parents=True)

    members = [
        {"codigo_grupo": "NIT1", "nit_participante": "A", "participacion": "50"},
        {"codigo_grupo": "NIT1", "nit_participante": "A", "participacion": "50"},  # duplicate
    ]
    (raw_dir / "consortium_members.json").write_text(json.dumps(members))
    monkeypatch.setattr("build_covariates.RAW_DIR", raw_dir)

    result = build_consortium_lookup()
    assert len(result) == 1
