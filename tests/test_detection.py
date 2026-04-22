"""Tests for detection flags and cohort assignment."""

import sys
from pathlib import Path

# Allow imports from scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from phase4_refpop import (
    is_mandato, is_eice, is_consortium,
    assign_cohort, classify_especial_subtype, classify_object,
)


# ── Mandato detection ─────────────────────────────────────────────

def test_mandato_positive_full():
    assert is_mandato("CONTRATO DE MANDATO SIN REPRESENTACIÓN para...")

def test_mandato_positive_s_slash():
    assert is_mandato("mandato s/ representación para la construcción")

def test_mandato_positive_s_rep():
    assert is_mandato("MANDATO S/REP PARA PAVIMENTACIÓN")

def test_mandato_positive_no_accent():
    assert is_mandato("mandato sin representacion de obras")

def test_mandato_negative_obra():
    assert not is_mandato("CONTRATO DE OBRA para construcción de vía")

def test_mandato_negative_legal():
    assert not is_mandato("mandato para representación legal")

def test_mandato_negative_convenio():
    assert not is_mandato("convenio interadministrativo de cooperación")

def test_mandato_negative_none():
    assert not is_mandato(None)


# ── EICE detection ────────────────────────────────────────────────

def test_eice_positive_desarrollo_territorial():
    assert is_eice("EMPRESA PARA EL DESARROLLO TERRITORIAL DE EL BAGRE")

def test_eice_positive_desarrollo_urbano():
    assert is_eice("Empresa de Desarrollo Urbano de La Ceja")

def test_eice_positive_industrial():
    assert is_eice("EMPRESA INDUSTRIAL Y COMERCIAL DEL ESTADO")

def test_eice_positive_dotted():
    assert is_eice("E.I.C.E. MUNICIPAL")

def test_eice_positive_acronym():
    assert is_eice("MUNICIPIO EICE DE OBRAS")

def test_eice_negative_consorcio():
    assert not is_eice("CONSORCIO DESARROLLO VIAL 2024")

def test_eice_negative_sas():
    assert not is_eice("INGENIERIA Y CONSULTORIA PM&A SAS")

def test_eice_negative_none():
    assert not is_eice(None)


# ── Cohort assignment ─────────────────────────────────────────────

def test_cohort_mandato_takes_priority():
    assert assign_cohort(method="CONTRATACION_DIRECTA", is_mandato=True, is_eice=True) == "mandato"

def test_cohort_eice_without_mandato():
    assert assign_cohort(method="CONTRATACION_DIRECTA", is_mandato=False, is_eice=True) == "eice"

def test_cohort_directa():
    assert assign_cohort(method="CONTRATACION_DIRECTA", is_mandato=False, is_eice=False) == "directa"

def test_cohort_directa_con_ofertas():
    assert assign_cohort(method="CONTRATACION_DIRECTA_CON_OFERTAS", is_mandato=False, is_eice=False) == "directa"

def test_cohort_minima():
    assert assign_cohort(method="MINIMA_CUANTIA", is_mandato=False, is_eice=False) == "minima"

def test_cohort_especial():
    # Without entity_name/object_description, defaults to especial_otro
    assert assign_cohort(method="REGIMEN_ESPECIAL", is_mandato=False, is_eice=False) == "especial_otro"

def test_cohort_especial_con_ofertas():
    assert assign_cohort(method="REGIMEN_ESPECIAL_CON_OFERTAS", is_mandato=False, is_eice=False) == "especial_otro"

def test_cohort_licitacion():
    assert assign_cohort(method="LICITACION_PUBLICA", is_mandato=False, is_eice=False) == "competitive"

def test_cohort_seleccion_abreviada():
    assert assign_cohort(method="SELECCION_ABREVIADA", is_mandato=False, is_eice=False) == "competitive"


# ── Consortium detection ──────────────────────────────��──────────

def test_consortium_consorcio():
    assert is_consortium("CONSORCIO VIAS DEL SUR 2024")

def test_consortium_union_temporal():
    assert is_consortium("UNION TEMPORAL HOSPITAL 2023")

def test_consortium_ut_dot():
    assert is_consortium("U.T. OBRAS CIVILES")

def test_consortium_negative_supplier():
    assert not is_consortium("CONSTRUCTORA ABC SAS")

def test_consortium_negative_none():
    assert not is_consortium(None)


# ── Regimen especial subtyping ───────────────────────────────────

def test_especial_ese():
    result = classify_especial_subtype("E.S.E. HOSPITAL MUNICIPAL", "obra de mantenimiento")
    assert result == "especial_ese"

def test_especial_ese_full():
    result = classify_especial_subtype("Empresa Social del Estado de Iscuande", "")
    assert result == "especial_ese"

def test_especial_universidad():
    result = classify_especial_subtype("UNIVERSIDAD NACIONAL DE COLOMBIA", "obra de laboratorio")
    assert result == "especial_universidad"

def test_especial_d092():
    result = classify_especial_subtype("ALCALDIA DE TUMACO", "decreto 092 solidaridad")
    assert result == "especial_d092"

def test_especial_convenio():
    result = classify_especial_subtype("GOBERNACION", "convenio interadministrativo")
    assert result == "especial_convenio"

def test_especial_otro():
    result = classify_especial_subtype("ENTIDAD CUALQUIERA", "obra publica")
    assert result == "especial_otro"

def test_cohort_especial_subtype():
    """assign_cohort with especial method should sub-classify."""
    result = assign_cohort(
        method="REGIMEN_ESPECIAL",
        is_mandato=False, is_eice=False,
        entity_name="E.S.E. HOSPITAL LOCAL",
        object_description="mantenimiento de sede",
    )
    assert result == "especial_ese"

def test_cohort_especial_universidad():
    result = assign_cohort(
        method="REGIMEN_ESPECIAL_CON_OFERTAS",
        is_mandato=False, is_eice=False,
        entity_name="UNIVERSIDAD DE ANTIOQUIA",
        object_description="",
    )
    assert result == "especial_universidad"


# ── Object classification ─────────────────────────────────────────

def test_classify_road():
    assert classify_object("7210", "pavimentacion de la via principal") == "road_construction"

def test_classify_health():
    assert classify_object("8510", "hospital nivel 2 centro de salud") == "health_infra"

def test_classify_education():
    assert classify_object("7212", "institucion educativa nueva aula") == "education_infra"

def test_classify_recreation():
    assert classify_object("7200", "construccion parque municipal") == "recreation"

def test_classify_water():
    assert classify_object("8310", "acueducto vereda el pinal") == "water_sanitation"

def test_classify_other():
    assert classify_object("7200", "obra civil general") == "other"
