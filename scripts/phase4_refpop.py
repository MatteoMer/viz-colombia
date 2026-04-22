"""Phase 4 — Reference Population & Lookups.

Builds the reference universe of all Obra contracts 2020-2024
and supporting lookup tables for signal computation.
"""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# ── Detection regexes ─────────────────────────────────────────────

MANDATO_RE = re.compile(
    r"mandato\s+sin\s+representaci[oó]n"
    r"|mandato\s+s/\s?representaci[oó]n"
    r"|mandato\s+s/\s?rep(?!resentaci)",
    re.IGNORECASE,
)

EICE_RE = re.compile(
    r"empresa\s+para\s+el\s+desarrollo"
    r"|empresa\s+industrial\s+y\s+comercial"
    r"|empresa\s+de\s+desarrollo"
    r"|\bEICE\b"
    r"|E\.I\.C\.E",
    re.IGNORECASE,
)

CONSORTIUM_RE = re.compile(
    r"^(?:CONSORCIO|UNION\s+TEMPORAL|U\.?T\.?\s|PROMESA\s+DE\s+SOCIEDAD\s+FUTURA)",
    re.IGNORECASE,
)

# Regimen especial sub-cohort regexes
ESE_RE = re.compile(r"E\.S\.E\.|empresa\s+social\s+del\s+estado", re.IGNORECASE)
UNIVERSIDAD_RE = re.compile(r"universidad|institucion\s+universitaria", re.IGNORECASE)
D092_RE = re.compile(r"decreto\s+092|solidaridad", re.IGNORECASE)
CONVENIO_RE = re.compile(r"convenio\s+interadministrativo", re.IGNORECASE)

# Object classification regexes
OBJ_HEALTH_RE = re.compile(r"hospital|centro\s+de\s+salud|puesto\s+de\s+salud|E\.S\.E\.", re.IGNORECASE)
OBJ_ROAD_RE = re.compile(r"pavimento|placa\s+huella|v[ií]a\b|carretera|puente", re.IGNORECASE)
OBJ_EDU_RE = re.compile(r"colegio|instituci[oó]n\s+educativa|aula|escuela", re.IGNORECASE)
OBJ_RECREATION_RE = re.compile(r"parque|cancha|polideportivo|recreaci[oó]n", re.IGNORECASE)
OBJ_WATER_RE = re.compile(r"acueducto|alcantarillado|agua\s+potable|saneamiento", re.IGNORECASE)


def is_mandato(text: str) -> bool:
    """Detect mandato sin representación contracts."""
    if not isinstance(text, str):
        return False
    return bool(MANDATO_RE.search(text))


def is_eice(text: str) -> bool:
    """Detect EICE (empresa industrial y comercial del estado) suppliers."""
    if not isinstance(text, str):
        return False
    return bool(EICE_RE.search(text))


def is_consortium(text: str) -> bool:
    """Detect consortium/UT suppliers by name prefix."""
    if not isinstance(text, str):
        return False
    return bool(CONSORTIUM_RE.search(text.strip()))


def classify_especial_subtype(entity_name: str, object_desc: str) -> str:
    """Sub-classify regimen especial contracts."""
    en = entity_name if isinstance(entity_name, str) else ""
    od = object_desc if isinstance(object_desc, str) else ""
    combined = en + " " + od
    if ESE_RE.search(en):
        return "especial_ese"
    if UNIVERSIDAD_RE.search(en):
        return "especial_universidad"
    if D092_RE.search(od):
        return "especial_d092"
    if CONVENIO_RE.search(combined):
        return "especial_convenio"
    return "especial_otro"


def classify_object(category_4digit: str, object_desc: str) -> str:
    """Classify contract object into semantic category."""
    cat = str(category_4digit) if isinstance(category_4digit, str) else ""
    desc = object_desc if isinstance(object_desc, str) else ""

    # UNSPSC prefix + keyword match
    if cat.startswith("85") and OBJ_HEALTH_RE.search(desc):
        return "health_infra"
    if OBJ_HEALTH_RE.search(desc) and cat.startswith("72"):
        return "health_infra"
    if cat.startswith("72") and OBJ_ROAD_RE.search(desc):
        return "road_construction"
    if OBJ_ROAD_RE.search(desc):
        return "road_construction"
    if cat.startswith("72") and OBJ_EDU_RE.search(desc):
        return "education_infra"
    if OBJ_EDU_RE.search(desc):
        return "education_infra"
    if OBJ_RECREATION_RE.search(desc):
        return "recreation"
    if (cat.startswith("83") or cat.startswith("72")) and OBJ_WATER_RE.search(desc):
        return "water_sanitation"
    if OBJ_WATER_RE.search(desc):
        return "water_sanitation"
    return "other"


def assign_cohort(row=None, *, method=None, is_mandato=False, is_eice=False,
                   entity_name="", object_description=""):
    """Assign procurement cohort key for z-score conditioning.

    Can be called with a DataFrame row or with keyword arguments directly.
    Regimen especial contracts get sub-cohort keys (especial_ese, etc.).
    """
    if row is not None:
        _is_mandato = row["is_mandato"]
        _is_eice = row["is_eice"]
        _method = row["procurement_method_norm"]
        _entity = row.get("entity_name", "") or ""
        _desc = row.get("object_description", "") or ""
    else:
        _is_mandato = is_mandato
        _is_eice = is_eice
        _method = method
        _entity = entity_name
        _desc = object_description

    if _is_mandato:
        return "mandato"
    if _is_eice:
        return "eice"
    if _method in ("CONTRATACION_DIRECTA", "CONTRATACION_DIRECTA_CON_OFERTAS"):
        return "directa"
    if _method == "MINIMA_CUANTIA":
        return "minima"
    if _method in ("REGIMEN_ESPECIAL", "REGIMEN_ESPECIAL_CON_OFERTAS"):
        return classify_especial_subtype(_entity, _desc)
    return "competitive"

DATA_DIR = Path("data")
PARQUET_DIR = DATA_DIR / "parquet"
SIGNALS_DIR = DATA_DIR / "signals"
RAW_DIR = DATA_DIR / "raw"


def read_parquet_dir(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    files = sorted(path.glob("**/data.parquet"))
    tables = [pq.ParquetFile(f).read(columns=columns) for f in files]
    return pa.concat_tables(tables, promote_options="default").to_pandas()


def build_reference_population():
    """All Obra contracts with fecha_firma 2020-01-01 to 2024-12-31."""
    print("Loading all contracts...", flush=True)
    contract_cols = [
        "contract_id", "process_id", "contract_type_raw", "entity_nit",
        "entity_name", "entity_level", "department_norm", "municipality_norm",
        "object_description", "procurement_method_raw", "procurement_method_norm",
        "estimated_value_cop", "awarded_value_cop", "contract_signature_date",
        "contract_start_date", "contract_end_date", "status_raw", "status_norm",
        "supplier_id", "supplier_name", "supplier_doc_type", "category_code",
        "valor_pagado", "valor_facturado", "valor_pendiente_pago",
        "dias_adicionados", "source_record_uri",
    ]
    contracts = read_parquet_dir(PARQUET_DIR / "contracts", columns=contract_cols)
    print(f"  Total contracts loaded: {len(contracts):,}", flush=True)

    # Filter: Obra, signed 2020–2024
    mask = (
        (contracts["contract_type_raw"] == "Obra")
        & (contracts["contract_signature_date"] >= "2020-01-01")
        & (contracts["contract_signature_date"] <= "2024-12-31")
    )
    refpop = contracts[mask].copy()
    print(f"  Obra contracts 2020-2024: {len(refpop):,}", flush=True)

    # Derived fields
    refpop["signature_year"] = refpop["contract_signature_date"].dt.year

    # Original duration in days (before extensions)
    refpop["original_duration_days"] = (
        (refpop["contract_end_date"] - refpop["contract_start_date"]).dt.days
        - refpop["dias_adicionados"].fillna(0)
    ).clip(lower=1)

    # Current progress ratio (snapshot)
    refpop["progress_pct"] = np.where(
        refpop["awarded_value_cop"] > 0,
        (refpop["valor_pagado"].fillna(0) / refpop["awarded_value_cop"]).clip(0, 1),
        0.0,
    )

    # Value creep: use estimated_value_cop from contracts as original baseline
    # awarded_value_cop is the current/final value
    refpop["value_creep_ratio"] = np.where(
        (refpop["estimated_value_cop"].notna()) & (refpop["estimated_value_cop"] > 0),
        (refpop["awarded_value_cop"] - refpop["estimated_value_cop"]) / refpop["estimated_value_cop"],
        np.nan,
    )

    # Slippage ratio
    refpop["slippage_ratio"] = np.where(
        refpop["original_duration_days"] > 0,
        refpop["dias_adicionados"].fillna(0) / refpop["original_duration_days"],
        0.0,
    )

    # Months since start (for stall computation)
    now = pd.Timestamp("2026-04-01", tz="UTC")
    refpop["months_since_start"] = (
        (now - refpop["contract_start_date"]).dt.days / 30
    ).clip(lower=0)

    # UNSPSC 4-digit category
    refpop["category_4digit"] = refpop["category_code"].str.extract(
        r"V1\.(\d{4})", expand=False
    )

    # Detection flags
    refpop["is_mandato"] = refpop["object_description"].apply(is_mandato)
    refpop["is_eice"] = refpop["supplier_name"].apply(is_eice)
    refpop["is_consortium"] = refpop["supplier_name"].apply(is_consortium)
    refpop["cohort_key"] = refpop.apply(assign_cohort, axis=1)

    # Object classification
    refpop["object_category"] = refpop.apply(
        lambda r: classify_object(r.get("category_4digit", ""), r.get("object_description", "")),
        axis=1,
    )

    # DIVIPOLA crosswalk (graceful if covariate data absent)
    crosswalk_path = Path("data/covariates/divipola_crosswalk.parquet")
    if crosswalk_path.exists():
        crosswalk = pd.read_parquet(crosswalk_path)
        refpop = refpop.merge(
            crosswalk[["department_norm", "municipality_norm", "codigo_divipola"]],
            on=["department_norm", "municipality_norm"],
            how="left",
        )
        n_matched = refpop["codigo_divipola"].notna().sum()
        print(f"  DIVIPOLA matched: {n_matched:,}/{len(refpop):,} contracts", flush=True)
    else:
        refpop["codigo_divipola"] = None
        print("  DIVIPOLA crosswalk not found, skipping", flush=True)

    print(f"  Mandato contracts: {refpop['is_mandato'].sum():,}", flush=True)
    print(f"  EICE contracts: {refpop['is_eice'].sum():,}", flush=True)
    print(f"  Consortium contracts: {refpop['is_consortium'].sum():,}", flush=True)
    print(f"  Cohort distribution:", flush=True)
    print(refpop["cohort_key"].value_counts().to_string(), flush=True)
    print(f"  Object category distribution:", flush=True)
    print(refpop["object_category"].value_counts().to_string(), flush=True)

    return refpop


def build_lookups(refpop: pd.DataFrame):
    """Build supporting lookup tables."""

    # 1. Entity-year budget proxy (sum of contracts signed per entity per year)
    print("Building entity_year_budget...", flush=True)
    # Use ALL contract types for entity budget, not just Obra
    all_contracts = read_parquet_dir(
        PARQUET_DIR / "contracts",
        columns=["entity_nit", "contract_signature_date", "awarded_value_cop"],
    )
    all_contracts["year"] = all_contracts["contract_signature_date"].dt.year
    all_contracts = all_contracts[
        (all_contracts["year"] >= 2020) & (all_contracts["year"] <= 2024)
    ]
    entity_budget = (
        all_contracts.groupby(["entity_nit", "year"])["awarded_value_cop"]
        .agg(["sum", "count"])
        .rename(columns={"sum": "total_awarded_cop", "count": "n_contracts"})
        .reset_index()
    )
    entity_budget.to_parquet(
        DATA_DIR / "entity_year_budget.parquet", index=False, compression="zstd"
    )
    print(f"  {len(entity_budget):,} entity-year rows", flush=True)

    # 2. Category medians
    print("Building category_medians...", flush=True)
    valid = refpop[refpop["category_4digit"].notna()].copy()
    cat_medians = (
        valid.groupby("category_4digit")
        .agg(
            median_value_cop=("awarded_value_cop", "median"),
            median_duration_days=("original_duration_days", "median"),
            n_contracts=("contract_id", "count"),
        )
        .reset_index()
    )
    cat_medians.to_parquet(
        DATA_DIR / "category_medians.parquet", index=False, compression="zstd"
    )
    print(f"  {len(cat_medians):,} categories", flush=True)

    # 3. Contractor portfolio
    print("Building contractor_portfolio...", flush=True)
    portfolio = (
        refpop.groupby("supplier_id")
        .agg(
            n_contracts=("contract_id", "count"),
            total_value_cop=("awarded_value_cop", "sum"),
            contract_ids=("contract_id", list),
            mean_value_creep=("value_creep_ratio", "mean"),
            mean_slippage=("slippage_ratio", "mean"),
        )
        .reset_index()
    )
    # Add contractor name (most common)
    name_map = refpop.groupby("supplier_id")["supplier_name"].agg(
        lambda x: x.value_counts().index[0] if len(x) > 0 else ""
    )
    portfolio["supplier_name"] = portfolio["supplier_id"].map(name_map)
    portfolio.to_parquet(
        DATA_DIR / "contractor_portfolio.parquet", index=False, compression="zstd"
    )
    print(f"  {len(portfolio):,} contractors", flush=True)

    return entity_budget, cat_medians, portfolio


def extract_bid_counts():
    """Extract bid counts from raw JSON for Obra processes.

    Reads respuestas_al_procedimiento from raw process JSONs.
    """
    print("Extracting bid counts from raw JSON...", flush=True)
    raw_dir = RAW_DIR / "secop2_procesos"
    batch_files = sorted(raw_dir.glob("batch_*.json"))
    # Skip manifest files
    batch_files = [f for f in batch_files if "manifest" not in f.name]

    rows = []
    for i, bf in enumerate(batch_files):
        if i % 20 == 0:
            print(f"  Processing batch {i + 1}/{len(batch_files)}...", flush=True)
        with open(bf) as f:
            data = json.load(f)
        for row in data:
            # Only Obra processes
            if row.get("tipo_de_contrato") != "Obra":
                continue
            pid = row.get("id_del_portafolio", "")
            bids = int(row.get("respuestas_al_procedimiento", 0) or 0)
            unique_bidders = int(row.get("proveedores_unicos_con", 0) or 0)
            method = row.get("modalidad_de_contratacion", "")
            entity_nit = str(row.get("nit_entidad", ""))
            pub_date = row.get("fecha_de_publicacion_del", "")
            rows.append({
                "process_id": pid,
                "bid_count": max(bids, unique_bidders),
                "procurement_method_raw": method,
                "entity_nit": entity_nit,
                "publication_year": pub_date[:4] if pub_date else "",
            })

    bid_df = pd.DataFrame(rows)
    bid_df.to_parquet(
        DATA_DIR / "obra_bid_counts.parquet", index=False, compression="zstd"
    )
    print(f"  {len(bid_df):,} Obra process bid counts extracted", flush=True)
    return bid_df


def main():
    print("=" * 60, flush=True)
    print("PHASE 4 — REFERENCE POPULATION & LOOKUPS", flush=True)
    print("=" * 60, flush=True)

    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    # Build reference population
    refpop = build_reference_population()
    refpop.to_parquet(
        DATA_DIR / "reference_population.parquet", index=False, compression="zstd"
    )
    print(f"\nSaved reference_population.parquet: {len(refpop):,} rows", flush=True)

    # Build lookups
    build_lookups(refpop)

    # Extract bid counts from raw JSON
    extract_bid_counts()

    # Summary
    print(f"\n{'=' * 60}", flush=True)
    print("REFERENCE POPULATION SUMMARY", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"Total Obra contracts 2020-2024: {len(refpop):,}", flush=True)
    print(f"\n--- By Year ---", flush=True)
    print(refpop["signature_year"].value_counts().sort_index().to_string(), flush=True)
    print(f"\n--- By Status ---", flush=True)
    print(refpop["status_raw"].value_counts().to_string(), flush=True)
    print(f"\n--- Value Stats ---", flush=True)
    print(f"  Median: {refpop['awarded_value_cop'].median():,.0f} COP", flush=True)
    print(f"  Mean: {refpop['awarded_value_cop'].mean():,.0f} COP", flush=True)
    print(f"  P95: {refpop['awarded_value_cop'].quantile(0.95):,.0f} COP", flush=True)
    print(f"\n--- Value Creep (where estimable) ---", flush=True)
    vc = refpop["value_creep_ratio"].dropna()
    print(f"  N: {len(vc):,}, Median: {vc.median():.3f}, P95: {vc.quantile(0.95):.3f}", flush=True)
    print(f"\n--- Slippage ---", flush=True)
    sl = refpop["slippage_ratio"]
    print(f"  N with extensions: {(sl > 0).sum():,}", flush=True)
    print(f"  Median (>0): {sl[sl > 0].median():.3f}", flush=True)


if __name__ == "__main__":
    main()
