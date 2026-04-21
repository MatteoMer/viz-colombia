#!/usr/bin/env python3
"""
Convert raw SECOP II JSON batches to normalized Parquet tables.

Produces three tables partitioned by (source, year):
  - processes  (from secop2_procesos)
  - contracts  (from secop2_contratos)
  - amendments (from secop2_adiciones)
"""
import json
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

RAW_DIR = Path("data/raw")
PARQUET_DIR = Path("data/parquet")
YEARS = range(2019, 2025)

SENTINEL_VALUES = frozenset({
    "No Definido", "No definido", "No Aplica", "No aplica",
    "Sin Descripcion", "No Adjudicado", "No Especificado",
    "No definido ", "NO DEFINIDO",
})

# ---------------------------------------------------------------------------
# Enum mappings (versioned — v1)
# ---------------------------------------------------------------------------
PROCUREMENT_METHOD_MAP = {
    "Contratación directa": "CONTRATACION_DIRECTA",
    "Contratación Directa (con ofertas)": "CONTRATACION_DIRECTA_CON_OFERTAS",
    "Contratación régimen especial": "REGIMEN_ESPECIAL",
    "Contratación régimen especial (con ofertas)": "REGIMEN_ESPECIAL_CON_OFERTAS",
    "Mínima cuantía": "MINIMA_CUANTIA",
    "Selección Abreviada de Menor Cuantía": "SELECCION_ABREVIADA_MENOR_CUANTIA",
    "Selección abreviada subasta inversa": "SUBASTA_INVERSA",
    "Licitación pública": "LICITACION_PUBLICA",
    "Licitación pública Obra Publica": "LICITACION_PUBLICA_OBRA",
    "Licitación Pública Acuerdo Marco de Precios": "ACUERDO_MARCO_PRECIOS",
    "Concurso de méritos abierto": "CONCURSO_MERITOS",
    "Concurso de méritos con precalificación": "CONCURSO_MERITOS_PRECALIFICACION",
    "Solicitud de información a los Proveedores": "SOLICITUD_INFORMACION",
    "Enajenación de bienes con subasta": "ENAJENACION_SUBASTA",
    "Enajenación de bienes con sobre cerrado": "ENAJENACION_SOBRE_CERRADO",
    "Subasta de prueba": "SUBASTA_PRUEBA",
    "Seleccion Abreviada Menor Cuantia Sin Manifestacion Interes": "SELECCION_ABREVIADA_SIN_MANIFESTACION",
}

STATUS_PROC_MAP = {
    "Seleccionado": "SELECCIONADO",
    "Publicado": "PUBLICADO",
    "Evaluación": "EVALUACION",
    "Cancelado": "CANCELADO",
    "Abierto": "ABIERTO",
    "Aprobado": "APROBADO",
    "Borrador": "BORRADOR",
    "Descartado": "DESCARTADO",
    "Adjudicado": "ADJUDICADO",
    "Cerrado": "CERRADO",
}

STATUS_CONTR_MAP = {
    "Cerrado": "CERRADO",
    "Aprobado": "APROBADO",
    "Modificado": "MODIFICADO",
    "terminado": "TERMINADO",
    "Terminado": "TERMINADO",
    "cedido": "CEDIDO",
    "Cedido": "CEDIDO",
    "Borrador": "BORRADOR",
    "Suspendido": "SUSPENDIDO",
    "enviado Proveedor": "ENVIADO_PROVEEDOR",
    "En aprobación": "EN_APROBACION",
    "Liquidado": "LIQUIDADO",
    "Cancelado": "CANCELADO",
    "En ejecución": "EN_EJECUCION",
}

AMENDMENT_TYPE_MAP = {
    "MODIFICACION GENERAL": "MODIFICACION_GENERAL",
    "CONCLUSION": "CONCLUSION",
    "No definido": "NO_DEFINIDO",
    "ADICION EN EL VALOR": "ADICION_VALOR",
    "CESION": "CESION",
    "REACTIVACIoN": "REACTIVACION",
    "SUSPENSIoN": "SUSPENSION",
    "EXTENSION": "EXTENSION",
    "REDUCCION EN EL VALOR": "REDUCCION_VALOR",
    "EXPIRACION": "EXPIRACION",
}


def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return s
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_geo(s):
    if not isinstance(s, str) or s in SENTINEL_VALUES:
        return None
    return strip_accents(s).upper().strip()


def clean_sentinels(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace(SENTINEL_VALUES, np.nan)
            # Also catch trimmed variants
            mask = df[col].apply(lambda x: isinstance(x, str) and x.strip() in SENTINEL_VALUES)
            df.loc[mask, col] = np.nan
    return df


def safe_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def safe_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def map_enum(series: pd.Series, mapping: dict, name: str) -> pd.Series:
    unmapped = set(series.dropna().unique()) - set(mapping.keys()) - SENTINEL_VALUES
    if unmapped:
        print(f"  WARNING: unmapped {name} values: {unmapped}", file=sys.stderr)
    return series.map(mapping)


def extract_url(val):
    if isinstance(val, dict):
        return val.get("url")
    if isinstance(val, str):
        return val
    return None


def load_year_batches(dataset_dir: Path, year: int) -> pd.DataFrame:
    pattern = f"batch_{year}_*.json"
    batch_files = sorted(dataset_dir.glob(pattern))
    # Exclude manifests and pilot files
    batch_files = [f for f in batch_files if not f.name.endswith(".manifest.json") and "pilot" not in f.name]

    if not batch_files:
        return pd.DataFrame()

    frames = []
    for bf in batch_files:
        data = json.loads(bf.read_bytes())
        if data:
            frames.append(pd.DataFrame(data))

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Normalization per table
# ---------------------------------------------------------------------------

def normalize_processes(df: pd.DataFrame, year: int) -> pd.DataFrame:
    df = clean_sentinels(df)
    now = datetime.now(timezone.utc).isoformat()

    out = pd.DataFrame()
    out["process_id"] = df.get("id_del_portafolio")
    out["process_ref"] = df.get("id_del_proceso")
    out["process_reference_number"] = df.get("referencia_del_proceso")
    out["contract_id"] = pd.Series(dtype="object")  # not in processes
    out["entity_nit"] = df.get("nit_entidad", pd.Series(dtype="object")).astype(str).str.strip()
    out["entity_name"] = df.get("entidad")
    out["entity_level"] = df.get("ordenentidad")
    out["entity_level_norm"] = df.get("ordenentidad")  # Nacional/Territorial/Corp Autonoma
    out["department_raw"] = df.get("departamento_entidad")
    out["department_norm"] = df.get("departamento_entidad", pd.Series(dtype="object")).apply(normalize_geo)
    out["municipality_raw"] = df.get("ciudad_entidad")
    out["municipality_norm"] = df.get("ciudad_entidad", pd.Series(dtype="object")).apply(normalize_geo)
    out["object_description"] = df.get("descripci_n_del_procedimiento")
    out["procurement_method_raw"] = df.get("modalidad_de_contratacion")
    out["procurement_method_norm"] = map_enum(
        df.get("modalidad_de_contratacion", pd.Series(dtype="object")),
        PROCUREMENT_METHOD_MAP, "procurement_method"
    )
    out["estimated_value_cop"] = safe_float(df.get("precio_base"))
    out["awarded_value_cop"] = safe_float(df.get("valor_total_adjudicacion"))
    out["original_currency"] = "COP"

    # Dates — normalized + raw
    out["publication_date"] = safe_datetime(df.get("fecha_de_publicacion_del"))
    out["publication_date_raw"] = df.get("fecha_de_publicacion_del")
    out["last_publication_date"] = safe_datetime(df.get("fecha_de_ultima_publicaci"))
    out["question_deadline"] = safe_datetime(df.get("fecha_de_recepcion_de"))
    out["question_deadline_raw"] = df.get("fecha_de_recepcion_de")
    out["bid_deadline"] = safe_datetime(df.get("fecha_de_apertura_de_respuesta"))
    out["bid_deadline_raw"] = df.get("fecha_de_apertura_de_respuesta")
    out["award_date"] = safe_datetime(df.get("fecha_adjudicacion"))
    out["award_date_raw"] = df.get("fecha_adjudicacion")
    out["contract_signature_date"] = pd.Series(dtype="datetime64[ns, UTC]")
    out["first_delivery_date"] = pd.Series(dtype="datetime64[ns, UTC]")
    out["final_payment_date"] = pd.Series(dtype="datetime64[ns, UTC]")

    out["status_raw"] = df.get("estado_del_procedimiento")
    out["status_norm"] = map_enum(
        df.get("estado_del_procedimiento", pd.Series(dtype="object")),
        STATUS_PROC_MAP, "status_proc"
    )
    out["supplier_id"] = df.get("nit_del_proveedor_adjudicado")
    out["supplier_name"] = df.get("nombre_del_proveedor")

    out["source"] = "secop2_procesos"
    out["source_record_uri"] = df.get("urlproceso", pd.Series(dtype="object")).apply(extract_url)
    out["ingested_at_utc"] = now

    # Extra useful columns from raw
    out["contract_type_raw"] = df.get("tipo_de_contrato")
    out["duration_value"] = safe_float(df.get("duracion"))
    out["duration_unit"] = df.get("unidad_de_duracion")
    out["entity_code"] = df.get("codigo_entidad")
    out["category_code"] = df.get("codigo_principal_de_categoria")
    out["is_awarded"] = df.get("adjudicado")
    out["phase"] = df.get("fase")
    out["num_lots"] = safe_float(df.get("numero_de_lotes"))

    return out


def normalize_contracts(df: pd.DataFrame, year: int) -> pd.DataFrame:
    df = clean_sentinels(df)
    now = datetime.now(timezone.utc).isoformat()

    out = pd.DataFrame()
    out["process_id"] = df.get("proceso_de_compra")
    out["contract_id"] = df.get("id_contrato")
    out["contract_reference"] = df.get("referencia_del_contrato")
    out["entity_nit"] = df.get("nit_entidad", pd.Series(dtype="object")).astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    out["entity_name"] = df.get("nombre_entidad")
    out["entity_level"] = df.get("orden")
    out["entity_level_norm"] = df.get("orden")
    out["department_raw"] = df.get("departamento")
    out["department_norm"] = df.get("departamento", pd.Series(dtype="object")).apply(normalize_geo)
    out["municipality_raw"] = df.get("ciudad")
    out["municipality_norm"] = df.get("ciudad", pd.Series(dtype="object")).apply(normalize_geo)
    out["object_description"] = df.get("objeto_del_contrato")
    out["procurement_method_raw"] = df.get("modalidad_de_contratacion")
    out["procurement_method_norm"] = map_enum(
        df.get("modalidad_de_contratacion", pd.Series(dtype="object")),
        PROCUREMENT_METHOD_MAP, "procurement_method"
    )
    out["estimated_value_cop"] = pd.Series(dtype="float64")  # not in contracts
    out["awarded_value_cop"] = safe_float(df.get("valor_del_contrato"))
    out["original_currency"] = "COP"

    out["publication_date"] = pd.Series(dtype="datetime64[ns, UTC]")
    out["question_deadline"] = pd.Series(dtype="datetime64[ns, UTC]")
    out["bid_deadline"] = pd.Series(dtype="datetime64[ns, UTC]")
    out["award_date"] = pd.Series(dtype="datetime64[ns, UTC]")
    out["contract_signature_date"] = safe_datetime(df.get("fecha_de_firma"))
    out["contract_signature_date_raw"] = df.get("fecha_de_firma")
    out["contract_start_date"] = safe_datetime(df.get("fecha_de_inicio_del_contrato"))
    out["first_delivery_date"] = safe_datetime(df.get("fecha_de_inicio_del_contrato"))
    out["contract_end_date"] = safe_datetime(df.get("fecha_de_fin_del_contrato"))
    out["contract_end_date_raw"] = df.get("fecha_de_fin_del_contrato")
    out["final_payment_date"] = pd.Series(dtype="datetime64[ns, UTC]")

    out["status_raw"] = df.get("estado_contrato")
    out["status_norm"] = map_enum(
        df.get("estado_contrato", pd.Series(dtype="object")),
        STATUS_CONTR_MAP, "status_contr"
    )
    out["supplier_id"] = df.get("documento_proveedor")
    out["supplier_name"] = df.get("proveedor_adjudicado")
    out["supplier_doc_type"] = df.get("tipodocproveedor")

    out["source"] = "secop2_contratos"
    out["source_record_uri"] = df.get("urlproceso", pd.Series(dtype="object")).apply(extract_url)
    out["ingested_at_utc"] = now

    # Extra useful columns
    out["contract_type_raw"] = df.get("tipo_de_contrato")
    out["sector"] = df.get("sector")
    out["branch"] = df.get("rama")
    out["entity_code"] = df.get("codigo_entidad")
    out["category_code"] = df.get("codigo_de_categoria_principal")
    out["valor_pagado"] = safe_float(df.get("valor_pagado"))
    out["valor_facturado"] = safe_float(df.get("valor_facturado"))
    out["valor_pendiente_pago"] = safe_float(df.get("valor_pendiente_de_pago"))
    out["dias_adicionados"] = safe_float(df.get("dias_adicionados"))
    out["is_pyme"] = df.get("es_pyme")
    out["is_group"] = df.get("es_grupo")
    out["is_post_conflict"] = df.get("espostconflicto")
    out["funding_source"] = df.get("origen_de_los_recursos")
    out["spending_destination"] = df.get("destino_gasto")
    out["duration_raw"] = df.get("duraci_n_del_contrato")
    out["last_updated"] = safe_datetime(df.get("ultima_actualizacion"))

    return out


def normalize_amendments(df: pd.DataFrame, year: int) -> pd.DataFrame:
    df = clean_sentinels(df)
    now = datetime.now(timezone.utc).isoformat()

    out = pd.DataFrame()
    out["amendment_id"] = df.get("identificador")
    out["contract_id"] = df.get("id_contrato")
    out["amendment_type_raw"] = df.get("tipo")
    out["amendment_type_norm"] = map_enum(
        df.get("tipo", pd.Series(dtype="object")),
        AMENDMENT_TYPE_MAP, "amendment_type"
    )
    out["description"] = df.get("descripcion")
    out["registration_date"] = safe_datetime(df.get("fecharegistro"))
    out["registration_date_raw"] = df.get("fecharegistro")
    out["source"] = "secop2_adiciones"
    out["ingested_at_utc"] = now

    return out


# ---------------------------------------------------------------------------
# Write Parquet
# ---------------------------------------------------------------------------

def write_parquet_partitioned(df: pd.DataFrame, table_name: str, source: str, year: int):
    out_dir = PARQUET_DIR / table_name / f"source={source}" / f"year={year}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_file, compression="zstd", compression_level=3)

    size_mb = out_file.stat().st_size / 1e6
    print(f"  Wrote {out_file}: {len(df):,} rows, {size_mb:.1f} MB")
    return len(df)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TABLE_CONFIG = {
    "secop2_procesos": ("processes", normalize_processes),
    "secop2_contratos": ("contracts", normalize_contracts),
    "secop2_adiciones": ("amendments", normalize_amendments),
}


def convert_dataset(dataset_name: str):
    table_name, normalizer = TABLE_CONFIG[dataset_name]
    dataset_dir = RAW_DIR / dataset_name
    total = 0
    dropped = 0

    print(f"\nConverting {dataset_name} → {table_name}")

    for year in YEARS:
        print(f"  Loading {year}...")
        df = load_year_batches(dataset_dir, year)
        if df.empty:
            print(f"  {year}: no data")
            continue

        raw_count = len(df)
        print(f"  {year}: {raw_count:,} raw rows, {len(df.columns)} columns")

        normalized = normalizer(df, year)
        norm_count = len(normalized)

        if raw_count != norm_count:
            diff = raw_count - norm_count
            dropped += diff
            print(f"  WARNING: {diff} rows dropped during normalization!", file=sys.stderr)

        total += write_parquet_partitioned(normalized, table_name, dataset_name, year)
        del df, normalized  # free memory

    print(f"  {dataset_name} total: {total:,} rows written, {dropped} dropped")
    return {"table": table_name, "total_rows": total, "dropped": dropped}


def main():
    print("Converting raw JSON → normalized Parquet")
    print(f"Raw dir: {RAW_DIR}")
    print(f"Parquet dir: {PARQUET_DIR}")

    results = {}
    for dataset_name in TABLE_CONFIG:
        results[dataset_name] = convert_dataset(dataset_name)

    print("\n" + "=" * 60)
    print("CONVERSION SUMMARY")
    print("=" * 60)
    for name, r in results.items():
        print(f"  {name} → {r['table']}: {r['total_rows']:,} rows, {r['dropped']} dropped")

    if any(r["dropped"] > 0 for r in results.values()):
        print("\nWARNING: Some rows were dropped. Check logs above.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
