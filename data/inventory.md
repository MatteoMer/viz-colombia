# Data Inventory — Colombian Public Procurement (SECOP II)

**Generated**: 2026-04-21
**Coverage**: 2019-01-01 through 2024-12-31
**Primary source**: SECOP II via datos.gov.co (Socrata SODA API)
**Source datasets**:
  - Procesos de Contratacion (`p6dx-8zbt`)
  - Contratos Electronicos (`jbjy-vk9h`)
  - Adiciones (`cb9c-h8sn`)

---

## 1. Coverage Summary

### Row Counts by Year

| Year | Processes | Contracts | Amendments | Total |
|------|-----------|-----------|------------|-------|
| 2019 | 186,860 | 142,442 | 257,694 | 586,996 |
| 2020 | 420,168 | 356,631 | 444,715 | 1,221,514 |
| 2021 | 653,312 | 560,458 | 687,745 | 1,901,515 |
| 2022 | 1,032,554 | 707,756 | 1,226,307 | 2,966,617 |
| 2023 | 1,514,071 | 835,400 | 4,137,022 | 6,486,493 |
| 2024 | 1,400,000* | 881,845 | 3,079,149 | 5,360,994 |
| **Total** | **5,206,965** | **3,484,532** | **9,832,632** | **18,524,129** |

*\* Processes 2024 is missing ~218,302 records from approximately Nov 5 - Dec 31, 2024 due to API rate limiting during ingestion. See Known Gaps below.*

### Storage

| Layer | Size |
|-------|------|
| Raw JSON (data/raw/) | ~26 GB |
| Parquet+zstd (data/parquet/) | ~978 MB |
| Total | ~27 GB |

---

## 2. Source Selection Justification

**Chosen**: SECOP II via Socrata (datos.gov.co)

**Rejected alternatives**:
- **Colombia Compra Eficiente OCDS API** (`apiocds.colombiacompra.gov.co`): Returns HTTP 502. Appears unmaintained. Last data coverage ~2022 per OCP registry. Bulk OCDS downloads exist at data.open-contracting.org but are less granular than native SECOP II.
- **SECOP I** (`f789-7hwg`): Legacy system. Available on datos.gov.co but very large (~20M+ rows, ~20 GB CSV). SECOP II is the mandatory modern platform and covers 2019-2024 comprehensively. Entities that still filed on SECOP I during this period are a known gap.

**Reasons for SECOP II Socrata**:
1. Actively maintained, daily updates
2. Separate process/contract/amendment tables matching deliverable structure
3. Stable SODA API with date filtering and paging
4. Rich schema (52-87 columns per table)

---

## 3. Tables & Schemas

### 3.1 Processes (from `secop2_procesos / p6dx-8zbt`)

Pre-award procurement process records. One row per procurement process.

**Normalized columns**:

| Column | Type | Source Field | Fill Rate | Notes |
|--------|------|-------------|-----------|-------|
| process_id | string | id_del_portafolio | 100% | CO1.BDOS.* — join key to contracts |
| process_ref | string | id_del_proceso | 100% | CO1.REQ.* — unique process reference |
| process_reference_number | string | referencia_del_proceso | 100% | Human-readable reference |
| entity_nit | string | nit_entidad | 100% | Normalized to string |
| entity_name | string | entidad | 100% | |
| entity_level | string | ordenentidad | 100% | Nacional / Territorial / Corporacion Autonoma |
| department_raw | string | departamento_entidad | ~94% real | Sentinels cleaned to null |
| department_norm | string | — | ~94% | Accent-stripped, uppercased |
| municipality_raw | string | ciudad_entidad | ~71% real | |
| municipality_norm | string | — | ~71% | |
| object_description | string | descripci_n_del_procedimiento | ~99% | |
| procurement_method_raw | string | modalidad_de_contratacion | 100% | |
| procurement_method_norm | string | — | 100% | Mapped to versioned enum (v1) |
| estimated_value_cop | float64 | precio_base | 100% | |
| awarded_value_cop | float64 | valor_total_adjudicacion | 100% | 0 if not yet awarded |
| original_currency | string | — | 100% | Always "COP" |
| publication_date | datetime[UTC] | fecha_de_publicacion_del | 100% | |
| publication_date_raw | string | fecha_de_publicacion_del | 100% | Original for TZ audit |
| question_deadline | datetime[UTC] | fecha_de_recepcion_de | ~5% | Sparse for unadjudicated |
| bid_deadline | datetime[UTC] | fecha_de_apertura_de_respuesta | ~3% | |
| award_date | datetime[UTC] | fecha_adjudicacion | ~3% | |
| status_raw | string | estado_del_procedimiento | 100% | |
| status_norm | string | — | 100% | |
| supplier_id | string | nit_del_proveedor_adjudicado | ~1% real | Sparse — most not yet awarded |
| supplier_name | string | nombre_del_proveedor | ~3% real | |
| source | string | — | 100% | "secop2_procesos" |
| source_record_uri | string | urlproceso.url | 100% | Extracted from nested JSON |
| ingested_at_utc | string | — | 100% | |

**Additional columns preserved**: contract_type_raw, duration_value, duration_unit, entity_code, category_code, is_awarded, phase, num_lots.

### 3.2 Contracts (from `secop2_contratos / jbjy-vk9h`)

Signed electronic contracts. One row per contract.

**Normalized columns**:

| Column | Type | Source Field | Fill Rate | Notes |
|--------|------|-------------|-----------|-------|
| process_id | string | proceso_de_compra | 100% | CO1.BDOS.* — join key to processes |
| contract_id | string | id_contrato | 100% | CO1.PCCNTR.* — PK |
| contract_reference | string | referencia_del_contrato | 100% | |
| entity_nit | string | nit_entidad | 100% | Cast from number, stripped |
| entity_name | string | nombre_entidad | 100% | |
| entity_level | string | orden | 100% | |
| department_raw | string | departamento | ~98% real | |
| department_norm | string | — | ~98% | |
| municipality_raw | string | ciudad | ~71% real | |
| municipality_norm | string | — | ~71% | |
| object_description | string | objeto_del_contrato | ~100% | NOT descripcion_del_proceso |
| procurement_method_raw | string | modalidad_de_contratacion | 100% | |
| procurement_method_norm | string | — | 100% | |
| awarded_value_cop | float64 | valor_del_contrato | 100% | |
| original_currency | string | — | 100% | Always "COP" |
| contract_signature_date | datetime[UTC] | fecha_de_firma | 100% | |
| contract_signature_date_raw | string | fecha_de_firma | 100% | |
| contract_start_date | datetime[UTC] | fecha_de_inicio_del_contrato | 100% | |
| contract_end_date | datetime[UTC] | fecha_de_fin_del_contrato | 100% | |
| contract_end_date_raw | string | fecha_de_fin_del_contrato | 100% | |
| status_raw | string | estado_contrato | 100% | |
| status_norm | string | — | 100% | |
| supplier_id | string | documento_proveedor | ~100% | |
| supplier_name | string | proveedor_adjudicado | 100% | |
| supplier_doc_type | string | tipodocproveedor | ~100% | CC, NIT, CE, etc. |
| source | string | — | 100% | "secop2_contratos" |
| source_record_uri | string | urlproceso | 100% | |
| ingested_at_utc | string | — | 100% | |

**Additional columns preserved**: contract_type_raw, sector, branch, entity_code, category_code, valor_pagado, valor_facturado, valor_pendiente_pago, dias_adicionados, is_pyme, is_group, is_post_conflict, funding_source, spending_destination, duration_raw, last_updated.

### 3.3 Amendments (from `secop2_adiciones / cb9c-h8sn`)

Contract modifications (otrosi), suspensions, reactivations, cessions, and closures.

| Column | Type | Source Field | Fill Rate | Notes |
|--------|------|-------------|-----------|-------|
| amendment_id | string | identificador | 100% | CO1.CTRMOD.* — PK |
| contract_id | string | id_contrato | 100% | CO1.PCCNTR.* — FK to contracts |
| amendment_type_raw | string | tipo | 100% | |
| amendment_type_norm | string | — | 100% | |
| description | string | descripcion | 100% | Free text |
| registration_date | datetime[UTC] | fecharegistro | 100% | |
| registration_date_raw | string | fecharegistro | 100% | |
| source | string | — | 100% | "secop2_adiciones" |
| ingested_at_utc | string | — | 100% | |

---

## 4. Join Keys

```
processes.process_id (CO1.BDOS.*) = contracts.process_id (CO1.BDOS.*)
contracts.contract_id (CO1.PCCNTR.*) = amendments.contract_id (CO1.PCCNTR.*)
```

- A process may have 0..N contracts (multi-lot or re-awarded).
- A contract may have 0..N amendments.

---

## 5. Enum Values Encountered

### Procurement Method (`procurement_method_raw` → `procurement_method_norm`)

| Raw Value | Normalized (v1) |
|-----------|----------------|
| Contratacion directa | CONTRATACION_DIRECTA |
| Contratacion Directa (con ofertas) | CONTRATACION_DIRECTA_CON_OFERTAS |
| Contratacion regimen especial | REGIMEN_ESPECIAL |
| Contratacion regimen especial (con ofertas) | REGIMEN_ESPECIAL_CON_OFERTAS |
| Minima cuantia | MINIMA_CUANTIA |
| Seleccion Abreviada de Menor Cuantia | SELECCION_ABREVIADA_MENOR_CUANTIA |
| Seleccion Abreviada Menor Cuantia Sin Manifestacion Interes | SELECCION_ABREVIADA_SIN_MANIFESTACION |
| Seleccion abreviada subasta inversa | SUBASTA_INVERSA |
| Licitacion publica | LICITACION_PUBLICA |
| Licitacion publica Obra Publica | LICITACION_PUBLICA_OBRA |
| Licitacion Publica Acuerdo Marco de Precios | ACUERDO_MARCO_PRECIOS |
| Concurso de meritos abierto | CONCURSO_MERITOS |
| Concurso de meritos con precalificacion | CONCURSO_MERITOS_PRECALIFICACION |
| Solicitud de informacion a los Proveedores | SOLICITUD_INFORMACION |
| Enajenacion de bienes con subasta | ENAJENACION_SUBASTA |
| Enajenacion de bienes con sobre cerrado | ENAJENACION_SOBRE_CERRADO |
| Subasta de prueba | SUBASTA_PRUEBA |

### Process Status (`estado_del_procedimiento`)

Seleccionado, Publicado, Evaluacion, Cancelado, Abierto, Aprobado, Borrador, Descartado, Adjudicado, Cerrado.

### Contract Status (`estado_contrato`)

Cerrado, Aprobado, Modificado, Terminado, Cedido, Borrador, Suspendido, Enviado Proveedor, En Aprobacion, Liquidado, Cancelado, En Ejecucion.

### Amendment Type (`tipo`)

MODIFICACION GENERAL, CONCLUSION, No definido, ADICION EN EL VALOR, CESION, REACTIVACION, SUSPENSION, EXTENSION, REDUCCION EN EL VALOR, EXPIRACION.

### Entity Level (`ordenentidad` / `orden`)

Nacional, Territorial, Corporacion Autonoma.

Note: No municipal vs. departamental breakout in the source. The `municipality_raw`/`municipality_norm` fields can be used to infer this.

---

## 6. Schema Drift Notes

1. **Procurement method enum**: `Seleccion Abreviada Menor Cuantia Sin Manifestacion Interes` first appears in 2020+ data, not present in early-2019 pilot.
2. **Contract status**: `Cancelado` and `En ejecucion` appear in 2020+ data but not in early-2019 pilot.
3. **Column count varies**: Procesos API returns 57 columns (metadata says 52 — some columns only appear when populated). Contratos returns 84 (metadata says 87 — 3 internal/hidden).
4. **`urlproceso` format**: Nested JSON object `{"url": "..."}` in Procesos, plain URL string in Contratos.
5. **`nit_entidad` type**: Text in Procesos, Number in Contratos. Normalized to string in both.

---

## 7. Sentinel Value Handling

The following values are treated as null (replaced with None in Parquet):

- "No Definido", "No definido", "NO DEFINIDO"
- "No Aplica", "No aplica"
- "Sin Descripcion"
- "No Adjudicado"
- "No Especificado"

Fill rates reported as "real %" exclude these sentinels.

---

## 8. Known Gaps

### 8.1 Missing Procesos Records (Nov-Dec 2024)

~218,302 process records from approximately Nov 5 - Dec 31, 2024 are missing due to API rate limiting during ingestion. The datos.gov.co API became unresponsive after ~26 GB of downloads.

**Impact**: Low. The corresponding *contracts* for this period are fully present in the contracts table (881,845 rows for all of 2024). Only the pre-award process metadata is missing.

**Remediation**: Re-run `make download` when API access is restored. The download script is idempotent and will fetch only the missing batches.

### 8.2 SECOP I Entities

Entities that still filed on SECOP I during 2019-2024 are NOT included. SECOP II is the mandatory modern system, but some legacy entities may have continued using SECOP I, particularly in early years (2019-2020).

The SECOP I dataset (`f789-7hwg`) is available on datos.gov.co but was excluded due to:
- Very large size (~20M+ rows, ~20 GB CSV)
- Overlap with SECOP II for most entities
- Different schema requiring separate normalization

### 8.3 No OCDS Data

The Colombia Compra Eficiente OCDS API was down (HTTP 502) during reconnaissance. OCDS bulk downloads exist at data.open-contracting.org but were not incorporated as SECOP II Socrata data is more complete and current.

### 8.4 Date Fields

- All timestamps are stored as UTC. The source data uses `T00:00:00.000` format with no timezone offset. These are assumed to be Colombia local time (UTC-5) stored without offset — timezone conversion was NOT applied to avoid introducing errors. The `_raw` columns preserve the original strings for future audit.
- `question_deadline`, `bid_deadline`, and `award_date` in processes have very low fill rates (~3-5%) because most process records in the dataset are pre-award.

### 8.5 Amendment Values

The amendments table contains the amendment *type* and *description* but does NOT contain structured value changes (e.g., the amount added in an "ADICION EN EL VALOR"). The `valor_del_contrato` in the contracts table reflects the current/final contract value, and `dias_adicionados` tracks cumulative days added.

---

## 9. Partitioning & File Layout

```
data/parquet/
  processes/source=secop2_procesos/year={2019..2024}/data.parquet
  contracts/source=secop2_contratos/year={2019..2024}/data.parquet
  amendments/source=secop2_adiciones/year={2019..2024}/data.parquet
```

Compression: zstd (level 3).

---

## 10. Reproducibility

```bash
make ingest    # Full pipeline: download + convert
make download  # Download only (idempotent, resumes)
make convert   # Convert only (requires raw data)
```

The download script is idempotent: it checks for existing manifest files and skips completed batches. Re-runs will only fetch missing data.
