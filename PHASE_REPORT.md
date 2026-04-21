# Phase Report — Colombia SECOP Procurement x Satellite Demo

Generated: 2026-04-21

---

## Phase 1 — Cohort Selection

**Input:** 3,484,532 SECOP II contracts (2019-2024)

**Filters applied (sequential):**

| Filter | Remaining |
|--------|-----------|
| `contract_type_raw = 'Obra'` | 30,893 |
| `awarded_value_cop > 5B COP` | 2,515 |
| Signature date 2021-06 to 2023-12 | 1,615 |
| Exclude Cancelado/Borrador | 1,615 |
| Target departments (10) | 563 |
| Exclude design/consultancy/supervision | 540 |

**Scoring:** `log(valor_contrato) x (n_amendments + 1) x (1.5 if had_suspension else 1.0)`

**Output:** Top 200 candidates saved to `cohort_candidates.parquet`

**Key stats:**
- Median contract value: 11.1B COP (~$2.6M USD)
- P90 contract value: 43.3B COP (~$10.3M USD)
- 190/200 have at least one suspension
- Amendment counts: 15-78 per contract (median ~22)
- Top departments: Valle del Cauca (48), Cundinamarca (41), Santander (31)
- 182 territorial, 17 national, 1 autonomous corporation
- Contractors are almost entirely unique consortia

**Decisions under ambiguity:**
- Excluded only `Cancelado` and `Borrador` statuses (not `terminado` or `cedido`, which are valid lifecycle states)
- Used `awarded_value_cop` as the value filter field (not `estimated_value_cop`)
- Amendment table has no structured value deltas (inventory note 8.5), so `total_value_delta` could not be computed; used amendment count as proxy

---

## Phase 2 — Geolocation

**Approach:** Tier B only (text extraction + Nominatim geocoding). Tier A (INVIAS cross-reference) was not needed. Tier C (manual PDF) was not performed.

**Municipality extraction:**
- 145/200 contracts: municipality extracted from object description (regex on "MUNICIPIO DE X" patterns)
- 46/200: fell back to entity municipality field
- 9/200: no municipality could be identified

**Key finding:** Many Valle del Cauca contracts are registered by Cali's departmental government but work occurs in other municipalities (Buga, Yotoco, Cartago, Jamundí, etc.). The description-extraction step was critical.

**Geocoding:**
- 90 unique municipality+department pairs identified
- 81/90 resolved via Nominatim (9 failed due to noisy extraction — trailing description text in municipality field)
- 2 additional rejections: coordinates fell outside department bounding box

**Output:** 182 contracts geolocated in `cohort_geolocated.parquet`

**Geocode confidence:**
- 0.60 (municipality from description): 136 contracts
- 0.40 (entity municipality fallback): 46 contracts

**Department coverage:**
| Department | Sites |
|------------|-------|
| Valle del Cauca | 46 |
| Cundinamarca | 38 |
| Santander | 30 |
| Antioquia | 16 |
| Meta | 15 |
| Tolima | 12 |
| Norte de Santander | 9 |
| Atlántico | 9 |
| Boyacá | 5 |
| Huila | 2 |

**Precision caveat:** Geocoding is at municipality center (~1-5km), not the 100m target. For the demo, this means the 200m satellite chip will show the general municipality area, not the specific construction site. Refinement to site-level precision would require either INVIAS geometry cross-reference (road projects) or manual PDF extraction (Phase 2 Tier C).

---

## Phase 3 — Declared-Progress Timeline

**Approach:** Linear interpolation from 0% at contract start to current `valor_pagado/awarded_value_cop` at snapshot date, with suspension periods flattened.

**Input data quality:**
- `valor_pagado` available (non-zero) for 97/182 contracts (53%)
- `valor_facturado` for 108/182 (59%)
- For closed/terminated contracts with 0 `valor_pagado`, assumed 100% completion

**Output:** 4,804 contract-month rows in `declared_progress.parquet`

**Duration stats:**
- Min: 6 months
- Median: 24 months
- Max: 52 months

**Progress distribution (final month):**
| Range | Count |
|-------|-------|
| 0% (no payment data) | 58 |
| 1-25% | 5 |
| 25-50% | 10 |
| 50-75% | 17 |
| 75-99% | 42 |
| 100% | 50 |

- 170/182 contracts have at least one suspended month
- 115 contracts end in "suspended" status (still suspended at timeline end)

**Limitations:**
- `valor_pagado` is a single cumulative snapshot, not a monthly time series. Progress curves are linearly interpolated, which smooths over actual payment patterns.
- Amendment table lacks structured value deltas. Value additions (`ADICION_VALOR`, 103 events) are noted as events but the denominator adjustment is unknown.
- 58 contracts show 0% progress due to missing payment data — these are flagged but included in the cohort.

---

## Phase 4 — Satellite Stack Pull

**Status:** Script written (`scripts/phase4_satellite.py`), not yet executed.

**Blocked on:** Google Earth Engine authentication and `earthengine-api` installation (PyPI connectivity issues during session).

**To run:**
```bash
uv pip install earthengine-api numpy
earthengine authenticate --project=YOUR_PROJECT_ID
uv run python scripts/phase4_satellite.py --project=YOUR_PROJECT_ID
```

**Design:**
- Uses GEE server-side processing for Sentinel-1 GRD and Sentinel-2 L2A
- 200m buffer around each site (20x20 pixel chip at 10m)
- Monthly median composites, SCL cloud masking for S2
- Per-site checkpointing (idempotent re-runs)
- Error logging to `data/stacks/fetch_errors.jsonl`

---

## Deliverables Status

| File | Status | Rows/Size |
|------|--------|-----------|
| `data/cohort_candidates.parquet` | Done | 200 rows |
| `data/cohort_geolocated.parquet` | Done | 182 rows |
| `data/declared_progress.parquet` | Done | 4,804 rows |
| `data/stacks/site_*.npz` | Pending (Phase 4) | — |
| `data/stacks/fetch_errors.jsonl` | Pending (Phase 4) | — |
| `PHASE_REPORT.md` | This file | — |
