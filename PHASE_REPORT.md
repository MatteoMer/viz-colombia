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

---

## Phase 4 — Signal Construction (Procurement-Only Path)

**Pivot:** Satellites dropped from scope. Demo rebuilt on pure procurement signals.

**Reference population:** 28,601 Obra contracts (2020-2024) nationwide.

### Signal Summary

| Signal | Scope | Coverage | Flag Rate | Notes |
|--------|-------|----------|-----------|-------|
| S1 Stall | contract | 13,140 | P95: score>1205 | 82.6% above raw threshold 100 — percentile used instead |
| S2 Value Creep | contract | 25,357 | 14.1% > 0.3 | Median 0%, P95 0.48, extreme outliers at 1499x |
| S2 Value Creep | contractor | 3,441 | 11.1% > 0.3 | Portfolio-weighted |
| S3 Slippage | contract | 23,272 | 7.1% > 0.5 | Healthy distribution |
| S3 Slippage | contractor | 2,984 | 6.2% > 0.5 | Portfolio-weighted |
| S4 Bunching | entity-yr | 23,781 | 3,880 flagged | Mínima cuantía most common |
| S5 Concentration | entity-qtr | 31,837 | 3,809 flagged | HHI>0.25 & streak≥3 |
| S6 Single Bidder | entity-yr | 3,199 | 67% mean rate | High baseline — data quality issue |
| S7 Award Speed | process | 18,543 | 5.0% flagged | R²=0.531, 326 fast + 596 slow |
| S8 Relationship | edge | 872 | max z=2.1 | Small Obra-only graph, compressed distribution |

**Decisions under ambiguity:**
- S2 uses process `estimated_value_cop` as baseline (contract field is empty for Obra)
- S4 uses simplified national thresholds (entity-specific budgets not available)
- S6 high baseline (67%) likely reflects unpopulated bid count fields rather than true single-bidder rates
- S8 restricted to Obra-only to avoid overflow from all-contract gravity model

---

## Phase 5 — Composite Score & Demo Cohort

**Composite distribution (28,601 contracts):**
- Min: -5.49, Median: -0.21, P90: 1.84, P95: 2.50, P99: 4.16, Max: 11.39

**Demo cohort:** 150 contracts from geolocated set, ranked by composite.

**Contractor league table:** 319 contractors with ≥3 contracts in demo or ≥10 in refpop.

**Top entity patterns:**
- **Mosquera (Cundinamarca):** 3 contracts in top 20, 2 to same contractor (Consorcio Génesis). HHI-driven.
- **Ricaurte (Cundinamarca):** 2 contracts in top 20 with bunching + stall.
- **Tuluá (Valle):** 2 sports facility contracts with single bidder.

**Sanity check:** 4/10 top contracts have >60% of composite from single signal (mostly HHI or bunching). Weights are first guesses — acceptable for demo, should be tuned with labeled outcomes.

---

## Phase 6 — Inspection & Methodology

**Inspection of top 20 flagged contracts:**

| Verdict | Count |
|---------|-------|
| Pattern holds | 10 |
| Benign explanation | 3 |
| Ambiguous | 7 |

**50% hold rate** — 10 of 20 contracts show patterns that warrant genuine review. Benign cases are mostly intergovernmental transfers and national standardized programs. See `data/inspection_log.md` for full details.

**Methodology document:** `METHODOLOGY.md` — covers all 8 signals, composite weights, claims/non-claims, blind spots, and extension paths.

**Recommended signal adjustments based on inspection:**
- S7 (Award speed): add entity type covariate to filter intergovernmental transfers
- S6 (Single bidder): investigate data quality of bid count field before increasing weight
- S5 (Concentration): consider normalizing by entity size to reduce over-flagging of large entities

---

## Deliverables Status

| File | Status | Rows/Size |
|------|--------|-----------|
| `data/cohort_candidates.parquet` | Done | 200 rows |
| `data/cohort_geolocated.parquet` | Done | 182 rows |
| `data/declared_progress.parquet` | Done | 4,804 rows |
| `data/reference_population.parquet` | Done | 28,601 rows |
| `data/entity_year_budget.parquet` | Done | 10,768 rows |
| `data/category_medians.parquet` | Done | 202 rows |
| `data/contractor_portfolio.parquet` | Done | 13,029 rows |
| `data/obra_bid_counts.parquet` | Done | 83,509 rows |
| `data/signals/s1–s8_*.parquet` | Done | 10 files |
| `data/anomaly_scored.parquet` | Done | 28,601 rows |
| `data/demo_cohort.parquet` | Done | 150 rows |
| `data/demo_contractors.parquet` | Done | 319 rows |
| `data/inspection_log.md` | Done | 20 contracts |
| `METHODOLOGY.md` | Done | — |
| `PHASE_REPORT.md` | This file | — |
