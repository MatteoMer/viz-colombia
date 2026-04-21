# Methodology — Colombia Procurement Anomaly Detection

**Version:** 0.1 (demo)
**Date:** 2026-04-21
**Author:** Generated pipeline, manual review

---

## 1. Data Sources

| Source | Dataset ID | Coverage | Access |
|--------|-----------|----------|--------|
| SECOP II Procesos | `p6dx-8zbt` | 2019–2024 | datos.gov.co Socrata API |
| SECOP II Contratos | `jbjy-vk9h` | 2019–2024 | datos.gov.co Socrata API |
| SECOP II Adiciones | `cb9c-h8sn` | 2019–2024 | datos.gov.co Socrata API |

**Ingestion date:** 2026-04-20. Data reflects SECOP II state as of that date.

**Known gap:** ~218K process records missing from Nov 5 – Dec 31, 2024 (API rate limiting). Contracts and amendments for this period are complete.

**Currency:** All values in Colombian Pesos (COP). USD equivalents shown at COP 4,000/USD — this is approximate and should be noted prominently in any presentation.

---

## 2. Reference Population

**Definition:** All contracts where:
- `contract_type_raw = 'Obra'` (construction works)
- `contract_signature_date` between 2020-01-01 and 2024-12-31

**Size:** 28,601 contracts from ~4,500 contracting entities.

**Rationale:** Obra contracts are the target for construction anomaly detection. The 5-year window provides sufficient statistical depth while staying within the current SECOP II system's mature period.

---

## 3. Signal Definitions

### S1 — Payment Stall (per contract)

**Formula:**
- `months_flat_while_active` = consecutive months at the latest timestep where declared progress changed by < 2 percentage points, restricted to months where contract was not officially suspended.
- `stall_score = log(valor_contrato) × months_flat_while_active`

**For the 182 geolocated cohort:** computed from full monthly declared-progress timeline (Phase 3) using payment snapshots and amendment events.

**For the broader reference population:** simplified metric based on current `valor_pagado / valor_contrato` ratio and months since contract start. Active contracts with progress < 10% and > 12 months elapsed are flagged proportionally.

**Threshold:** Percentile ranking used for composite. Raw stall_score > 500 indicates a large, long-stalled contract.

**Limitations:** `valor_pagado` is a cumulative snapshot in SECOP II, not a time series. Linear interpolation between anchor points (0% at start, current ratio at now) smooths actual payment patterns. 53% of contracts have non-zero `valor_pagado`.

### S2 — Value Creep (per contract + per contractor)

**Formula:**
- `value_creep_ratio = (valor_contrato_current - valor_estimado_proceso) / valor_estimado_proceso`

Where `valor_estimado_proceso` is the pre-award estimated value from the procurement process record, and `valor_contrato_current` is the current contract value (`awarded_value_cop`).

**Per contractor:** value-weighted mean of `value_creep_ratio` across all their contracts in the reference population (minimum 2 contracts).

**Simplification:** The amendments table does NOT contain structured value deltas (SECOP II inventory note 8.5). We cannot distinguish bid premium from subsequent value additions. The `value_creep_ratio` captures the combined effect.

Multi-lot processes are excluded (only single-contract processes used) to avoid comparing process-level estimates to individual lot awards.

**Coverage:** 25,357 contracts (89% of reference population).

### S3 — Schedule Slippage (per contract + per contractor)

**Formula:**
- `original_duration_days = (contract_end_date - contract_start_date) - dias_adicionados`
- `slippage_ratio = dias_adicionados / original_duration_days`

**Per contractor:** value-weighted mean across portfolio (minimum 2 contracts).

**Threshold:** Slippage ratio > 0.5 means the contract was extended by more than half its original duration.

**Limitation:** `dias_adicionados` is cumulative in the contracts table. We cannot determine when extensions occurred.

### S4 — Threshold Bunching (per entity per year)

**Colombia's procurement thresholds** (Ley 1150 / Decreto 1082):
- **Mínima cuantía:** Up to 10% of menor cuantía threshold
- **Menor cuantía:** Varies by entity annual budget tier (280–1000 SMMLV)

**Simplification:** We use **fixed national thresholds** based on 2024 SMMLV (COP 1,300,000):
- Mínima cuantía: 28 × SMMLV = 36.4M COP
- Menor cuantía (low): 280 × SMMLV = 364M COP
- Menor cuantía (high): 1000 × SMMLV = 1.3B COP

**This is a known simplification.** Actual thresholds vary by entity budget tier. A production system should use entity-specific budget data from Contraloría or DNP.

**Formula:** For each (entity, year, threshold T):
- `bunching_ratio = count([0.85T, T)) / count((T, 1.15T])`

Values > 1.5 indicate suspicious clustering below the threshold.

**Applied to all contract types** (not just Obra) since entities bunch contracts across types.

### S5 — Contractor Concentration / HHI (per entity)

**Formula:** For each entity, rolling 12-month windows ending each quarter:
- `HHI = Σ(contractor_share²)` where `contractor_share = contractor_awarded / entity_total_awarded`
- `top_contractor_share` = maximum single contractor's share
- `streak_length` = consecutive windows where the same contractor holds top share

**Flag:** HHI > 0.25 AND streak_length ≥ 3.

**Applied to all contract types** for entity-level assessment.

### S6 — Single-Bidder Rate (per entity per year)

**Input:** Bid count extracted from raw SECOP II process JSON (`respuestas_al_procedimiento` and `proveedores_unicos_con` fields, taking the maximum).

**Restricted to competitive methods:** Licitación pública, Selección abreviada, Mínima cuantía, etc. Contratación directa is excluded (single-source by design).

**Formula:** `single_bidder_rate = n_processes_with_bid_count_≤_1 / n_competitive_processes`

**Known issue:** The national mean single-bidder rate for competitive Obra processes is 67%. This is high and may reflect data quality issues (bid counts not always populated in SECOP II). The signal is still useful for relative comparison — entities significantly above this baseline warrant attention.

### S7 — Time-to-Award Residual (per process)

**OLS regression:**
```
log(days_to_award + 1) ~ log(valor_estimado) + C(procurement_method) + C(year)
```

**R² = 0.531** — the model explains about half the variance, which is typical for procurement timing.

**Residual z-score:** Standardized residual from the OLS. |z| > 2 is flagged.

- **Fast anomalies (z < -2):** Possible pre-wired awards. 326 flagged (1.8%).
- **Slow anomalies (z > 2):** Capacity problems or contested processes. 596 flagged (3.2%).

**Limitation:** The model does not include entity type (municipal vs. departmental vs. national enterprise) as a covariate. Intergovernmental transfers appear as fast anomalies because they are legitimately awarded quickly. A production model should add entity type.

### S8 — Contractor-Entity Relationship Intensity (per edge)

**Graph:** Bipartite graph of entities × contractors, restricted to Obra contracts 2022–2024 with valid supplier IDs.

**Gravity null model:**
- `w_expected = (w_entity_total × w_contractor_total) / w_graph_total`
- Z-score: `log(w_observed / w_expected)`, standardized across all edges

**Edge filter:** Minimum 3 contracts per edge.

**Coverage:** 872 edges from 18,823 contracts.

**Limitation:** The Obra-only graph is relatively small, producing a compressed z-score distribution (max 2.1). In a production system, this should be computed across all contract types for more statistical power.

---

## 4. Composite Score

**Formula:**
```
composite = 1.0×z_stall + 1.0×z_creep_contract + 0.5×z_creep_contractor
          + 0.7×z_slip_contract + 0.5×z_slip_contractor
          + 0.7×z_bunching_entity + 0.5×z_hhi_entity
          + 0.7×z_single_bidder_entity
          + 0.5×|z_award_speed| + 0.8×z_relationship
```

**Each signal is standardized:** z-score using reference-population mean and standard deviation, clipped to [-5, 5], NaN replaced with 0.

**Weights are NOT optimized.** They are defensible first guesses based on signal informativeness:
- Direct contract-level signals (stall, creep) weighted highest
- Entity-level signals (bunching, concentration) weighted moderately
- Inherited contractor signals weighted lower to avoid double-counting
- Award speed uses absolute value (both fast and slow are interesting)

**Distribution:** Composite ranges from -5.5 to 11.4, median -0.2. P90 = 1.84, P99 = 4.16.

---

## 5. What This Tool Claims

This tool identifies **contracts, contractors, and contracting entities whose procurement patterns are statistically unusual across multiple dimensions.**

It surfaces patterns that warrant human review:
- Contracts with stalled payments and schedule overruns
- Contractors with portfolio-wide value creep above peers
- Entities with concentrated contractor relationships
- Procurement processes with anomalous timing
- Contract values clustered below regulatory thresholds

---

## 6. What This Tool Does NOT Claim

- **No claims of fraud, corruption, or illegality.** Statistical anomaly is not evidence of wrongdoing.
- **No causal inference.** The composite score reflects correlation of unusual patterns, not causation.
- **No prediction of outcomes.** The tool does not predict whether a project will fail or succeed.
- **No replacement for audit.** Flagged contracts require human review of source documents (contracts, amendment PDFs, site visits) before any conclusions.

---

## 7. Known Blind Spots

1. **No satellite ground-truth.** Construction progress is inferred from payment data, not observed.
2. **No audit-outcome labels.** Without Contraloría findings as ground truth, we cannot estimate false-positive/negative rates.
3. **Reference population includes flagged observations.** Z-scores are slightly conservative (outliers pull the mean/std toward them).
4. **SECOP I entities are excluded.** Some entities (especially pre-2020) may have filed on SECOP I.
5. **Bid count data quality is poor.** The 67% single-bidder baseline suggests the field is often unpopulated rather than reflecting true single-bidder rates.
6. **Entity budget thresholds are simplified.** Fixed national thresholds used instead of entity-specific budget tiers.
7. **Value creep conflates bid premium and amendments.** Without structured amendment value data, we cannot distinguish between high initial bids and post-award value increases.
8. **Temporal lag.** Data reflects SECOP II state at ingestion (April 2026). Recent changes not captured.

---

## 8. How to Extend

1. **Add audit-outcome labels** from Contraloría General findings. This enables supervised weight tuning and false-positive estimation.
2. **Add satellite imagery layer** for construction subset. Compare declared progress (this pipeline) against observed construction from Sentinel-1/2.
3. **Add entity-specific budget thresholds** from DNP or Contraloría data for more accurate bunching detection.
4. **Add entity type covariate** to time-to-award model. This would correctly handle intergovernmental transfers.
5. **Expand relationship graph** to all contract types for more statistical power.
6. **Supervised weight optimization** once labeled outcomes are available — replace first-guess weights with logistic regression or gradient-boosted weights.
7. **Temporal dynamics** — track signal evolution over time to identify deteriorating patterns (contractor developing stall habit, entity's HHI increasing).
