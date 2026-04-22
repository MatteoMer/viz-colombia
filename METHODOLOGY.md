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

**vp>0 restriction:** S1 is restricted to contracts with recorded payments (`valor_pagado > 0`). 58.8% of Obra contracts have `valor_pagado = 0` in SECOP II, including 47.8% of closed/completed contracts. For these contracts, the stall signal is not evaluated and set to neutral (0σ). Without this filter, 85% of stall flags were driven by reporting gaps rather than actual stalled payments.

**Limitations:** `valor_pagado` is a cumulative snapshot in SECOP II, not a time series. Linear interpolation between anchor points (0% at start, current ratio at now) smooths actual payment patterns.

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

**Year-specific SMMLV thresholds:**

| Year | SMMLV (COP) | Mínima cuantía (28×) | Menor cuantía low (280×) | Menor cuantía high (1000×) |
|------|------------|---------------------|-------------------------|--------------------------|
| 2020 | 877,803 | 24.6M | 245.8M | 877.8M |
| 2021 | 908,526 | 25.4M | 254.4M | 908.5M |
| 2022 | 1,000,000 | 28.0M | 280.0M | 1,000M |
| 2023 | 1,160,000 | 32.5M | 324.8M | 1,160M |
| 2024 | 1,300,000 | 36.4M | 364.0M | 1,300M |

**Note:** Actual thresholds vary by entity budget tier. A production system should use entity-specific budget data from Contraloría or DNP.

**Formula:** For each (entity, year, threshold T):
- `bunching_ratio = count([0.85T, T)) / count((T, 1.15T])`

**Permutation null model:** Each (entity, year, threshold) is tested against 500 random permutations of the entity's contract values. The p-value is the fraction of permuted ratios that equal or exceed the observed ratio. Only statistically significant bunching (p < 0.05) contributes to the composite score. This eliminates false positives from small samples where high ratios arise by chance.

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
log(days_to_award + 1) ~ log(valor_estimado) + C(procurement_method) + C(year) + C(entity_level)
```

The `entity_level` covariate (Territorial, Nacional, etc.) accounts for structural differences in award timing between municipal, departmental, and national entities. This reduces false positives from intergovernmental transfers that are legitimately awarded quickly.

**Residual z-score:** Standardized residual from the OLS. |z| > 2 is flagged.

- **Fast anomalies (z < -2):** Possible pre-wired awards. 326 flagged (1.8%).
- **Slow anomalies (z > 2):** Capacity problems or contested processes. 596 flagged (3.2%).

**Limitation:** The model does not include UNSPSC category codes as covariates. Different construction categories (roads, buildings, water infrastructure) have structurally different timelines that are currently unmodeled.

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

**Dual contractor weighting:** Contractor-level signals (S2c, S3c) are computed with both value-weighted (large contracts dominate) and count-weighted (each contract equal) portfolio means. The composite uses `max(z_value_weighted, z_count_weighted)` to capture anomalous patterns regardless of whether they manifest in large or many-small contracts.

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

1. **valor_pagado field missingness (severity: high).** 58.8% of Obra contracts have `valor_pagado = 0` in SECOP II, including 47.8% of closed/completed contracts. S1 (payment stall) is restricted to contracts with recorded payments. For the remaining 58.8%, the stall signal is not evaluated and set to neutral (0σ). This means the tool cannot detect stalled payments for the majority of contracts.
2. **No satellite ground-truth.** Construction progress is inferred from payment data, not observed.
3. **No audit-outcome labels.** Without Contraloría findings as ground truth, we cannot estimate false-positive/negative rates.
4. **Reference population includes flagged observations.** Z-scores are slightly conservative (outliers pull the mean/std toward them).
5. **SECOP I entities are excluded.** Some entities (especially pre-2020) may have filed on SECOP I.
6. **Bid count data quality is poor.** The 67% single-bidder baseline suggests the field is often unpopulated rather than reflecting true single-bidder rates.
7. **Entity budget thresholds are simplified.** Year-specific national SMMLV used, but actual thresholds vary by entity budget tier.
8. **Value creep conflates bid premium and amendments.** Without structured amendment value data, we cannot distinguish between high initial bids and post-award value increases.
9. **Temporal lag.** Data reflects SECOP II state at ingestion (April 2026). Recent changes not captured.

---

## 8. Contextual Enrichment Layer

The raw composite score treats all signal-firing contracts equally. The contextual enrichment layer adds external data to distinguish **structural anomalies** (expected in a thin market) from **behavioral anomalies** (warrant investigation).

### 8.1 External Data Sources

| Source | Data | Use |
|--------|------|-----|
| DIVIPOLA (datos.gov.co) | Municipality codes, coordinates | Geographic crosswalk (99.6% match rate) |
| PDET list (datos.gov.co) | 170 conflict-affected municipalities | Thin-market card trigger |
| ZOMAC list (Finagro) | 344 post-conflict special zones | Thin-market card trigger |
| SECOP II Grupos de Proveedores | 290K consortium groups, 647K member rows | Consortium decomposition |

### 8.2 Consortium Decomposition

Of 28,601 Obra contracts, 5,993 are awarded to consortiums (Consorcio / Union Temporal / UT). The consortium vehicle hides the underlying member firms, making supplier-level signals (HHI, relationships, fragmentation) unreliable.

**Approach:** Match consortium supplier names to the SECOP II *Grupos de Proveedores* dataset. For matched consortiums, expand each contract into N member rows with `effective_value = awarded_value * participation_pct`. Signals S5 (HHI), S8 (relationships), S9 (fragmentation), S2c/S3c (contractor portfolios) are recomputed on the decomposed view.

### 8.3 Regimen Especial Sub-Cohorts

The 2,280 regimen especial contracts are split into 5 sub-cohorts based on entity name and object description:

| Sub-cohort | Detection | Count (approx.) |
|------------|-----------|-----------------|
| `especial_ese` | Entity name matches `E.S.E.` or `empresa social del estado` | ~800 |
| `especial_universidad` | Entity name matches `universidad` or `institucion universitaria` | ~200 |
| `especial_d092` | Object description matches `decreto 092` or `solidaridad` | ~100 |
| `especial_convenio` | Text matches `convenio interadministrativo` | ~300 |
| `especial_otro` | Catch-all remainder | ~800 |

Z-scores are conditioned within each sub-cohort, reducing false positives from structural differences between health entities and universities.

### 8.4 Context Cards

Each contract receives 0–N context cards explaining why signals fire:

| Card Type | Trigger | Confidence | Affected Signals |
|-----------|---------|------------|-----------------|
| **Thin market** | HHI or single-bidder firing + PDET/ZOMAC/fiscal cat 5–6 | High/Moderate | hhi, single |
| **Consortium** | Supplier is consortium form with matched members | Low | hhi, rel, frag |
| **Regimen subtype** | Contract is regimen especial sub-cohort | Moderate | bunch, hhi, speed |
| **Value plausibility** | Value >P90 or <P10 for (object category, fiscal category) | Low | creep_c |
| **No explanation** | Red signals (>1.5σ) with no benign card | Alert | — |

### 8.5 Context-Adjusted Composite

Each signal's z-score is multiplied by a factor based on the highest-confidence card covering it:

| Confidence | Multiplier |
|------------|-----------|
| High | 0.25× |
| Moderate | 0.50× |
| Low | 0.85× |
| No card | 1.00× |

`composite_adjusted = Σ(z_signal × weight × multiplier)`

The adjusted score is used for map coloring, department choropleth, and table sorting. Both raw and adjusted scores are shown in the detail panel. Correlation between raw and adjusted composites is >0.99 — the adjustment is targeted, not wholesale.

---

## 9. How to Extend

> Note: Items 1–7 below are from the original methodology. Items 8–9 are new.


1. **Add audit-outcome labels** from Contraloría General findings. This enables supervised weight tuning and false-positive estimation.
2. **Add satellite imagery layer** for construction subset. Compare declared progress (this pipeline) against observed construction from Sentinel-1/2.
3. **Add entity-specific budget thresholds** from DNP or Contraloría data for more accurate bunching detection.
4. **Add UNSPSC category covariates** to time-to-award model for construction sub-type control.
5. **Expand relationship graph** to all contract types for more statistical power.
6. **Supervised weight optimization** once labeled outcomes are available — replace first-guess weights with logistic regression or gradient-boosted weights.
7. **Temporal dynamics** — track signal evolution over time to identify deteriorating patterns (contractor developing stall habit, entity's HHI increasing).

---

## 10. Robustness Checks

### 10.1 Bootstrap Stability

1,000 bootstrap resamples of the reference population are used to assess ranking stability. For each resample, z-scores are recomputed from resampled mean/std and applied to all contracts. Contracts are flagged as `ranking_unstable` if they appear in the top-50 in fewer than 90% of resamples.

This distinguishes contracts that are robustly anomalous from those whose ranking depends on a few influential observations in the reference population.

### 10.2 Out-of-Sample Validation

The scored population is split temporally: train = 2020–2022, test = 2023–2024. Z-scores are computed on train-only statistics and applied to test contracts. The composite is then evaluated against three outcome proxies:

1. **Terminated/cancelled** — contract status indicates early termination
2. **Has amendments** — `dias_adicionados > 0` (any time extension)
3. **Large value creep** — `value_creep_ratio > 0.3` (>30% cost overrun)

AUC and Spearman rank correlation are reported for each proxy. AUC > 0.5 for at least one proxy provides evidence that the composite captures meaningful risk variation, not just noise.
