# Inspection Log — Top 20 Flagged Contracts

**Date:** 2026-04-21
**Inspector:** Automated + manual review of SECOP descriptions and signal patterns
**Note:** Full SECOP PDF review was not performed (Tier C). Assessment is based on contract metadata, signal patterns, and entity/contractor cross-referencing.

---

### 1. CO1.PCCNTR.5311998 — Ricaurte road paving (12.2B COP)
- **Entity:** Alcaldía Ricaurte | **Supplier:** Consorcio Diagonal RC
- **Signals:** Threshold bunching (3.2σ), Single bidder (0.9σ)
- **Assessment:** Ricaurte (small Cundinamarca municipality) shows strong bunching below mínima cuantía threshold. Combined with single-bidder flag on a 12B contract, suggests possible contract splitting. Road paving is a standard project type.
- **Verdict:** `pattern_holds`

### 2. CO1.PCCNTR.5033462 — Chiquinquirá rural roads (5.4B COP)
- **Entity:** Municipio de Chiquinquirá | **Supplier:** Unión Temporal MR-6
- **Signals:** Award speed (2.0σ), Concentration (2.0σ), Single bidder (1.0σ)
- **Assessment:** Part of "Programa Colombia Rural" — a national standardized rural road program with preset technical specs. Fast award likely reflects standardized process, not anomaly. Single bidder plausible for remote rural work.
- **Verdict:** `benign_explanation`

### 3. CO1.PCCNTR.3811713 — Mosquera sports parks (25.1B COP)
- **Entity:** Alcaldía Mosquera | **Supplier:** Consorcio Escenarios Deportivos
- **Signals:** Concentration (2.5σ), Single bidder (1.7σ)
- **Assessment:** Mosquera appears 4 times in top 20 (this, #6, #9, plus nearby Funza #11). Entity has very high HHI. 25B COP sports parks with single bidder is noteworthy.
- **Verdict:** `pattern_holds`

### 4. CO1.PCCNTR.3155050 — Soacha municipal HQ (119.3B COP)
- **Entity:** Municipio de Soacha | **Supplier:** Consorcio Edificar CGJ2
- **Signals:** Payment stall (1.7σ), Award speed (1.1σ), Concentration (0.5σ)
- **Assessment:** Largest contract in cohort. 119B COP municipal headquarters with payment stall. Soacha is Colombia's 7th largest city. Project of this magnitude with stalled payments warrants review.
- **Verdict:** `pattern_holds`

### 5. CO1.PCCNTR.4354825 — N. Santander road improvement (36.4B COP)
- **Entity:** Gobernación Norte de Santander | **Supplier:** Faro del Catatumbo S.A.S.
- **Signals:** Award speed (3.4σ fast), Concentration (0.9σ), Single bidder (0.6σ)
- **Assessment:** Awarded to "Faro del Catatumbo" — a departmental development entity (EICE). This is an intergovernmental transfer, not a competitive procurement. Fast award is expected for government-to-government transfers.
- **Verdict:** `benign_explanation`

### 6. CO1.PCCNTR.3138947 — Mosquera community center (16.6B COP)
- **Entity:** Alcaldía Mosquera | **Supplier:** Consorcio Génesis
- **Signals:** Concentration (2.5σ), Award speed (0.8σ), Single bidder (0.5σ)
- **Assessment:** Same Mosquera entity as #3. "Consorcio Génesis" also appears in #9 for schools. Two large contracts from same entity to same contractor group. Pattern is clear.
- **Verdict:** `pattern_holds`

### 7. CO1.PCCNTR.3695801 — Apartadó rural roads (6.3B COP)
- **Entity:** Alcaldía Apartadó | **Supplier:** Sigma Construcciones SAS
- **Signals:** Concentration (1.4σ), Single bidder (1.4σ), Relationship (1.3σ)
- **Assessment:** Urabá region (Apartadó) is remote, which may limit contractor competition naturally. But three signals firing together with relationship intensity is notable.
- **Verdict:** `ambiguous`

### 8. CO1.PCCNTR.2746801 — Villavicencio road (8.0B COP)
- **Entity:** Alcaldía Villavicencio | **Supplier:** ECOBRAS S.A.
- **Signals:** Award speed (3.3σ fast), Single bidder (0.7σ)
- **Assessment:** Terminated contract. Very fast award (awarded in ~2 days vs expected ~16). Villavicencio is a major city with sufficient contractor market. Speed anomaly is striking.
- **Verdict:** `ambiguous`

### 9. CO1.PCCNTR.3138231 — Mosquera schools (34.6B COP)
- **Entity:** Alcaldía Mosquera | **Supplier:** Consorcio Génesis
- **Signals:** Concentration (2.5σ), Award speed (0.9σ), Single bidder (0.5σ)
- **Assessment:** Same entity and contractor as #6. Third Mosquera contract in top 20. 34.6B COP for school construction from the same Consorcio Génesis. Clear contractor-entity concentration pattern.
- **Verdict:** `pattern_holds`

### 10. CO1.PCCNTR.4338955 — Ricaurte market plaza (7.5B COP)
- **Entity:** Alcaldía Ricaurte | **Supplier:** Consorcio Plaza Ricaurte 2023
- **Signals:** Bunching (3.2σ), Payment stall (1.5σ), Single bidder (0.9σ)
- **Assessment:** **SUSPENDED** contract. Same entity as #1. Market plaza construction suspended with payment stall and threshold bunching. Three distinct signals firing. Second Ricaurte contract in top 20.
- **Verdict:** `pattern_holds`

### 11. CO1.PCCNTR.3952242 — Funza coliseo (53.6B COP)
- **Entity:** Municipio de Funza | **Supplier:** Consorcio Coliseo 2022
- **Signals:** Concentration (2.6σ), Single bidder (0.7σ), Award speed (0.5σ)
- **Assessment:** **SUSPENDED** 53.6B COP sports coliseum. Funza is a Cundinamarca municipality near Bogotá (similar profile to Mosquera). Large suspended project with concentration signal.
- **Verdict:** `pattern_holds`

### 12. CO1.PCCNTR.2980808 — Paipa market plaza (23.2B COP)
- **Entity:** Gobernación de Boyacá | **Supplier:** Consorcio Plaza Paipa
- **Signals:** Concentration (2.0σ), Single bidder (1.0σ)
- **Assessment:** Departmental government project in a touristic town (Paipa has thermal baths). Market plaza construction. Gobernación as contracting entity has large portfolio; concentration may reflect size rather than capture.
- **Verdict:** `ambiguous`

### 13. CO1.PCCNTR.3660455 — Ibagué pool complex (20.7B COP)
- **Entity:** Alcaldía Ibagué | **Supplier:** Unión Temporal Unidad Deportiva 2022
- **Signals:** Award speed (1.8σ), Concentration (0.8σ), Single bidder (0.7σ)
- **Assessment:** Ibagué is Tolima's capital with significant sports infrastructure investment. Unidad Deportiva La 42 is a known complex. Moderately fast award for a specific facility.
- **Verdict:** `ambiguous`

### 14. CO1.PCCNTR.5450301 — Soacha sewage (12.2B COP)
- **Entity:** EPUXUA E.I.C.E. | **Supplier:** Consorcio Vertimientos 23
- **Signals:** Single bidder (1.7σ), Concentration (1.1σ), Award speed (0.8σ)
- **Assessment:** **SUSPENDED** wastewater connection project. EPUXUA is Soacha's water/sewage utility. Suspended + single bidder on court-ordered infrastructure (sentencia del Consejo de Estado).
- **Verdict:** `pattern_holds`

### 15. CO1.PCCNTR.3118691 — CVC PTAR Buga (6.2B COP)
- **Entity:** CVC (Corporación Autónoma Regional) | **Supplier:** Consorcio Colectores Buga 2022
- **Signals:** Single bidder (0.9σ), Slippage (0.9σ), Award speed (0.7σ)
- **Assessment:** CVC is the Valle del Cauca environmental authority. Wastewater treatment is specialized work with limited contractor pool. Schedule slippage on water treatment is common due to technical complexity.
- **Verdict:** `benign_explanation`

### 16. CO1.PCCNTR.3642649 — Tuluá skating rink (6.7B COP)
- **Entity:** Alcaldía Tuluá | **Supplier:** Consorcio Patinaje 2022
- **Signals:** Single bidder (1.4σ), Concentration (1.1σ)
- **Assessment:** Specialized sports infrastructure (skating rink). Tuluá appears twice (#16, #19) with same pattern. Limited competition for specialized work is plausible but Tuluá's repeat pattern is notable.
- **Verdict:** `ambiguous`

### 17. CO1.PCCNTR.3141143 — Cartago market plaza (12.6B COP)
- **Entity:** Municipio de Cartago | **Supplier:** Unión Temporal Plaza de Mercado Cartago
- **Signals:** Award speed (3.0σ fast), Single bidder (1.4σ), Payment stall (0.6σ)
- **Assessment:** Still in execution. Award in ~2 days (3.0σ fast) + single bidder + payment stall on 12.6B market plaza. Multiple independent signals.
- **Verdict:** `pattern_holds`

### 18. CO1.PCCNTR.3858733 — Ibagué animal center (8.0B COP)
- **Entity:** Alcaldía Ibagué | **Supplier:** Mario Gabriel Jimenez Martinez
- **Signals:** Contractor slip (0.9σ), Concentration (0.8σ), Single bidder (0.7σ)
- **Assessment:** Individual person as contractor (not a consortium/company) for an 8B COP animal center. Contractor slippage is the primary signal. Individual contractors on large projects are less common.
- **Verdict:** `ambiguous`

### 19. CO1.PCCNTR.3634972 — Tuluá weightlifting coliseum (9.6B COP)
- **Entity:** Alcaldía Tuluá | **Supplier:** Consorcio Coliseo de Pesas Tuluá 2022
- **Signals:** Single bidder (1.4σ), Concentration (1.1σ)
- **Assessment:** Same entity as #16. Second Tuluá sports infrastructure project with single bidder. Entity has concentrated contracting pattern for sports facilities.
- **Verdict:** `pattern_holds`

### 20. CO1.PCCNTR.4053384 — Ibagué aqueduct (26.9B COP)
- **Entity:** IBAL SA ESP (Ibagué water utility) | **Supplier:** Consorcio Cócora Ibaltol
- **Signals:** Concentration (1.8σ), Single bidder (1.7σ), Payment stall (1.5σ)
- **Assessment:** Large water utility aqueduct project (Bocatoma Cócora to PTAP La Pola). Three distinct signals. Water utility procurement with concentration + stall is concerning pattern.
- **Verdict:** `pattern_holds`

---

## Summary

| Verdict | Count |
|---------|-------|
| `pattern_holds` | 10 |
| `benign_explanation` | 3 |
| `ambiguous` | 7 |

**Key patterns observed:**
- **Mosquera cluster:** 3 contracts from same entity, 2 to same contractor (Consorcio Génesis). Clear concentration.
- **Ricaurte cluster:** 2 contracts from same entity with bunching + stall.
- **Tuluá cluster:** 2 sports facility contracts with single bidder + concentration.
- **Benign explanations** are mostly intergovernmental transfers (fast awards to departmental entities) and national standardized programs (Colombia Rural).
- **Ambiguous cases** are typically in regions with limited contractor markets (Urabá) or involve specialized work (water treatment).

**Signal quality assessment:**
- **Threshold bunching (S4)** fires strongly and correctly identifies municipalities with systematic splitting patterns.
- **Contractor concentration (S5)** is the most frequently firing signal — effective but may over-flag large entities with naturally concentrated portfolios.
- **Award speed (S7)** correctly identifies intergovernmental transfers as fast outliers. The signal could be improved by adding a "contracting entity type" covariate (EICE/ESP vs municipality).
- **Single bidder (S6)** has high baseline rate (67%), reducing its discriminating power. Still useful as a contributing signal.
- **Value creep (S2)** and **Slippage (S3)** fire less often in the top 20, suggesting the composite weighting is appropriate.
