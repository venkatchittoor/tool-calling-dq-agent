# Data Quality Report
**Run ID:** 20260430_044842
**Timestamp:** 2026-04-30T04:48:42.714495
**Tables Monitored:** 26 (ecommerce: 17, offset_well_crew: 9)
**Tool Calls:** 115 across 6 iterations

---

Excellent — all investigations are complete. I now have a comprehensive, fully verified picture. Here is the full summary:

---

# 📊 Data Quality Assessment — Full Report
**Scope:** 26 Delta tables | Projects: `ecommerce`, `offset_well_crew` | Checks: Row Counts, Freshness, Null Rates, Schema Drift + 10 custom SQL deep-dives

---

## ✅ What's Healthy Across All Tables

| Dimension | Result |
|---|---|
| **Row Counts** | ✅ All 26 tables — 0% deviation from baseline |
| **Schema Drift** | ✅ All 26 tables — no columns added or removed |
| **Null Rates (automated flags)** | ✅ No columns breached the +5pp threshold |

---

## 🔴 Critical Issues

### 1. `ecommerce.pipeline_runs` — `failed_checks` is 96.15% NULL (Logging Bug)
**Severity: HIGH**
- The `failed_checks` column — which should record why a pipeline failed — is **null in 25 out of 26 runs**, including all 6 of the 7 FAILED status runs.
- Only **1 run** (the earliest failure, `QUALITY_CHECK` layer) ever populated `failed_checks`, and it revealed a meaningful alert: *"bronze_orders: row_count (expected >= 5,000, actual 1,000)"*.
- All 6 subsequent failures (April 15) stalled at the **BRONZE layer in under 3 seconds** with no diagnostic data written.
- All 19 SUCCESS runs also have `failed_checks = NULL`, confirming the field is never populated on success — but the failure-case null is a **critical observability gap**.
- **Action:** Fix the pipeline error-logging code to always write a failure reason when `status = 'FAILED'`. The April 15 cluster of 6 fast BRONZE failures (avg ~2.5s) suggests a connectivity or permissions issue that went entirely undiagnosed.

---

## 🟠 Moderate Issues

### 2. Widespread Staleness — 22 of 26 Tables Are Stale (>48h)
**Severity: MODERATE–HIGH**

| Category | Tables | Hours Since Last Write |
|---|---|---|
| **Critically stale (>370h / ~16 days)** | `bronze_orders_stream`, `gold_stream_anomalies`, `silver_customers_enriched`, `gold_customer_segments` | 378–384h |
| **Stale ~60h (~2.5 days)** | Most active bronze/silver/gold ecommerce tables | ~60h |
| **Stale ~49–60h (just over threshold)** | `bronze_well_logs`, `well_registry`, `silver_log_qc_flags`, `silver_formation_tops`, `silver_formation_deviations` | 49–60h |
| **FRESH ✅** | `gold_well_reports`, `agent_registry`, `silver_reservoir_flags`, `silver_drillability_forecast` | 43–46h |

Notable patterns:
- `ecommerce.silver_customers_enriched` and `ecommerce.gold_customer_segments` were **created once via `CREATE OR REPLACE TABLE AS SELECT`** on April 14 and have **never been refreshed**. These are frozen snapshots, not living tables.
- `ecommerce.bronze_orders_stream` and `ecommerce.gold_stream_anomalies` were also last written April 14 — the stream pipeline appears **entirely stalled**.
- Most other ecommerce tables run on a recurring daily pattern but are now ~2.5 days behind — likely the pipeline that last ran April 27 has not re-triggered.

---

### 3. `offset_well_crew.bronze_well_logs` — DT (Sonic Log) 50.51% NULL, but **Expected and Registry-Consistent**
**Severity: LOW (documented, not a defect)**
- Deep-dive confirms DT nulls are **fully explained by the well registry**:
  - `15_9-F-1C` (current well): `has_dt = false` → 100% DT null ✅
  - `15_9-F-11B` (offset): `has_dt = false` → 100% DT null ✅
  - `15_9-F-11A` and `15_9-F-1A` (offsets): `has_dt = true` → 0% DT null ✅
  - **`15_9-F-1B` is absent from the registry** — it has 3,001 rows in `bronze_well_logs` with no DT nulls but no registry entry. This is a minor referential integrity gap worth noting.
- **Action:** Add `15_9-F-1B` to `well_registry`. The DT nulls are inherent to tool availability and are correctly flagged in `silver_log_qc_flags` (15 QC flag records).

---

## 🟡 Minor Issues

### 4. `ecommerce.gold_customer_segments` — 3 Customers With NULL Spend/Orders
**Severity: LOW (legitimate business case)**
- Customers **75 (Krista Bell MD, Gambia)**, **162 (Cynthia Wallace, Finland)**, and **175 (Harold Morgan, Norway)** have `total_spend`, `total_orders`, and `avg_order_value` all NULL.
- Confirmed via a join to `bronze_orders` — these 3 customers exist in `bronze_customers` but **have placed zero orders**.
- Notably, 2 of the 3 are in the **"Loyal" tenure segment** (800+ days since signup), which is a labeling inconsistency: long-tenured customers with no orders shouldn't be "Loyal."
- **Action:** Update the segmentation logic to handle zero-order customers — either exclude them, assign a "Lapsed" segment, or fill nulls with `0`.

### 5. `offset_well_crew.gold_well_reports` — 25% NULL on `question`
**Severity: LOW (by design)**
- The null row is `report_type = 'full_report'` — a comprehensive report that isn't driven by a specific question. The 3 `report_type = 'question'` rows all have valid question text.
- **Action:** No fix needed; consider documenting that `question` is intentionally null for `full_report` rows, or add a NOT NULL constraint only on rows where `report_type = 'question'`.

---

## 📋 Summary Scorecard

| Check | # Tables Passed | # Tables Flagged |
|---|---|---|
| Row Counts | 26 / 26 ✅ | 0 |
| Schema Drift | 26 / 26 ✅ | 0 |
| Null Rate Thresholds | 26 / 26 ✅ | 0 (automated) |
| Freshness (48h SLA) | 4 / 26 ✅ | **22 🔴** |
| `pipeline_runs` logging | ❌ | `failed_checks` never populated |
| DT nulls vs. registry | ✅ Consistent | 1 unregistered well |
| 0-order customers in gold | ⚠️ | 3 customers / labeling issue |
| `gold_well_reports.question` | ✅ By design | — |

---

## 🔧 Recommended Actions (Priority Order)

1. **[P1] Fix pipeline failure logging** — `failed_checks` must be populated on every `FAILED` run. Investigate the April 15 cluster of 6 BRONZE-layer failures.
2. **[P1] Restart the streaming pipeline** — `bronze_orders_stream` and `gold_stream_anomalies` are 16 days stale.
3. **[P2] Refresh stale static tables** — Schedule recurring jobs for `silver_customers_enriched` and `gold_customer_segments` (both have only ever been written once, 16 days ago).
4. **[P2] Resume regular pipeline runs** — The main ecommerce pipeline last ran April 27; needs to re-trigger.
5. **[P3] Fix `gold_customer_segments` null handling** — Replace NULLs with 0 for spend/orders on no-order customers and correct their `tenure_segment` label.
6. **[P3] Add `15_9-F-1B` to `well_registry`** — Close the referential integrity gap in the offset_well_crew project.
