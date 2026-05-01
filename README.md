# tool-calling-dq-agent

An autonomous data quality monitoring agent that inspects Delta table health across Databricks projects — using **true tool-calling**, where Claude drives the investigation loop, not your code.

Built with **Claude API** + **Databricks**.

> *Your other agents follow a script. This one doesn't.*
> Claude receives a goal and a toolkit — and decides what to investigate, in what order, based on what it finds.

---

## Business Impact

> In a single automated run — 124 tool calls, 7 iterations, zero human direction — the agent autonomously discovered a 16-day pipeline staleness root cause, a schema design gap hiding failure diagnostics, and 3 zero-order customers from a LEFT JOIN artifact. Findings that would take a data engineer hours to uncover manually.

## The Architectural Distinction

Every agent in this portfolio reasons through prompts. This one is different.

| Pattern | Who decides the next step? | This project |
|---|---|---|
| Prompt-based (all prior agents) | Your Python code | ✗ |
| **Tool-calling (this agent)** | **Claude** | ✓ |

In prompt-based agents your code orchestrates the sequence — Eyes → Brain → Hands — and Claude fills in the reasoning at each step. In this agent Claude holds the loop. It calls tools, receives results, decides what to investigate next, and stops when satisfied. Your code only executes what Claude asks for.

```
Your code: "Here's your goal and your toolkit. Go."
    ↓
Claude: list_tables()           → sees 26 tables across 2 projects
Claude: check_row_counts() ×26  → all normal
Claude: check_freshness() ×26   → 22 stale tables detected
Claude: check_null_rates() ×26  → DT nulls flagged, investigates further
Claude: run_custom_sql(...)      → writes its own SQL to dig deeper
Claude: run_custom_sql(...)      → follows a thread it noticed
Claude: "I have enough. Here's my report."
    ↓
Your code: writes to Delta + saves markdown
```

---

## What the Agent Monitors

**26 Delta tables across 2 projects:**

| Project | Tables | Layers |
|---|---|---|
| `ecommerce` | 17 | Bronze · Silver · Gold · Other |
| `offset_well_crew` | 9 | Bronze · Silver · Gold |

---

## The Toolkit — 6 Tools Claude Calls Autonomously

| Tool | What it does | Anomaly threshold |
|---|---|---|
| `list_tables` | Inventory all monitored tables with baselines | — |
| `check_row_counts` | Current vs baseline row count | >10% deviation |
| `check_freshness` | Last modified timestamp | >48 hours stale |
| `check_null_rates` | Null % per column vs baseline | >5pp increase |
| `check_schema` | Columns added or removed | Any drift |
| `run_custom_sql` | Claude-generated read-only SQL | Claude decides when to use |

`run_custom_sql` is the most powerful — Claude writes arbitrary SELECT queries mid-investigation to follow threads it notices. No human wrote those queries. A safety guard blocks all destructive keywords (`DROP`, `DELETE`, `INSERT`, etc.).

---

## What Claude Found Autonomously

In a single run — **124 tool calls across 7 iterations** — Claude discovered findings that would take a data engineer hours to uncover manually:

- **April 15 crash cluster root cause** — connected 6 rapid pipeline failures (avg 2.5s, BRONZE layer) to 4 tables that had been stale for 16 days. The streaming and enrichment workflows broke that day and were never rebuilt.
- **`failed_checks` schema design issue** — `pipeline_runs.failed_checks` is NULL in 96.15% of rows, including all BRONZE-layer failures. The column is only populated on QUALITY_CHECK failures, leaving all other failure types undiagnosed.
- **3 zero-order "Loyal" customers** — identified from null spend patterns in `gold_customer_segments`. Customers with 800+ days tenure and zero purchases — a LEFT JOIN artifact, not a pipeline defect, but flagged for CRM review.
- **DT nulls correctly attributed** — `bronze_well_logs` shows 50.51% null DT. Claude cross-referenced `well_registry.has_dt` and the silver QC flags to confirm this is a documented instrument gap for wells 15_9-F-1C and 15_9-F-11B — not a pipeline defect.

---

## Output — Two Destinations, Two Purposes

| Destination | What's stored | Who uses it |
|---|---|---|
| **`dq_monitor.gold_dq_reports`** (Delta table) | Structured run metadata — timestamps, tool call counts, status, full report text, complete tool call log as JSON | Queryable over time — trend analysis, run comparison, dashboards |
| **`dq_report.md`** (Databricks Volume) | Human-readable narrative — executive summary, findings tables, prioritized recommendations | Engineers and stakeholders who want to read the report |

The Gold table enables longitudinal tracking — after 30 daily runs you can query whether Claude is finding more or fewer issues as pipelines stabilize. The markdown file is what you share in a standup or attach to a ticket.

---

## Sample Output

See [`sample_output/dq_report_sample.md`](sample_output/dq_report_sample.md) for a real agent run output across all 26 tables.

**Excerpt — Freshness findings:**

> *22 of 26 tables are STALE. Four tables created on April 14 have never been refreshed — the April 15 crash cluster halted streaming and enrichment workflows entirely. All 19 subsequent pipeline runs succeeded but did not rebuild these tables.*

**Recommended actions (agent-generated, ranked by severity):**
1. 🔴 Restart streaming pipeline — `bronze_orders_stream` stale 16 days
2. 🔴 Rebuild `gold_customer_segments` and `silver_customers_enriched`
3. 🟠 Resume pricing engine — idle since April 22
4. 🟡 Fix `failed_checks` logging for BRONZE failures
5. ℹ️ Review 3 zero-order Loyal customers — possible data linkage issue

---

## Phases

| Phase | Notebook | Description |
|---|---|---|
| 1 | `Phase1_ToolDefinitions_Registry.py` | Discover tables, compute baselines, define and validate 6 tools |
| 2 | `Phase2_ToolCallingAgentLoop.py` | Core tool-calling loop — Claude drives the investigation |
| 3 | `Phase3_ReportSynthesis.py` | Persist output to Gold Delta table + markdown Volume file |
| 4 | `Phase4_ProductionRun.py` | Production entry point with rate limit retry — runs as scheduled Databricks Job |

---

## Delta Table Inventory

| Layer | Table | Description |
|---|---|---|
| Registry | `dq_monitor.monitored_tables` | 26 tables with baselines — row counts, null rates, schemas, last modified |
| Gold | `dq_monitor.gold_dq_reports` | Every agent run — timestamped, auditable, queryable |

---

## Scheduling

Phase 4 is designed to run as a daily Databricks Job:

1. Go to **Jobs & Pipelines** → **Create Job**
2. Name: `tool-calling-dq-agent-daily`
3. Task name: `dq-agent-daily-run`
4. Notebook: `Phase4_ProductionRun`
5. Cluster: Serverless
6. Schedule: Daily at preferred time
7. Save

Rate limit retry is built in — if the agent hits the API rate limit mid-run it waits 65 seconds and retries automatically (up to 3 attempts).

---

## Tech Stack

| Component | Technology |
|---|---|
| AI reasoning | Claude API (claude-sonnet-4-6) — tool-calling mode |
| Data platform | Databricks (Serverless) |
| Storage | Delta Lake |
| Data processing | PySpark |
| Language | Python 3 |

---

## Setup

**Prerequisites:** Databricks workspace, Anthropic API key

1. Run `Phase1_ToolDefinitions_Registry.py` — discovers tables and computes baselines
2. Run `Phase2_ToolCallingAgentLoop.py` — validates the tool-calling loop
3. Run `Phase3_ReportSynthesis.py` — confirms Gold table output
4. Schedule `Phase4_ProductionRun.py` as a daily Databricks Job

Add your Anthropic API key in the configuration cell of each notebook (Phases 2–4).

---

## Related Projects

| Repo | Pattern | Description |
|---|---|---|
| [data-incident-agent](https://github.com/venkatchittoor/data-incident-agent) | Prompt-based | Monitoring agent — Eyes/Brain/Hands |
| [pricing-decision-agent](https://github.com/venkatchittoor/pricing-decision-agent) | Prompt-based | Confidence-gated autonomous decisions |
| [customer-behavior-crew](https://github.com/venkatchittoor/customer-behavior-crew) | Prompt-based | Multi-agent crew with LLM routing |
| [offset-well-intelligence-crew](https://github.com/venkatchittoor/offset-well-intelligence-crew) | Prompt-based | Multi-agent O&G well intelligence |
| [drilling-npt-agent](https://github.com/venkatchittoor/drilling-npt-agent) | Prompt-based | Domain-expert NPT monitoring |

---

*Built by [VC](https://github.com/venkatchittoor) — demonstrating the full spectrum of agentic AI patterns, from prompt-based orchestration to true tool-calling.*
