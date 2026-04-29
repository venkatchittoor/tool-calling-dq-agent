# Databricks notebook source
# MAGIC %md
# MAGIC # Agentic Data Quality Monitor
# MAGIC ## Phase 1: Tool Definitions + Table Registry
# MAGIC
# MAGIC Establishes the foundation for the tool-calling agent:
# MAGIC - Discovers all monitored Delta tables across both projects
# MAGIC - Computes baselines from current state (row counts, schemas, null rates)
# MAGIC - Writes baselines to `dq_monitor.monitored_tables` registry
# MAGIC - Defines the 6 tools Claude will call autonomously
# MAGIC
# MAGIC **Projects monitored:**
# MAGIC - `ecommerce` — Bronze/Silver/Gold ecommerce pipeline tables
# MAGIC - `offset_well_crew` — Bronze/Silver/Gold well intelligence tables

# COMMAND ----------
# MAGIC %md ### Step 1: Create database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS dq_monitor")
spark.sql("USE dq_monitor")
print("Database ready: dq_monitor")

# COMMAND ----------
# MAGIC %md ### Step 2: Discover all monitored tables

# COMMAND ----------

from pyspark.sql.functions import col
import pandas as pd
from datetime import datetime

# Define projects and their schemas to monitor
MONITORED_PROJECTS = {
    "ecommerce": {
        "catalog": "workspace",
        "schema": "ecommerce",
        "layer_map": {
            "bronze": ["bronze_customers", "bronze_products", "bronze_orders", "bronze_order_items"],
            "silver": ["silver_order_items"],
            "gold":   ["gold_revenue_by_category", "gold_top_customers", "gold_return_analysis"]
        }
    },
    "offset_well_crew": {
        "catalog": "workspace",
        "schema": "offset_well_crew",
        "layer_map": {
            "bronze": ["bronze_well_logs", "well_registry"],
            "silver": ["silver_log_qc_flags", "silver_formation_tops",
                      "silver_formation_deviations", "silver_reservoir_flags",
                      "silver_drillability_forecast"],
            "gold":   ["gold_well_reports", "agent_registry"]
        }
    }
}

# Build flat list of all tables
all_tables = []
for project, config in MONITORED_PROJECTS.items():
    schema = config["schema"]
    for layer, tables in config["layer_map"].items():
        for table in tables:
            full_name = f"{schema}.{table}"
            all_tables.append({
                "project":    project,
                "schema":     schema,
                "table_name": table,
                "full_name":  full_name,
                "layer":      layer,
            })

print(f"Total tables to monitor: {len(all_tables)}")
for t in all_tables:
    print(f"  {t['layer']:8} | {t['full_name']}")

# COMMAND ----------
# MAGIC %md ### Step 3: Compute baselines from current state

# COMMAND ----------

from pyspark.sql import functions as F
import json

def compute_table_baseline(full_name, project, layer):
    """Compute baseline metrics for a single Delta table."""
    try:
        df = spark.table(full_name)
        row_count = df.count()
        columns = df.columns
        col_count = len(columns)

        # Compute null rates per column
        null_rates = {}
        for c in columns:
            null_count = df.filter(F.col(c).isNull()).count()
            null_rates[c] = round(null_count / row_count * 100, 2) if row_count > 0 else 0.0

        # Get schema as JSON string
        schema_json = df.schema.json()

        # Get last modified timestamp from Delta history
        try:
            history = spark.sql(f"DESCRIBE HISTORY {full_name} LIMIT 1")
            last_modified = history.select("timestamp").collect()[0][0]
            last_modified_str = str(last_modified)
        except:
            last_modified_str = "unknown"

        return {
            "full_name":          full_name,
            "project":            project,
            "layer":              layer,
            "baseline_row_count": row_count,
            "column_count":       col_count,
            "columns":            json.dumps(columns),
            "baseline_null_rates": json.dumps(null_rates),
            "baseline_schema":    schema_json,
            "last_modified":      last_modified_str,
            "baseline_computed_at": datetime.now().isoformat(),
            "status":             "active"
        }
    except Exception as e:
        print(f"  Error computing baseline for {full_name}: {e}")
        return {
            "full_name":          full_name,
            "project":            project,
            "layer":              layer,
            "baseline_row_count": -1,
            "column_count":       -1,
            "columns":            "[]",
            "baseline_null_rates": "{}",
            "baseline_schema":    "{}",
            "last_modified":      "unknown",
            "baseline_computed_at": datetime.now().isoformat(),
            "status":             "error"
        }

# Compute baselines for all tables
print("Computing baselines...")
baselines = []
for t in all_tables:
    print(f"  Processing: {t['full_name']}...")
    baseline = compute_table_baseline(t["full_name"], t["project"], t["layer"])
    baselines.append(baseline)
    if baseline["status"] == "active":
        print(f"    Rows: {baseline['baseline_row_count']:,} | Cols: {baseline['column_count']}")

print(f"\nBaselines computed: {len(baselines)}")

# COMMAND ----------
# MAGIC %md ### Step 4: Write table registry to Delta

# COMMAND ----------

df_registry = spark.createDataFrame(pd.DataFrame(baselines))

(df_registry
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("dq_monitor.monitored_tables")
)

print("Table registry written: dq_monitor.monitored_tables")
spark.table("dq_monitor.monitored_tables") \
    .select("project", "layer", "full_name", "baseline_row_count", "column_count", "status") \
    .orderBy("project", "layer", "full_name") \
    .show(truncate=50)

# COMMAND ----------
# MAGIC %md ### Step 5: Define the 6 tools Claude will call
# MAGIC
# MAGIC These are the Python functions that back each tool.
# MAGIC Claude will call these autonomously during Phase 2.

# COMMAND ----------

import json
from pyspark.sql import functions as F

# ── Safety guard for run_custom_sql ──────────────────────────────────────────
FORBIDDEN_KEYWORDS = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE",
                      "CREATE", "REPLACE", "MERGE", "OVERWRITE"]

def is_safe_query(sql):
    """Return True if query is read-only safe."""
    sql_upper = sql.upper()
    for kw in FORBIDDEN_KEYWORDS:
        if kw in sql_upper:
            return False, kw
    return True, None

# ── Tool 1: list_tables ───────────────────────────────────────────────────────
def list_tables():
    """List all monitored tables with baseline info."""
    df = spark.table("dq_monitor.monitored_tables")
    rows = df.select(
        "project", "layer", "full_name",
        "baseline_row_count", "column_count", "status"
    ).orderBy("project", "layer").collect()

    result = []
    for row in rows:
        result.append({
            "full_name":            row["full_name"],
            "project":              row["project"],
            "layer":                row["layer"],
            "baseline_row_count":   row["baseline_row_count"],
            "column_count":         row["column_count"],
            "status":               row["status"]
        })
    return {"tables": result, "total": len(result)}

# ── Tool 2: check_row_counts ──────────────────────────────────────────────────
def check_row_counts(table_name):
    """Check current row count vs baseline for a table."""
    try:
        # Get baseline
        baseline_row = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name") == table_name).collect()

        if not baseline_row:
            return {"error": f"Table {table_name} not in registry"}

        baseline_count = baseline_row[0]["baseline_row_count"]

        # Get current count
        current_count = spark.table(table_name).count()

        # Compute deviation
        if baseline_count > 0:
            deviation_pct = round((current_count - baseline_count) / baseline_count * 100, 2)
        else:
            deviation_pct = 0.0

        # Flag if deviation > 10%
        flag = abs(deviation_pct) > 10

        return {
            "table_name":      table_name,
            "baseline_count":  baseline_count,
            "current_count":   current_count,
            "deviation_pct":   deviation_pct,
            "anomaly_flag":    flag,
            "assessment":      "ANOMALY" if flag else "NORMAL"
        }
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

# ── Tool 3: check_freshness ───────────────────────────────────────────────────
def check_freshness(table_name):
    """Check when a table was last modified."""
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 5")
        rows = history.select("version", "timestamp", "operation").collect()

        last_modified = rows[0]["timestamp"]
        hours_since = (datetime.now() - last_modified.replace(tzinfo=None)).total_seconds() / 3600

        # Flag if stale > 48 hours
        flag = hours_since > 48

        return {
            "table_name":      table_name,
            "last_modified":   str(last_modified),
            "hours_since":     round(hours_since, 1),
            "anomaly_flag":    flag,
            "assessment":      "STALE" if flag else "FRESH",
            "recent_operations": [{"version": r["version"],
                                   "timestamp": str(r["timestamp"]),
                                   "operation": r["operation"]} for r in rows]
        }
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

# ── Tool 4: check_null_rates ──────────────────────────────────────────────────
def check_null_rates(table_name):
    """Check null rates per column vs baseline."""
    try:
        # Get baseline null rates
        baseline_row = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name") == table_name).collect()

        if not baseline_row:
            return {"error": f"Table {table_name} not in registry"}

        baseline_nulls = json.loads(baseline_row[0]["baseline_null_rates"])

        # Compute current null rates
        df = spark.table(table_name)
        row_count = df.count()
        current_nulls = {}
        flagged_columns = []

        for c in df.columns:
            null_count = df.filter(F.col(c).isNull()).count()
            current_rate = round(null_count / row_count * 100, 2) if row_count > 0 else 0.0
            current_nulls[c] = current_rate
            baseline_rate = baseline_nulls.get(c, 0.0)

            # Flag if null rate increased by more than 5 percentage points
            if current_rate - baseline_rate > 5.0:
                flagged_columns.append({
                    "column":        c,
                    "baseline_rate": baseline_rate,
                    "current_rate":  current_rate,
                    "delta":         round(current_rate - baseline_rate, 2)
                })

        return {
            "table_name":       table_name,
            "row_count":        row_count,
            "current_null_rates": current_nulls,
            "baseline_null_rates": baseline_nulls,
            "flagged_columns":  flagged_columns,
            "anomaly_flag":     len(flagged_columns) > 0,
            "assessment":       "ANOMALY" if flagged_columns else "NORMAL"
        }
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

# ── Tool 5: check_schema ──────────────────────────────────────────────────────
def check_schema(table_name):
    """Check current schema vs baseline — detect drift."""
    try:
        # Get baseline schema
        baseline_row = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name") == table_name).collect()

        if not baseline_row:
            return {"error": f"Table {table_name} not in registry"}

        baseline_cols = json.loads(baseline_row[0]["columns"])
        current_cols  = spark.table(table_name).columns

        added   = [c for c in current_cols if c not in baseline_cols]
        removed = [c for c in baseline_cols if c not in current_cols]
        flag    = len(added) > 0 or len(removed) > 0

        return {
            "table_name":      table_name,
            "baseline_columns": baseline_cols,
            "current_columns":  current_cols,
            "columns_added":    added,
            "columns_removed":  removed,
            "anomaly_flag":     flag,
            "assessment":       "SCHEMA_DRIFT" if flag else "NORMAL"
        }
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

# ── Tool 6: run_custom_sql ────────────────────────────────────────────────────
def run_custom_sql(query):
    """Execute a read-only SQL query. Claude uses this for open-ended investigation."""
    safe, forbidden_kw = is_safe_query(query)
    if not safe:
        return {
            "error": f"Query rejected — contains forbidden keyword: {forbidden_kw}",
            "query": query,
            "status": "BLOCKED"
        }
    try:
        df = spark.sql(query)
        rows = df.limit(50).collect()
        result = [row.asDict() for row in rows]
        return {
            "query":        query,
            "row_count":    len(result),
            "results":      result,
            "status":       "SUCCESS"
        }
    except Exception as e:
        return {"error": str(e), "query": query, "status": "ERROR"}

# ── Tool dispatcher ───────────────────────────────────────────────────────────
def dispatch_tool(tool_name, tool_input):
    """Route Claude's tool call to the correct Python function."""
    if tool_name == "list_tables":
        return list_tables()
    elif tool_name == "check_row_counts":
        return check_row_counts(tool_input.get("table_name"))
    elif tool_name == "check_freshness":
        return check_freshness(tool_input.get("table_name"))
    elif tool_name == "check_null_rates":
        return check_null_rates(tool_input.get("table_name"))
    elif tool_name == "check_schema":
        return check_schema(tool_input.get("table_name"))
    elif tool_name == "run_custom_sql":
        return run_custom_sql(tool_input.get("query"))
    else:
        return {"error": f"Unknown tool: {tool_name}"}

print("All 6 tools defined and dispatcher ready.")
print("\nTools available to Claude:")
tools_list = ["list_tables", "check_row_counts", "check_freshness",
              "check_null_rates", "check_schema", "run_custom_sql"]
for t in tools_list:
    print(f"  ✓ {t}")

# COMMAND ----------
# MAGIC %md ### Step 6: Validate tool functions

# COMMAND ----------

print("=== Testing: list_tables ===")
result = list_tables()
print(f"Total tables: {result['total']}")
for t in result['tables']:
    print(f"  {t['layer']:8} | {t['full_name']:50} | {t['baseline_row_count']:>8} rows")

print("\n=== Testing: check_row_counts ===")
result = check_row_counts("offset_well_crew.bronze_well_logs")
print(json.dumps(result, indent=2))

print("\n=== Testing: check_freshness ===")
result = check_freshness("offset_well_crew.bronze_well_logs")
print(json.dumps(result, indent=2, default=str))

print("\n=== Testing: check_null_rates ===")
result = check_null_rates("offset_well_crew.silver_log_qc_flags")
print(json.dumps(result, indent=2))

print("\n=== Testing: check_schema ===")
result = check_schema("ecommerce.silver_order_items")
print(json.dumps(result, indent=2))

print("\n=== Testing: run_custom_sql (safe) ===")
result = run_custom_sql("SELECT well_name, COUNT(*) as cnt FROM offset_well_crew.silver_log_qc_flags GROUP BY well_name")
print(json.dumps(result, indent=2, default=str))

print("\n=== Testing: run_custom_sql (blocked) ===")
result = run_custom_sql("DROP TABLE offset_well_crew.bronze_well_logs")
print(json.dumps(result, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 1 Complete ✅
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `dq_monitor.monitored_tables` | Registry of all monitored tables with baselines |
# MAGIC
# MAGIC **6 tools defined and validated:**
# MAGIC | Tool | Purpose |
# MAGIC |------|---------|
# MAGIC | `list_tables` | List all monitored tables with baselines |
# MAGIC | `check_row_counts` | Current vs baseline row count |
# MAGIC | `check_freshness` | Last modified timestamp — stale detection |
# MAGIC | `check_null_rates` | Null % per column vs baseline |
# MAGIC | `check_schema` | Schema drift detection |
# MAGIC | `run_custom_sql` | Claude-generated read-only SQL investigation |
# MAGIC
# MAGIC **Next:** Phase 2 — Tool-Calling Agent Loop
