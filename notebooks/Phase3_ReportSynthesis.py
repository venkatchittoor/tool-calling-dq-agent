# Databricks notebook source
# MAGIC %md
# MAGIC # Tool-Calling DQ Agent
# MAGIC ## Phase 3: Report Synthesis + Gold Delta Table
# MAGIC
# MAGIC Takes the agent output from Phase 2 and:
# MAGIC - Parses the tool call log into structured findings
# MAGIC - Writes the full report to `dq_monitor.gold_dq_reports` Delta table
# MAGIC - Saves a human-readable markdown report to Volumes
# MAGIC - Validates the Gold layer output

# COMMAND ----------
# MAGIC %md ### Step 1: Install Anthropic SDK

# COMMAND ----------

# MAGIC %pip install anthropic
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md ### Step 2: Configuration

# COMMAND ----------

import os
os.environ["ANTHROPIC_API_KEY"] = "<YOUR_API_KEY_HERE>"

REPORT_PATH = "/Volumes/workspace/offset_well_crew/volve_data/dq_report.md"

print("Configuration loaded.")

# COMMAND ----------
# MAGIC %md ### Step 3: Re-define tools and run agent
# MAGIC
# MAGIC Re-runs the full tool-calling agent loop from Phase 2.
# MAGIC Output is captured for structured storage.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json
import pandas as pd
import anthropic

FORBIDDEN_KEYWORDS = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER",
                      "TRUNCATE", "CREATE", "REPLACE", "MERGE", "OVERWRITE"]

def is_safe_query(sql):
    sql_upper = sql.upper()
    for kw in FORBIDDEN_KEYWORDS:
        if kw in sql_upper:
            return False, kw
    return True, None

def list_tables():
    df = spark.table("dq_monitor.monitored_tables")
    rows = df.select("project", "layer", "full_name",
                     "baseline_row_count", "column_count", "status") \
             .orderBy("project", "layer").collect()
    return {"tables": [row.asDict() for row in rows], "total": len(rows)}

def check_row_counts(table_name):
    try:
        baseline_row = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name") == table_name).collect()
        if not baseline_row:
            return {"error": f"Table {table_name} not in registry"}
        baseline_count = baseline_row[0]["baseline_row_count"]
        current_count  = spark.table(table_name).count()
        deviation_pct  = round((current_count - baseline_count) / baseline_count * 100, 2) \
                         if baseline_count > 0 else 0.0
        flag = abs(deviation_pct) > 10
        return {"table_name": table_name, "baseline_count": baseline_count,
                "current_count": current_count, "deviation_pct": deviation_pct,
                "anomaly_flag": flag, "assessment": "ANOMALY" if flag else "NORMAL"}
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

def check_freshness(table_name):
    try:
        history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 5")
        rows    = history.select("version", "timestamp", "operation").collect()
        last_modified = rows[0]["timestamp"]
        hours_since   = (datetime.now() - last_modified.replace(tzinfo=None)).total_seconds() / 3600
        flag = hours_since > 48
        return {"table_name": table_name, "last_modified": str(last_modified),
                "hours_since": round(hours_since, 1),
                "anomaly_flag": flag, "assessment": "STALE" if flag else "FRESH",
                "recent_operations": [{"version": r["version"],
                                       "timestamp": str(r["timestamp"]),
                                       "operation": r["operation"]} for r in rows]}
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

def check_null_rates(table_name):
    try:
        baseline_row = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name") == table_name).collect()
        if not baseline_row:
            return {"error": f"Table {table_name} not in registry"}
        baseline_nulls = json.loads(baseline_row[0]["baseline_null_rates"])
        df = spark.table(table_name)
        row_count = df.count()
        current_nulls = {}
        flagged_columns = []
        for c in df.columns:
            null_count   = df.filter(F.col(c).isNull()).count()
            current_rate = round(null_count / row_count * 100, 2) if row_count > 0 else 0.0
            current_nulls[c] = current_rate
            baseline_rate = baseline_nulls.get(c, 0.0)
            if current_rate - baseline_rate > 5.0:
                flagged_columns.append({"column": c, "baseline_rate": baseline_rate,
                                        "current_rate": current_rate,
                                        "delta": round(current_rate - baseline_rate, 2)})
        return {"table_name": table_name, "row_count": row_count,
                "current_null_rates": current_nulls,
                "flagged_columns": flagged_columns,
                "anomaly_flag": len(flagged_columns) > 0,
                "assessment": "ANOMALY" if flagged_columns else "NORMAL"}
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

def check_schema(table_name):
    try:
        baseline_row = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name") == table_name).collect()
        if not baseline_row:
            return {"error": f"Table {table_name} not in registry"}
        baseline_cols = json.loads(baseline_row[0]["columns"])
        current_cols  = spark.table(table_name).columns
        added   = [c for c in current_cols if c not in baseline_cols]
        removed = [c for c in baseline_cols if c not in current_cols]
        flag    = len(added) > 0 or len(removed) > 0
        return {"table_name": table_name, "baseline_columns": baseline_cols,
                "current_columns": list(current_cols),
                "columns_added": added, "columns_removed": removed,
                "anomaly_flag": flag,
                "assessment": "SCHEMA_DRIFT" if flag else "NORMAL"}
    except Exception as e:
        return {"error": str(e), "table_name": table_name}

def run_custom_sql(query):
    safe, forbidden_kw = is_safe_query(query)
    if not safe:
        return {"error": f"Query rejected — contains forbidden keyword: {forbidden_kw}",
                "query": query, "status": "BLOCKED"}
    try:
        df   = spark.sql(query)
        rows = df.limit(50).collect()
        return {"query": query, "row_count": len(rows),
                "results": [row.asDict() for row in rows], "status": "SUCCESS"}
    except Exception as e:
        return {"error": str(e), "query": query, "status": "ERROR"}

def dispatch_tool(tool_name, tool_input):
    if tool_name == "list_tables":        return list_tables()
    elif tool_name == "check_row_counts": return check_row_counts(tool_input.get("table_name"))
    elif tool_name == "check_freshness":  return check_freshness(tool_input.get("table_name"))
    elif tool_name == "check_null_rates": return check_null_rates(tool_input.get("table_name"))
    elif tool_name == "check_schema":     return check_schema(tool_input.get("table_name"))
    elif tool_name == "run_custom_sql":   return run_custom_sql(tool_input.get("query"))
    else: return {"error": f"Unknown tool: {tool_name}"}

TOOL_SCHEMAS = [
    {"name": "list_tables",
     "description": "List all monitored Delta tables with baseline info. Call this first.",
     "input_schema": {"type": "object", "properties": {}, "required": []}},
    {"name": "check_row_counts",
     "description": "Check current row count vs baseline. Flags >10% deviation.",
     "input_schema": {"type": "object",
                      "properties": {"table_name": {"type": "string"}},
                      "required": ["table_name"]}},
    {"name": "check_freshness",
     "description": "Check last modified timestamp. Flags tables stale >48 hours.",
     "input_schema": {"type": "object",
                      "properties": {"table_name": {"type": "string"}},
                      "required": ["table_name"]}},
    {"name": "check_null_rates",
     "description": "Check null rates per column vs baseline. Flags >5pp increase.",
     "input_schema": {"type": "object",
                      "properties": {"table_name": {"type": "string"}},
                      "required": ["table_name"]}},
    {"name": "check_schema",
     "description": "Check for schema drift — columns added or removed.",
     "input_schema": {"type": "object",
                      "properties": {"table_name": {"type": "string"}},
                      "required": ["table_name"]}},
    {"name": "run_custom_sql",
     "description": "Execute read-only SQL for deeper investigation. SELECT only.",
     "input_schema": {"type": "object",
                      "properties": {"query": {"type": "string"}},
                      "required": ["query"]}}
]

client = anthropic.Anthropic()

def run_dq_agent(goal=None):
    if goal is None:
        goal = "Assess data quality across all monitored Delta tables. " \
               "Investigate thoroughly — check row counts, freshness, null rates, " \
               "and schema for tables across both projects. Use run_custom_sql " \
               "to dig deeper into anything unusual. " \
               "When you have a complete picture, summarize your findings."

    system_prompt = """You are an autonomous data quality monitoring agent for a 
Databricks Lakehouse. You monitor Delta tables across ecommerce and offset_well_crew projects.
Be systematic — list all tables first, then investigate each one.
Use run_custom_sql when standard checks reveal something unusual.
When satisfied you have a complete picture, stop calling tools and summarize findings."""

    messages = [{"role": "user", "content": goal}]
    tool_call_log = []
    iteration = 0
    max_iterations = 30

    print("Agent running...")

    while iteration < max_iterations:
        iteration += 1
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            system=system_prompt,
            tools=TOOL_SCHEMAS,
            messages=messages
        )

        if response.stop_reason == "end_turn":
            final_text = "".join(
                block.text for block in response.content if hasattr(block, "text")
            )
            print(f"Agent complete — {iteration} iterations, {len(tool_call_log)} tool calls")
            return {"goal": goal, "tool_call_log": tool_call_log,
                    "final_report": final_text, "iterations": iteration,
                    "total_tool_calls": len(tool_call_log)}

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    print(f"  [{iteration:02d}] {block.name}({json.dumps(block.input)[:50]})")
                    result = dispatch_tool(block.name, block.input)
                    tool_call_log.append({
                        "iteration": iteration, "tool_name": block.name,
                        "tool_input": block.input, "result_summary": str(result)[:200]
                    })
                    tool_results.append({
                        "type": "tool_result", "tool_use_id": block.id,
                        "content": json.dumps(result, default=str)
                    })
            messages.append({"role": "user", "content": tool_results})

    return {"error": "max_iterations_reached", "tool_call_log": tool_call_log}

print("Agent and tools ready.")

# COMMAND ----------
# MAGIC %md ### Step 4: Run the agent and capture output

# COMMAND ----------

run_timestamp = datetime.now()
result = run_dq_agent()

print(f"\nRun complete at: {run_timestamp.isoformat()}")
print(f"Tool calls: {result.get('total_tool_calls', 0)}")
print(f"Iterations: {result.get('iterations', 0)}")

# COMMAND ----------
# MAGIC %md ### Step 5: Write Gold Delta table

# COMMAND ----------

# Serialize tool call log
tool_call_log_json = json.dumps(result.get("tool_call_log", []), default=str)

# Build summary counts from tool call log
tool_counts = {}
for call in result.get("tool_call_log", []):
    tool_counts[call["tool_name"]] = tool_counts.get(call["tool_name"], 0) + 1

# Create Gold record
gold_record = {
    "run_id":            run_timestamp.strftime("%Y%m%d_%H%M%S"),
    "run_timestamp":     run_timestamp.isoformat(),
    "goal":              result.get("goal", ""),
    "total_iterations":  result.get("iterations", 0),
    "total_tool_calls":  result.get("total_tool_calls", 0),
    "list_tables_calls": tool_counts.get("list_tables", 0),
    "row_count_calls":   tool_counts.get("check_row_counts", 0),
    "freshness_calls":   tool_counts.get("check_freshness", 0),
    "null_rate_calls":   tool_counts.get("check_null_rates", 0),
    "schema_calls":      tool_counts.get("check_schema", 0),
    "custom_sql_calls":  tool_counts.get("run_custom_sql", 0),
    "final_report":      result.get("final_report", ""),
    "tool_call_log_json": tool_call_log_json,
    "status":            "SUCCESS" if "final_report" in result else "ERROR",
}

df_gold = spark.createDataFrame(pd.DataFrame([gold_record]))
(df_gold.write.format("delta").mode("append")
    .saveAsTable("dq_monitor.gold_dq_reports"))

print(f"Gold record written: run_id = {gold_record['run_id']}")

# COMMAND ----------
# MAGIC %md ### Step 6: Save markdown report to Volume

# COMMAND ----------

# Build markdown report
md_report = f"""# Data Quality Report
**Run ID:** {gold_record['run_id']}
**Timestamp:** {gold_record['run_timestamp']}
**Tables Monitored:** 26 (ecommerce: 17, offset_well_crew: 9)
**Tool Calls:** {gold_record['total_tool_calls']} across {gold_record['total_iterations']} iterations

---

{result.get('final_report', 'No report generated.')}
"""

dbutils.fs.put(REPORT_PATH, md_report, overwrite=True)
print(f"Report saved to: {REPORT_PATH}")

# COMMAND ----------
# MAGIC %md ### Step 7: Validate Gold table

# COMMAND ----------

print("=== Gold DQ Reports Table ===")
df_validate = spark.table("dq_monitor.gold_dq_reports")
df_validate.select(
    "run_id", "run_timestamp", "status",
    "total_tool_calls", "total_iterations", "custom_sql_calls"
).orderBy("run_timestamp", ascending=False).show(truncate=60)

print(f"\nTotal runs on record: {df_validate.count()}")

# Preview report
print("\n=== Report Preview (first 30 lines) ===")
preview_lines = result.get("final_report", "").split("\n")[:30]
print("\n".join(preview_lines))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 3 Complete ✅
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `dq_monitor.monitored_tables` | Registry — 26 tables with baselines |
# MAGIC | `dq_monitor.gold_dq_reports` | Every agent run — timestamped, queryable, auditable |
# MAGIC
# MAGIC | File | Description |
# MAGIC |------|-------------|
# MAGIC | `dq_report.md` | Human-readable DQ report in Volumes |
# MAGIC
# MAGIC **Next:** Phase 4 — Scheduled Run + README + LinkedIn Card
