# Databricks notebook source
# MAGIC %md
# MAGIC # Tool-Calling DQ Agent
# MAGIC ## Phase 2: Tool-Calling Agent Loop
# MAGIC
# MAGIC This is where the architectural distinction becomes real.
# MAGIC
# MAGIC Claude receives a goal and a toolkit — and decides:
# MAGIC - Which tools to call
# MAGIC - In what order
# MAGIC - With what arguments
# MAGIC - When to stop
# MAGIC
# MAGIC Your code does not predetermine the sequence.
# MAGIC Claude drives the investigation loop.
# MAGIC
# MAGIC **Goal:** "Assess data quality across all monitored Delta tables"
# MAGIC **Tools:** list_tables, check_row_counts, check_freshness,
# MAGIC            check_null_rates, check_schema, run_custom_sql

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

print("Configuration loaded.")

# COMMAND ----------
# MAGIC %md ### Step 3: Re-define tools and dispatcher
# MAGIC
# MAGIC Same tool functions from Phase 1.
# MAGIC Must be re-defined after restartPython().

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json
import pandas as pd

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

print("Tools and dispatcher ready.")

# COMMAND ----------
# MAGIC %md ### Step 4: Define tool schemas for Claude
# MAGIC
# MAGIC These tell Claude what tools exist, what they do,
# MAGIC and what arguments they accept.
# MAGIC Claude uses these to decide which tool to call.

# COMMAND ----------

TOOL_SCHEMAS = [
    {
        "name": "list_tables",
        "description": "List all monitored Delta tables with their baseline row counts and status. Always call this first to understand the scope of monitoring.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "check_row_counts",
        "description": "Check the current row count of a table against its baseline. Flags anomalies where count deviates more than 10% from baseline.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Full table name e.g. ecommerce.silver_order_items"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "check_freshness",
        "description": "Check when a table was last modified. Flags tables not updated in more than 48 hours as STALE.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Full table name e.g. ecommerce.bronze_orders"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "check_null_rates",
        "description": "Check null rates per column vs baseline. Flags columns where null rate increased by more than 5 percentage points.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Full table name e.g. offset_well_crew.silver_log_qc_flags"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "check_schema",
        "description": "Check if a table's schema has drifted from baseline — columns added or removed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {
                    "type": "string",
                    "description": "Full table name e.g. ecommerce.silver_order_items"
                }
            },
            "required": ["table_name"]
        }
    },
    {
        "name": "run_custom_sql",
        "description": "Execute a read-only SQL query for deeper investigation. Use this when standard checks reveal something unusual that warrants further exploration. Only SELECT statements are permitted.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A read-only SQL SELECT query to run against Databricks tables"
                }
            },
            "required": ["query"]
        }
    }
]

print(f"Tool schemas defined: {len(TOOL_SCHEMAS)} tools")
for t in TOOL_SCHEMAS:
    print(f"  ✓ {t['name']}")

# COMMAND ----------
# MAGIC %md ### Step 5: Tool-Calling Agent Loop
# MAGIC
# MAGIC This is the core of the architecture.
# MAGIC Claude drives — your code just executes what Claude asks for.

# COMMAND ----------

import anthropic

client = anthropic.Anthropic()

def run_dq_agent(goal=None):
    """
    Tool-calling agent loop.
    Claude receives a goal + tools and drives the investigation autonomously.
    Loop continues until Claude stops calling tools (stop_reason = 'end_turn').
    """
    if goal is None:
        goal = "Assess data quality across all monitored Delta tables. " \
               "Investigate thoroughly — check row counts, freshness, null rates, " \
               "and schema for tables across both projects. Use run_custom_sql " \
               "to dig deeper into anything that looks unusual. " \
               "When you have a complete picture, summarize your findings."

    system_prompt = """You are an autonomous data quality monitoring agent for a 
Databricks Lakehouse environment. You have access to tools that let you inspect 
Delta tables across two projects: ecommerce and offset_well_crew.

Your job is to thoroughly investigate data quality across all monitored tables.
Be systematic — start by listing all tables, then investigate each one.
Use run_custom_sql when you need deeper investigation beyond standard checks.
You decide what to check, in what order, based on what you find.
When you are satisfied you have a complete picture, stop calling tools 
and provide a comprehensive summary of your findings."""

    messages = [{"role": "user", "content": goal}]

    tool_call_log = []
    iteration = 0
    max_iterations = 30  # safety ceiling

    print(f"Goal: {goal[:80]}...")
    print(f"{'='*60}")
    print("Agent starting investigation...\n")

    while iteration < max_iterations:
        iteration += 1

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            system=system_prompt,
            tools=TOOL_SCHEMAS,
            messages=messages
        )

        # Check stop reason
        if response.stop_reason == "end_turn":
            # Claude finished — extract final text response
            final_text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    final_text += block.text
            print(f"\n{'='*60}")
            print(f"Agent completed after {iteration} iterations, "
                  f"{len(tool_call_log)} tool calls")
            print(f"{'='*60}\n")
            return {
                "goal":           goal,
                "tool_call_log":  tool_call_log,
                "final_report":   final_text,
                "iterations":     iteration,
                "total_tool_calls": len(tool_call_log)
            }

        # Claude wants to use tools
        if response.stop_reason == "tool_use":
            # Add Claude's response to message history
            messages.append({"role": "assistant", "content": response.content})

            # Process all tool calls in this response
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_name  = block.name
                    tool_input = block.input
                    tool_id    = block.id

                    print(f"[{iteration:02d}] Claude calls: {tool_name}("
                          f"{json.dumps(tool_input)[:60]})")

                    # Execute the tool
                    result = dispatch_tool(tool_name, tool_input)

                    # Log the call
                    tool_call_log.append({
                        "iteration":  iteration,
                        "tool_name":  tool_name,
                        "tool_input": tool_input,
                        "result_summary": str(result)[:200]
                    })

                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": tool_id,
                        "content":     json.dumps(result, default=str)
                    })

            # Feed tool results back to Claude
            messages.append({"role": "user", "content": tool_results})

        else:
            print(f"Unexpected stop reason: {response.stop_reason}")
            break

    print(f"Max iterations ({max_iterations}) reached.")
    return {"error": "max_iterations_reached", "tool_call_log": tool_call_log}

print("DQ Agent loop defined.")

# COMMAND ----------
# MAGIC %md ### Step 6: Run the agent

# COMMAND ----------

result = run_dq_agent()

# Print tool call summary
print("=== Tool Call Log ===")
for call in result.get("tool_call_log", []):
    print(f"  [{call['iteration']:02d}] {call['tool_name']:25} | "
          f"{json.dumps(call['tool_input'])[:50]}")

print(f"\nTotal tool calls: {result.get('total_tool_calls', 0)}")
print(f"Total iterations: {result.get('iterations', 0)}")

# COMMAND ----------
# MAGIC %md ### Step 7: Print final report

# COMMAND ----------

print(result.get("final_report", "No report generated."))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 2 Complete ✅
# MAGIC
# MAGIC **What just happened — the key distinction:**
# MAGIC
# MAGIC Your code defined the tools and started the loop.
# MAGIC Claude decided:
# MAGIC - Which tables to check
# MAGIC - Which tools to call on each table
# MAGIC - When to use run_custom_sql for deeper investigation
# MAGIC - When it had enough information to stop
# MAGIC
# MAGIC This is tool-calling — Claude drives, your code executes.
# MAGIC
# MAGIC **Next:** Phase 3 — Report Synthesis + Gold Delta Table
