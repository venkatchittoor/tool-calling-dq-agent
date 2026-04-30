# Databricks notebook source
# MAGIC %md
# MAGIC # Tool-Calling DQ Agent
# MAGIC ## Phase 4: Production Run Notebook
# MAGIC
# MAGIC This is the scheduled entry point for the DQ agent.
# MAGIC Designed to run as a Databricks Job on a daily schedule.
# MAGIC
# MAGIC **Features:**
# MAGIC - Rate limit retry with exponential backoff
# MAGIC - Full tool-calling agent loop
# MAGIC - Gold Delta table persistence
# MAGIC - Markdown report saved to Volume
# MAGIC - Job-friendly exit codes

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

REPORT_PATH    = "/Volumes/workspace/offset_well_crew/volve_data/dq_report.md"
MAX_ITERATIONS = 30
MAX_RETRIES    = 3
RETRY_WAIT_SEC = 65  # just over 60s rate limit window

print("Configuration loaded.")

# COMMAND ----------
# MAGIC %md ### Step 3: Tools + Agent loop with retry

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import json, time, pandas as pd
import anthropic

# ── Safety guard ──────────────────────────────────────────────────────────────
FORBIDDEN = ["DROP","DELETE","INSERT","UPDATE","ALTER",
             "TRUNCATE","CREATE","REPLACE","MERGE","OVERWRITE"]

def is_safe_query(sql):
    for kw in FORBIDDEN:
        if kw in sql.upper(): return False, kw
    return True, None

# ── Tool functions ─────────────────────────────────────────────────────────────
def list_tables():
    rows = spark.table("dq_monitor.monitored_tables") \
        .select("project","layer","full_name","baseline_row_count","column_count","status") \
        .orderBy("project","layer").collect()
    return {"tables": [r.asDict() for r in rows], "total": len(rows)}

def check_row_counts(table_name):
    try:
        b = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name")==table_name).collect()
        if not b: return {"error": f"Not in registry: {table_name}"}
        baseline = b[0]["baseline_row_count"]
        current  = spark.table(table_name).count()
        dev      = round((current-baseline)/baseline*100,2) if baseline>0 else 0.0
        flag     = abs(dev) > 10
        return {"table_name":table_name,"baseline_count":baseline,"current_count":current,
                "deviation_pct":dev,"anomaly_flag":flag,"assessment":"ANOMALY" if flag else "NORMAL"}
    except Exception as e: return {"error":str(e),"table_name":table_name}

def check_freshness(table_name):
    try:
        rows = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 5") \
            .select("version","timestamp","operation").collect()
        last  = rows[0]["timestamp"]
        hours = (datetime.now()-last.replace(tzinfo=None)).total_seconds()/3600
        flag  = hours > 48
        return {"table_name":table_name,"last_modified":str(last),
                "hours_since":round(hours,1),"anomaly_flag":flag,
                "assessment":"STALE" if flag else "FRESH",
                "recent_operations":[{"version":r["version"],"timestamp":str(r["timestamp"]),
                                      "operation":r["operation"]} for r in rows]}
    except Exception as e: return {"error":str(e),"table_name":table_name}

def check_null_rates(table_name):
    try:
        b = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name")==table_name).collect()
        if not b: return {"error": f"Not in registry: {table_name}"}
        baseline_nulls = json.loads(b[0]["baseline_null_rates"])
        df = spark.table(table_name)
        n  = df.count()
        current_nulls, flagged = {}, []
        for c in df.columns:
            rate = round(df.filter(F.col(c).isNull()).count()/n*100,2) if n>0 else 0.0
            current_nulls[c] = rate
            if rate - baseline_nulls.get(c,0.0) > 5.0:
                flagged.append({"column":c,"baseline_rate":baseline_nulls.get(c,0.0),
                                "current_rate":rate,"delta":round(rate-baseline_nulls.get(c,0.0),2)})
        return {"table_name":table_name,"row_count":n,"current_null_rates":current_nulls,
                "flagged_columns":flagged,"anomaly_flag":len(flagged)>0,
                "assessment":"ANOMALY" if flagged else "NORMAL"}
    except Exception as e: return {"error":str(e),"table_name":table_name}

def check_schema(table_name):
    try:
        b = spark.table("dq_monitor.monitored_tables") \
            .filter(F.col("full_name")==table_name).collect()
        if not b: return {"error": f"Not in registry: {table_name}"}
        baseline_cols = json.loads(b[0]["columns"])
        current_cols  = list(spark.table(table_name).columns)
        added   = [c for c in current_cols if c not in baseline_cols]
        removed = [c for c in baseline_cols if c not in current_cols]
        flag    = len(added)>0 or len(removed)>0
        return {"table_name":table_name,"baseline_columns":baseline_cols,
                "current_columns":current_cols,"columns_added":added,"columns_removed":removed,
                "anomaly_flag":flag,"assessment":"SCHEMA_DRIFT" if flag else "NORMAL"}
    except Exception as e: return {"error":str(e),"table_name":table_name}

def run_custom_sql(query):
    safe, kw = is_safe_query(query)
    if not safe: return {"error":f"Blocked — forbidden keyword: {kw}","query":query,"status":"BLOCKED"}
    try:
        rows = spark.sql(query).limit(50).collect()
        return {"query":query,"row_count":len(rows),
                "results":[r.asDict() for r in rows],"status":"SUCCESS"}
    except Exception as e: return {"error":str(e),"query":query,"status":"ERROR"}

def dispatch_tool(name, inp):
    return {"list_tables":        lambda: list_tables(),
            "check_row_counts":   lambda: check_row_counts(inp.get("table_name")),
            "check_freshness":    lambda: check_freshness(inp.get("table_name")),
            "check_null_rates":   lambda: check_null_rates(inp.get("table_name")),
            "check_schema":       lambda: check_schema(inp.get("table_name")),
            "run_custom_sql":     lambda: run_custom_sql(inp.get("query")),
            }.get(name, lambda: {"error":f"Unknown tool: {name}"})()

TOOL_SCHEMAS = [
    {"name":"list_tables","description":"List all monitored tables. Call this first.",
     "input_schema":{"type":"object","properties":{},"required":[]}},
    {"name":"check_row_counts","description":"Check row count vs baseline. Flags >10% deviation.",
     "input_schema":{"type":"object","properties":{"table_name":{"type":"string"}},"required":["table_name"]}},
    {"name":"check_freshness","description":"Check last modified. Flags tables stale >48 hours.",
     "input_schema":{"type":"object","properties":{"table_name":{"type":"string"}},"required":["table_name"]}},
    {"name":"check_null_rates","description":"Check null rates vs baseline. Flags >5pp increase.",
     "input_schema":{"type":"object","properties":{"table_name":{"type":"string"}},"required":["table_name"]}},
    {"name":"check_schema","description":"Check for schema drift — columns added or removed.",
     "input_schema":{"type":"object","properties":{"table_name":{"type":"string"}},"required":["table_name"]}},
    {"name":"run_custom_sql","description":"Run read-only SQL for deeper investigation. SELECT only.",
     "input_schema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}}
]

# ── Agent loop with retry ──────────────────────────────────────────────────────
def run_dq_agent():
    goal = ("Assess data quality across all monitored Delta tables. "
            "Check row counts, freshness, null rates, and schema for all tables. "
            "Use run_custom_sql to investigate anomalies. "
            "Summarize findings when complete.")

    system = ("You are an autonomous data quality agent for a Databricks Lakehouse. "
              "Monitor ecommerce and offset_well_crew Delta tables. "
              "Start by listing all tables, then investigate systematically. "
              "Stop when you have a complete picture.")

    client   = anthropic.Anthropic()
    messages = [{"role":"user","content":goal}]
    tool_log = []
    iteration = 0

    print(f"Agent starting — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    while iteration < MAX_ITERATIONS:
        iteration += 1

        # ── API call with retry ──
        for attempt in range(MAX_RETRIES):
            try:
                response = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=4000,
                    system=system,
                    tools=TOOL_SCHEMAS,
                    messages=messages
                )
                break  # success
            except anthropic.RateLimitError:
                if attempt < MAX_RETRIES - 1:
                    print(f"  Rate limit hit — waiting {RETRY_WAIT_SEC}s (attempt {attempt+1}/{MAX_RETRIES})")
                    time.sleep(RETRY_WAIT_SEC)
                else:
                    raise

        if response.stop_reason == "end_turn":
            final = "".join(b.text for b in response.content if hasattr(b,"text"))
            print(f"Agent complete — {iteration} iterations, {len(tool_log)} tool calls")
            return {"goal":goal,"tool_call_log":tool_log,"final_report":final,
                    "iterations":iteration,"total_tool_calls":len(tool_log),"status":"SUCCESS"}

        if response.stop_reason == "tool_use":
            messages.append({"role":"assistant","content":response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    print(f"  [{iteration:02d}] {block.name}({json.dumps(block.input)[:50]})")
                    result = dispatch_tool(block.name, block.input)
                    tool_log.append({"iteration":iteration,"tool_name":block.name,
                                     "tool_input":block.input,"result_summary":str(result)[:200]})
                    tool_results.append({"type":"tool_result","tool_use_id":block.id,
                                         "content":json.dumps(result,default=str)})
            messages.append({"role":"user","content":tool_results})

    return {"status":"MAX_ITERATIONS_REACHED","tool_call_log":tool_log}

print("Agent ready with rate limit retry.")

# COMMAND ----------
# MAGIC %md ### Step 4: Run agent + persist to Gold

# COMMAND ----------

run_timestamp = datetime.now()
result = run_dq_agent()

# ── Write Gold record ──
tool_counts = {}
for call in result.get("tool_call_log",[]):
    tool_counts[call["tool_name"]] = tool_counts.get(call["tool_name"],0) + 1

gold_record = {
    "run_id":             run_timestamp.strftime("%Y%m%d_%H%M%S"),
    "run_timestamp":      run_timestamp.isoformat(),
    "goal":               result.get("goal",""),
    "total_iterations":   result.get("iterations",0),
    "total_tool_calls":   result.get("total_tool_calls",0),
    "list_tables_calls":  tool_counts.get("list_tables",0),
    "row_count_calls":    tool_counts.get("check_row_counts",0),
    "freshness_calls":    tool_counts.get("check_freshness",0),
    "null_rate_calls":    tool_counts.get("check_null_rates",0),
    "schema_calls":       tool_counts.get("check_schema",0),
    "custom_sql_calls":   tool_counts.get("run_custom_sql",0),
    "final_report":       result.get("final_report",""),
    "tool_call_log_json": json.dumps(result.get("tool_call_log",[]),default=str),
    "status":             result.get("status","ERROR"),
}

df_gold = spark.createDataFrame(pd.DataFrame([gold_record]))
(df_gold.write.format("delta").mode("append")
    .saveAsTable("dq_monitor.gold_dq_reports"))

print(f"Gold record written: {gold_record['run_id']}")

# ── Save markdown report ──
md = f"""# Data Quality Report
**Run ID:** {gold_record['run_id']}
**Timestamp:** {gold_record['run_timestamp']}
**Tables Monitored:** 26 | **Tool Calls:** {gold_record['total_tool_calls']} | **Iterations:** {gold_record['total_iterations']}

---

{result.get('final_report','')}
"""
dbutils.fs.put(REPORT_PATH, md, overwrite=True)
print(f"Report saved: {REPORT_PATH}")

# ── Validate ──
print("\n=== Gold DQ Reports ===")
spark.table("dq_monitor.gold_dq_reports") \
    .select("run_id","run_timestamp","status","total_tool_calls","total_iterations") \
    .orderBy("run_timestamp", ascending=False).show(truncate=60)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Phase 4 Complete ✅
# MAGIC
# MAGIC **To schedule this notebook as a daily Databricks Job:**
# MAGIC 1. Go to Jobs & Pipelines → Create Job
# MAGIC 2. Name: `tool-calling-dq-agent-daily`
# MAGIC 3. Task: select this notebook
# MAGIC 4. Cluster: Serverless
# MAGIC 5. Schedule: Daily at your preferred time
# MAGIC 6. Click Save
# MAGIC
# MAGIC **Next:** README + LinkedIn Card
