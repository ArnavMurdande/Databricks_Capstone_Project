# Databricks notebook source
# DBTITLE 1,Gold configuration
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid

# Update these values ONLY if your Day 1 00_config uses different variable names.
# Prefer the existing values from 00_config.
CATALOG_NAME = globals().get("CATALOG", "capstone_8hospital")
BRONZE_SCHEMA_NAME = globals().get("BRONZE_SCHEMA", "hospital_schema")
SILVER_SCHEMA_NAME = globals().get("SILVER_SCHEMA", "silver_schema")
GOLD_SCHEMA_NAME = globals().get("GOLD_SCHEMA", "gold")

assert not CATALOG_NAME.startswith("REPLACE_"), "Set CATALOG_NAME to your actual Unity Catalog catalog."

def tbl(schema, name):
    return f"{CATALOG_NAME}.{schema}.{name}"

SRC_ENCOUNTERS = tbl(SILVER_SCHEMA_NAME, "silver_encounter_events")
SRC_STATUS = tbl(SILVER_SCHEMA_NAME, "silver_status_history")
SRC_CURRENT = tbl(SILVER_SCHEMA_NAME, "silver_encounter_current")
SRC_SECONDARY = tbl(SILVER_SCHEMA_NAME, "silver_secondary_events")
SRC_DEPARTMENT = tbl(SILVER_SCHEMA_NAME, "silver_department")
SRC_PATIENT = tbl(SILVER_SCHEMA_NAME, "silver_patient")

GOLD_TABLES = {
    "fact_encounter_activity": tbl(GOLD_SCHEMA_NAME, "fact_encounter_activity"),
    "fact_status_transitions": tbl(GOLD_SCHEMA_NAME, "fact_status_transitions"),
    "dim_department": tbl(GOLD_SCHEMA_NAME, "dim_department"),
    "dim_patient": tbl(GOLD_SCHEMA_NAME, "dim_patient"),
    "mart_encounter_daily": tbl(GOLD_SCHEMA_NAME, "mart_encounter_daily"),
    "gold_run_log": tbl(GOLD_SCHEMA_NAME, "gold_run_log"),
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{GOLD_SCHEMA_NAME}")

RUN_ID = str(uuid.uuid4())
RUN_TS = datetime.now(timezone.utc).replace(tzinfo=None)
print("Gold run:", RUN_ID, RUN_TS)

# COMMAND ----------

# Some teams may use different approved Silver reference/secondary table names.
# If a table name below differs, update only that source variable after checking Catalog Explorer.
sources = {
    "encounters": SRC_ENCOUNTERS,
    "status_history": SRC_STATUS,
    "current_status": SRC_CURRENT,
    "secondary_events": SRC_SECONDARY,
    "department_reference": SRC_DEPARTMENT,
    "patient_reference": SRC_PATIENT,
}

for label, name in sources.items():
    exists = spark.catalog.tableExists(name)
    print(label, name, "EXISTS" if exists else "NOT FOUND")
    if exists:
        print("Rows:", spark.table(name).count())
        spark.table(name).printSchema()

# COMMAND ----------

# Load the mandatory curated sources.
assert spark.catalog.tableExists(SRC_ENCOUNTERS), f"Missing {SRC_ENCOUNTERS}"
assert spark.catalog.tableExists(SRC_STATUS), f"Missing {SRC_STATUS}"
assert spark.catalog.tableExists(SRC_CURRENT), f"Missing {SRC_CURRENT}"

enc = spark.table(SRC_ENCOUNTERS)
status = spark.table(SRC_STATUS)
current_status = spark.table(SRC_CURRENT)

display(enc.limit(5))
display(status.limit(5))
display(current_status.limit(5))

# COMMAND ----------

# DBTITLE 1,Validate required columns
def require_columns(df, required, label):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: missing required columns {missing}. Inspect printSchema and map the actual names.")

require_columns(
    enc,
    ["encounter_id", "event_timestamp", "department_id", "patient_id",
     "visit_type", "encounter_status", "bill_amount", "wait_minutes"],
    "silver_encounter_events"
)
if "change_timestamp" not in status.columns and "effective_timestamp" in status.columns:
    status = status.withColumnRenamed("effective_timestamp", "change_timestamp")

require_columns(
    status,
    ["encounter_id", "change_timestamp", "previous_status", "new_status", "source_sequence"],
    "silver_status_history"
)
require_columns(current_status, ["encounter_id"], "silver_encounter_current")

print("Fact grain: one curated encounter activity row per encounter_id, based on deduplicated Silver encounter events.")
print("Status fact grain: one row per encounter_id + source_sequence.")

# COMMAND ----------

# DBTITLE 1,Build department and patient dimensions
# Prefer the Day 2 Silver reference tables when available.
# If the reference extracts were staged with different names, map those names here.
SRC_DEPARTMENT = tbl(BRONZE_SCHEMA_NAME, "department_bronze")
SRC_PATIENT = tbl(BRONZE_SCHEMA_NAME, "patient_bronze")
if spark.catalog.tableExists(SRC_DEPARTMENT):
    dept_src = spark.table(SRC_DEPARTMENT)
else:
    raise ValueError("Silver department reference table not found. Confirm approved Day 2 output name.")

if spark.catalog.tableExists(SRC_PATIENT):
    patient_src = spark.table(SRC_PATIENT)
else:
    raise ValueError("Silver patient reference table not found. Confirm approved Day 2 output name.")

require_columns(dept_src, ["department_id"], "silver_department")
require_columns(patient_src, ["patient_id"], "silver_patient")

dim_department = (
    dept_src
    .select(
        F.col("department_id").cast("string"),
        *[F.col(c) for c in ["display_name", "region", "active_flag", "effective_date", "risk_tier"] if c in dept_src.columns]
    )
    .dropDuplicates(["department_id"])
    .withColumn("department_sk", F.xxhash64("department_id"))
)

# Pseudonymous patient surrogate: do not expose patient_id/display_name in Gold.
# This is not anonymisation. Publish only after the approved masking/access policy is confirmed.
dim_patient = (
    patient_src
    .select(
        F.col("patient_id").cast("string"),
        *[F.col(c) for c in ["region", "active_flag", "effective_date", "risk_tier"] if c in patient_src.columns]
    )
    .dropDuplicates(["patient_id"])
    .withColumn("patient_sk", F.sha2(F.col("patient_id"), 256))
    .drop("patient_id")
)

display(dim_department.limit(10))
display(dim_patient.limit(10))

# COMMAND ----------

# DBTITLE 1,Build encounter activity fact
# Create dimension lookup joins.
dept_lookup = dim_department.select("department_id", "department_sk")
patient_lookup = dim_patient

enc_for_gold = enc
if "doctor_id" in enc_for_gold.columns:
    enc_for_gold = enc_for_gold.drop("doctor_id")

fact_encounter_activity = (
    enc_for_gold
    .join(dept_lookup, "department_id", "left")
    .join(
        patient_lookup,
        F.sha2(F.col("patient_id").cast("string"), 256) == F.col("patient_sk"),
        "left"
    )
)

# Add latest status from the Day 2 current-state table, preserving source status labels.
current_status = (
    current_status
    .withColumnRenamed("current_status", "new_status")
    .withColumnRenamed("status_updated_at", "change_timestamp")
)
current_cols = [c for c in ["encounter_id", "new_status", "change_timestamp", "source_sequence"] if c in current_status.columns]
current_for_join = current_status.select(*current_cols).dropDuplicates(["encounter_id"])

fact_encounter_activity = (
    fact_encounter_activity
    .join(current_for_join, "encounter_id", "left")
    .select(
        "encounter_id",
        F.col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        F.to_date("event_timestamp").alias("event_date"),
        "department_id",
        "department_sk",
        "patient_sk",
        "visit_type",
        F.col("encounter_status").alias("source_encounter_status"),
        F.col("new_status").alias("latest_status_update"),
        F.col("bill_amount").cast("decimal(12,2)").alias("bill_amount"),
        F.col("wait_minutes").cast("int").alias("wait_minutes"),
        *([F.col("ingest_timestamp").cast("timestamp").alias("ingest_timestamp")] if "ingest_timestamp" in fact_encounter_activity.columns else [])
    )
)

# Enforce declared business grain.
dupe_keys = fact_encounter_activity.groupBy("encounter_id").count().filter("count > 1")
assert dupe_keys.limit(1).count() == 0, "fact_encounter_activity is not unique by encounter_id."

display(fact_encounter_activity.limit(10))
print("Fact rows:", fact_encounter_activity.count())

# COMMAND ----------

fact_status_transitions = (
    status
    .select(
        "encounter_id",
        F.col("source_sequence").cast("long").alias("source_sequence"),
        F.col("change_timestamp").cast("timestamp").alias("change_timestamp"),
        "previous_status",
        "new_status",
        *([F.col("change_reason")] if "change_reason" in status.columns else [])
    )
    .dropDuplicates(["encounter_id", "source_sequence"])
)

transition_dupes = (
    fact_status_transitions.groupBy("encounter_id", "source_sequence")
    .count().filter("count > 1")
)
assert transition_dupes.limit(1).count() == 0, "Duplicate status transition grain found."

display(fact_status_transitions.orderBy("encounter_id", "source_sequence").limit(20))

# COMMAND ----------

# Supported waiting-time measures.
mart_waiting = (
    fact_encounter_activity
    .groupBy("event_date", "department_id")
    .agg(
        F.countDistinct("encounter_id").alias("encounter_count"),
        F.round(F.avg("wait_minutes"), 2).alias("average_wait_minutes"),
        F.expr("percentile_approx(wait_minutes, 0.5)").alias("median_wait_minutes"),
        F.max("wait_minutes").alias("maximum_wait_minutes"),
        F.sum("bill_amount").cast("decimal(18,2)").alias("total_bill_amount"),
        F.round(F.avg("bill_amount"), 2).alias("average_bill_amount")
    )
)

display(mart_waiting.orderBy("event_date", "department_id"))

# COMMAND ----------

# Explicit blockers for the two KPIs that require approved business inputs.
kpi_readiness = spark.createDataFrame([
    ("waiting_time", "READY", "Calculated from wait_minutes in Silver encounter events."),
    ("length_of_stay", "BLOCKED", "Requires approved admission/discharge status mapping and valid stay interval rules."),
    ("bed_utilization", "BLOCKED", "Requires approved bed-capacity source and occupancy definition.")
], ["kpi_name", "readiness", "reason"])

display(kpi_readiness)

# COMMAND ----------

# Required table name from Client Project Specification.
# Daily mart contains supported metrics only until LOS/bed-capacity dependencies are approved.
mart_encounter_daily = (
    fact_encounter_activity
    .groupBy("event_date")
    .agg(
        F.countDistinct("encounter_id").alias("encounter_count"),
        F.round(F.avg("wait_minutes"), 2).alias("average_wait_minutes"),
        F.expr("percentile_approx(wait_minutes, 0.5)").alias("median_wait_minutes"),
        F.max("wait_minutes").alias("maximum_wait_minutes"),
        F.sum("bill_amount").cast("decimal(18,2)").alias("total_bill_amount"),
        F.round(F.avg("bill_amount"), 2).alias("average_bill_amount")
    )
    .withColumn("length_of_stay_status", F.lit("BLOCKED_PENDING_STATUS_MAPPING"))
    .withColumn("bed_utilization_status", F.lit("BLOCKED_PENDING_CAPACITY_SOURCE"))
)

display(mart_encounter_daily.orderBy("event_date"))

# COMMAND ----------

# Set True only for a controlled first build or an approved full rebuild.
ALLOW_FULL_REFRESH = False

gold_dfs = {
    "fact_encounter_activity": fact_encounter_activity,
    "fact_status_transitions": fact_status_transitions,
    "dim_department": dim_department,
    "dim_patient": dim_patient,
    "mart_encounter_daily": mart_encounter_daily,
}

existing = [name for name in GOLD_TABLES.values() if spark.catalog.tableExists(name)]
if existing and not ALLOW_FULL_REFRESH:
    raise ValueError(
        "Gold target(s) already exist. Do not overwrite blindly. Implement MERGE/incremental logic "
        "or set ALLOW_FULL_REFRESH=True only with approval: " + ", ".join(existing)
    )

for key, df in gold_dfs.items():
    target = GOLD_TABLES[key]
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(target))
    print("Created", target, "rows =", spark.table(target).count())

# COMMAND ----------

# Source-to-Gold reconciliation.
checks = []

def record_check(test_name, passed, detail):
    checks.append((test_name, "PASS" if passed else "FAIL", str(detail)))
    print(("PASS" if passed else "FAIL"), test_name, detail)

gold_fact = spark.table(GOLD_TABLES["fact_encounter_activity"])
gold_status = spark.table(GOLD_TABLES["fact_status_transitions"])
gold_mart = spark.table(GOLD_TABLES["mart_encounter_daily"])

record_check(
    "Fact encounter business key unique",
    gold_fact.count() == gold_fact.select("encounter_id").distinct().count(),
    f"rows={gold_fact.count()}"
)
record_check(
    "Status transition grain unique",
    gold_status.count() == gold_status.select("encounter_id", "source_sequence").distinct().count(),
    f"rows={gold_status.count()}"
)
record_check(
    "Daily mart encounter count reconciles",
    (gold_mart.agg(F.sum("encounter_count")).first()[0] or 0) == gold_fact.count(),
    f"fact={gold_fact.count()}, mart_sum={gold_mart.agg(F.sum('encounter_count')).first()[0]}"
)
record_check(
    "No direct patient_id or doctor_id in Gold encounter fact",
    not {"patient_id", "doctor_id", "display_name"}.intersection(gold_fact.columns),
    f"columns={gold_fact.columns}"
)

check_df = spark.createDataFrame(checks, "test_name string, result string, detail string")
display(check_df)
assert check_df.filter(F.col("result") == "FAIL").count() == 0, "Gold reconciliation failed."

# COMMAND ----------

# Replay check helper: capture counts before and after rerunning the SAME source batch.
# Execute this after your MERGE implementation and replaying the same input.
def table_count_snapshot():
    return {
        key: spark.table(name).count()
        for key, name in GOLD_TABLES.items()
        if key != "gold_run_log" and spark.catalog.tableExists(name)
    }

print("Current Gold count snapshot:")
print(table_count_snapshot())

# COMMAND ----------

# Delta history: use after the table has been written.
display(spark.sql(f"DESCRIBE HISTORY {GOLD_TABLES['fact_encounter_activity']}"))

# Explain a representative KPI query.
mart_waiting.explain("formatted")

# COMMAND ----------

print("CAPSTONE 08 – GOLD LAYER")
for key, name in GOLD_TABLES.items():
    if key != "gold_run_log" and spark.catalog.tableExists(name):
        print(key, "|", name, "| rows:", spark.table(name).count())

display(kpi_readiness)
print("Before sign-off: attach expected-vs-actual reconciliation, replay test evidence, Delta history, query plan, and approval for LOS/bed-utilization dependencies.")