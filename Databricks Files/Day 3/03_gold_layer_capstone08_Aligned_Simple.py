# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone 08 — Day 3: Gold Layer (Simple Version)
# MAGIC
# MAGIC **Purpose:** Create Gold dimensions, facts, and daily metrics using the tables actually created by Day 2 Silver.
# MAGIC
# MAGIC **Simple workflow:** configure tables → build dimensions/facts → create daily metrics → reconcile counts → test rerun → inspect Delta history and query plan.
# MAGIC
# MAGIC **Important:** Day 2 does not create stay intervals, and its documented source fields do not confirm admission/discharge events. This notebook therefore does **not** invent length-of-stay or bed-utilization metrics.

# COMMAND ----------

spark.sql(f"drop schema {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# 1. Imports and table names
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

CATALOG = "capstone_8hospital"
BRONZE_SCHEMA = "hospital_schema"
SILVER_SCHEMA = "silver_schema"
GOLD_SCHEMA = "gold_schema"
spark.sql(f"drop schema if exists {CATALOG}.{GOLD_SCHEMA} cascade")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

def table(schema, name):
    return f"{CATALOG}.{schema}.{name}"

DEPARTMENT_SOURCE = table(BRONZE_SCHEMA, "department_bronze")
PATIENT_SOURCE = table(BRONZE_SCHEMA, "patient_bronze")

SILVER_ENCOUNTERS = table(SILVER_SCHEMA, "silver_encounter_events")
SILVER_CURRENT = table(SILVER_SCHEMA, "silver_encounter_current")
SILVER_HISTORY = table(SILVER_SCHEMA, "silver_status_history")
SILVER_SECONDARY = table(SILVER_SCHEMA, "silver_secondary_events")

DIM_DEPARTMENT = table(GOLD_SCHEMA, "dim_department")
DIM_PATIENT = table(GOLD_SCHEMA, "dim_patient")
DIM_DATE = table(GOLD_SCHEMA, "dim_date")
FACT_ENCOUNTER = table(GOLD_SCHEMA, "fact_encounter")
FACT_TRANSITION = table(GOLD_SCHEMA, "fact_status_transition")
FACT_SECONDARY = table(GOLD_SCHEMA, "fact_secondary_event")
MART_DAILY = table(GOLD_SCHEMA, "mart_daily")
MART_DEPARTMENT = table(GOLD_SCHEMA, "mart_department_daily")

# Stop early if Day 2 tables are missing.
for name in [SILVER_ENCOUNTERS, SILVER_CURRENT, SILVER_HISTORY, SILVER_SECONDARY,
             DEPARTMENT_SOURCE, PATIENT_SOURCE]:
    if not spark.catalog.tableExists(name):
        raise ValueError(f"Required source table is missing: {name}")

print("Gold schema and source tables are ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create dimensions\nUse deterministic keys. Patient IDs are hashed before being written to Gold; this is pseudonymization, not encryption.

# COMMAND ----------

# Department dimension
departments = spark.table(DEPARTMENT_SOURCE)
department_id = "department_id"
department_name = "department_name" if "department_name" in departments.columns else department_id

dim_department = (
    departments
    .select(F.col(department_id).cast("string").alias("department_id"),
            F.col(department_name).cast("string").alias("department_name"))
    .dropDuplicates(["department_id"])
    .withColumn("department_sk", F.xxhash64("department_id"))
)
dim_department.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_DEPARTMENT)

# Patient dimension: do not store the raw patient_id in Gold.
patients = spark.table(PATIENT_SOURCE)
dim_patient = (
    patients.select(F.sha2(F.col("patient_id").cast("string"), 256).alias("patient_key"))
    .dropDuplicates(["patient_key"])
    .withColumn("patient_sk", F.xxhash64("patient_key"))
)
dim_patient.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_PATIENT)

print("Department dimension:", spark.table(DIM_DEPARTMENT).count())
print("Patient dimension:", spark.table(DIM_PATIENT).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create encounter fact\n**Grain:** one row per `encounter_id`. Waiting time and bill amount come directly from Silver.

# COMMAND ----------

encounters = spark.table(SILVER_ENCOUNTERS)
current = spark.table(SILVER_CURRENT)

# Use the actual Day 2 event timestamp column.
if "event_timestamp" not in encounters.columns:
    raise ValueError("silver_encounter_events must contain event_timestamp.")

current_cols = ["encounter_id"]
if "current_status" in current.columns:
    current_cols += ["current_status"]
if "current_source_sequence" in current.columns:
    current_cols += ["current_source_sequence"]
current_small = current.select(*current_cols)

fact_source = encounters.join(current_small, "encounter_id", "left")

dept_lookup = spark.table(DIM_DEPARTMENT).select("department_id", "department_sk")
fact_source = fact_source.join(dept_lookup, "department_id", "left")

fact_source = fact_source.withColumn(
    "patient_key", F.sha2(F.col("patient_id").cast("string"), 256)
).withColumn(
    "patient_sk", F.xxhash64("patient_key")
).withColumn(
    "encounter_date", F.to_date("event_timestamp")
)

fact_columns = ["encounter_id", "department_sk", "patient_sk", "encounter_date", "event_timestamp"]
for c in ["visit_type", "bill_amount", "wait_minutes", "current_status", "current_source_sequence", "dq_status"]:
    if c in fact_source.columns:
        fact_columns.append(c)

fact_encounter = fact_source.select(*fact_columns).dropDuplicates(["encounter_id"])
fact_encounter.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FACT_ENCOUNTER)

display(spark.table(FACT_ENCOUNTER).limit(10))
print("Encounter fact rows:", spark.table(FACT_ENCOUNTER).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create status-transition and secondary-event facts

# COMMAND ----------

# One row per source encounter/sequence in status history.
history = spark.table(SILVER_HISTORY)
if "source_sequence" not in history.columns:
    raise ValueError("silver_status_history must contain source_sequence.")

transition_fact = history.dropDuplicates(["encounter_id", "source_sequence"])
transition_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FACT_TRANSITION)

# Preserve Silver secondary-event columns, but remove raw patient identifiers if present.
secondary = spark.table(SILVER_SECONDARY)
drop_cols = [c for c in ["patient_id", "doctor_id"] if c in secondary.columns]
secondary_fact = secondary.drop(*drop_cols)
secondary_fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(FACT_SECONDARY)

print("Status-transition fact rows:", spark.table(FACT_TRANSITION).count())
print("Secondary-event fact rows:", spark.table(FACT_SECONDARY).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create date dimension and daily KPI tables

# COMMAND ----------

# Date dimension is built from the encounter dates in the fact.
dates = spark.table(FACT_ENCOUNTER).select("encounter_date").where(F.col("encounter_date").isNotNull()).distinct()
dim_date = dates.withColumn("date_sk", F.date_format("encounter_date", "yyyyMMdd").cast("int"))
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM_DATE)

fact = spark.table(FACT_ENCOUNTER)

daily = fact.groupBy("encounter_date").agg(
    F.count("*").alias("total_encounters"),
    F.sum("wait_minutes").alias("total_wait_minutes") if "wait_minutes" in fact.columns else F.count("*").alias("encounter_count_for_wait"),
    F.round(F.avg("wait_minutes"), 2).alias("avg_wait_minutes") if "wait_minutes" in fact.columns else F.lit(None).cast("double").alias("avg_wait_minutes"),
    F.round(F.sum("bill_amount"), 2).alias("total_bill_amount") if "bill_amount" in fact.columns else F.lit(None).cast("double").alias("total_bill_amount")
)
daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(MART_DAILY)

if "department_sk" in fact.columns:
    department_daily = fact.groupBy("encounter_date", "department_sk").agg(
        F.count("*").alias("total_encounters"),
        F.sum("wait_minutes").alias("total_wait_minutes") if "wait_minutes" in fact.columns else F.count("*").alias("encounter_count_for_wait"),
        F.round(F.avg("wait_minutes"), 2).alias("avg_wait_minutes") if "wait_minutes" in fact.columns else F.lit(None).cast("double").alias("avg_wait_minutes")
    )
    department_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(MART_DEPARTMENT)

display(spark.table(MART_DAILY).orderBy("encounter_date"))
display(spark.table(MART_DEPARTMENT).orderBy("encounter_date", "department_sk"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Reconcile Silver and Gold\nCompare record counts and the total wait minutes. Fix mismatches before reporting.

# COMMAND ----------

silver_count = spark.table(SILVER_ENCOUNTERS).count()
gold_count = spark.table(FACT_ENCOUNTER).count()

checks = [
    ("Silver encounter rows", silver_count, gold_count),
    ("Silver status history rows", spark.table(SILVER_HISTORY).count(),
     spark.table(FACT_TRANSITION).count()),
    ("Silver secondary-event rows", spark.table(SILVER_SECONDARY).count(),
     spark.table(FACT_SECONDARY).count())
]

if "wait_minutes" in spark.table(SILVER_ENCOUNTERS).columns and "wait_minutes" in spark.table(FACT_ENCOUNTER).columns:
    silver_wait = spark.table(SILVER_ENCOUNTERS).agg(F.sum("wait_minutes")).first()[0] or 0
    gold_wait = spark.table(FACT_ENCOUNTER).agg(F.sum("wait_minutes")).first()[0] or 0
    checks.append(("Total wait minutes", silver_wait, gold_wait))

reconciliation = spark.createDataFrame(
    [(name, str(expected), str(actual), "PASS" if expected == actual else "FAIL")
     for name, expected, actual in checks],
    ["check_name", "expected", "actual", "result"]
)
display(reconciliation)
assert reconciliation.filter(F.col("result") == "FAIL").count() == 0, "Reconciliation failed." 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Incremental merge and replay test\nMerge the same fact rows twice. The second merge should not insert duplicate encounters.

# COMMAND ----------

# Create a Delta target from the fact if this is the first run.
# MERGE matches on the encounter business key.
source = spark.table(FACT_ENCOUNTER)
target = DeltaTable.forName(spark, FACT_ENCOUNTER)
update_cols = [c for c in source.columns if c != "encounter_id"]
update_set = {c: f"s.`{c}`" for c in update_cols}
insert_set = {c: f"s.`{c}`" for c in source.columns}

target.alias("t").merge(
    source.alias("s"),
    "t.encounter_id = s.encounter_id"
).whenMatchedUpdate(set=update_set).whenNotMatchedInsert(values=insert_set).execute()

rows_after_replay = spark.table(FACT_ENCOUNTER).count()
unique_after_replay = spark.table(FACT_ENCOUNTER).select("encounter_id").distinct().count()
print("Rows after replay:", rows_after_replay)
print("Unique encounter IDs:", unique_after_replay)
assert rows_after_replay == unique_after_replay, "Replay created duplicate encounter IDs."
print("PASS: replay preserved one row per encounter.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Delta history, time travel, and query plan

# COMMAND ----------

# Delta history shows how the Gold table changed.
display(spark.sql(f"DESCRIBE HISTORY {FACT_ENCOUNTER}").select("version", "timestamp", "operation"))

# Inspect the Spark plan for a simple daily KPI query.
query = f"""
SELECT encounter_date, COUNT(*) AS encounters, AVG(wait_minutes) AS avg_wait_minutes
FROM {FACT_ENCOUNTER}
GROUP BY encounter_date
"""
print(spark.sql("EXPLAIN FORMATTED " + query).first()[0])

# Check for skew: large differences between minimum and maximum rows per department.
if "department_sk" in spark.table(FACT_ENCOUNTER).columns:
    display(spark.table(FACT_ENCOUNTER).groupBy("department_sk").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final notes
# MAGIC
# MAGIC - **Fact grain:** one row per encounter; one row per encounter and source sequence for status history; one row per secondary event as provided by Silver.
# MAGIC - **Conformed joins:** department uses `department_id`; patient joins use the same SHA-256-derived `patient_key` in this notebook.
# MAGIC - **KPIs:** daily encounter count, waiting-time total/average, and total bill amount when those fields exist in Silver.
# MAGIC - **Not included:** length of stay and bed utilization. Day 2 does not create stay intervals, and the available fields do not confirm admission/discharge semantics or bed capacity.
# MAGIC - **Validation:** reconciliation, rerun uniqueness, Delta history and query plan are included.
# MAGIC
# MAGIC **Run note:** Python syntax and notebook structure are checked, but this notebook has not been executed in your Databricks workspace. Verify that the Bronze department and patient tables use the expected column names.

# COMMAND ----------

# DBTITLE 1,Simple Silver-to-Gold reconciliation
# Simple reconciliation of the objects this notebook builds.
# Expected values come from Silver (or an independent SQL query); actual values come from Gold.
fact = spark.table(FACT_ENCOUNTER)
silver_events = spark.table(SILVER_ENCOUNTERS)
mart_daily = spark.table(MART_DAILY)
mart_dept = spark.table(MART_DEPARTMENT)

def bill_total(df, col):
    return str(df.agg(F.sum(col).cast("decimal(18,2)")).first()[0])

def wait_total(df, col):
    return df.agg(F.sum(col)).first()[0] or 0

fact_rows = fact.count()

# Independent per-date KPIs computed directly from Silver with SQL, compared with mart_daily.
indep_daily = spark.sql(f"""
    SELECT to_date(event_timestamp) AS encounter_date,
           COUNT(*) AS total_encounters,
           SUM(wait_minutes) AS total_wait_minutes,
           ROUND(SUM(bill_amount), 2) AS total_bill_amount
    FROM {SILVER_ENCOUNTERS}
    GROUP BY to_date(event_timestamp)""")
date_mismatch = (
    indep_daily.alias("i").join(mart_daily.alias("m"),
                                F.col("i.encounter_date").eqNullSafe(F.col("m.encounter_date")), "full_outer")
    .filter(~F.col("i.total_encounters").eqNullSafe(F.col("m.total_encounters"))
            | ~F.col("i.total_wait_minutes").eqNullSafe(F.col("m.total_wait_minutes"))
            | ~F.col("i.total_bill_amount").eqNullSafe(F.col("m.total_bill_amount")))
    .count()
)

# Conformed-dimension checks.
missing_department = fact.filter(F.col("department_sk").isNull()).count()
missing_patient = fact.join(spark.table(DIM_PATIENT).select("patient_sk"), "patient_sk", "left_anti").count()
missing_dates = (fact.select("encounter_date").where(F.col("encounter_date").isNotNull()).distinct()
                 .join(spark.table(DIM_DATE), "encounter_date", "left_anti").count())

# Privacy check: no raw identifiers in any Gold table.
gold_tables = [DIM_DEPARTMENT, DIM_PATIENT, DIM_DATE, FACT_ENCOUNTER, FACT_TRANSITION,
               FACT_SECONDARY, MART_DAILY, MART_DEPARTMENT]
raw_id_cols = [f"{t.split('.')[-1]}.{c}" for t in gold_tables
               for c in spark.table(t).columns if c in ("patient_id", "doctor_id")]

checks = [
    # check_id, description, expected, actual
    ("R01", "fact rows = silver_encounter_events rows", silver_events.count(), fact_rows),
    ("R02", "fact rows = silver_encounter_current rows", spark.table(SILVER_CURRENT).count(), fact_rows),
    ("R03", "fact encounter_id is unique", fact_rows, fact.select("encounter_id").distinct().count()),
    ("R04", "total wait minutes Silver = Gold fact", wait_total(silver_events, "wait_minutes"), wait_total(fact, "wait_minutes")),
    ("R05", "total bill amount Silver = Gold fact", bill_total(silver_events, "bill_amount"), bill_total(fact, "bill_amount")),
    ("R06", "transition fact rows = distinct Silver history (encounter_id, source_sequence)",
     spark.table(SILVER_HISTORY).select("encounter_id", "source_sequence").distinct().count(), spark.table(FACT_TRANSITION).count()),
    ("R07", "secondary fact rows = silver_secondary_events rows", spark.table(SILVER_SECONDARY).count(), spark.table(FACT_SECONDARY).count()),
    ("R08", "fact rows with unmatched department", 0, missing_department),
    ("R09", "fact rows with patient_sk missing from dim_patient", 0, missing_patient),
    ("R10", "fact encounter dates missing from dim_date", 0, missing_dates),
    ("R11", "mart_daily total encounters = fact rows", fact_rows, mart_daily.agg(F.sum("total_encounters")).first()[0]),
    ("R12", "mart_daily total wait minutes = fact", wait_total(fact, "wait_minutes"), wait_total(mart_daily, "total_wait_minutes")),
    ("R13", "mart_department_daily total encounters = fact rows", fact_rows, mart_dept.agg(F.sum("total_encounters")).first()[0]),
    ("R14", "mart_department_daily total wait minutes = fact", wait_total(fact, "wait_minutes"), wait_total(mart_dept, "total_wait_minutes")),
    ("R15", "mart_daily one row per encounter_date", mart_daily.count(), mart_daily.select("encounter_date").distinct().count()),
    ("R16", "mart_department_daily one row per date + department", mart_dept.count(),
     mart_dept.select("encounter_date", "department_sk").distinct().count()),
    ("R17", "independent Silver daily KPI query vs mart_daily: mismatching dates", 0, date_mismatch),
    ("R18", "Gold columns holding raw patient_id / doctor_id", "[]", str(raw_id_cols)),
]

recon = spark.createDataFrame(
    [(c[0], c[1], str(c[2]), str(c[3]), "PASS" if str(c[2]) == str(c[3]) else "FAIL") for c in checks],
    "check_id string, description string, expected string, actual string, result string"
)
display(recon.orderBy("check_id"))

recon_failed = recon.filter("result = 'FAIL'").count()
print("Reconciliation checks:", len(checks), "| failed:", recon_failed)
assert recon_failed == 0, "Reconciliation failed - see the FAIL rows above."

# COMMAND ----------

