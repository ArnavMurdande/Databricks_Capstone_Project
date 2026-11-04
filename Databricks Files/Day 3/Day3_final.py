# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone 08 — Day 3: Gold Layer and Analytics Marts
# MAGIC
# MAGIC ## Objective
# MAGIC Build a dimensional Gold model and business-facing analytical marts from the **updated Day 2 Silver tables**.
# MAGIC
# MAGIC ### Day 2 inputs
# MAGIC - `silver_patient_encounters`
# MAGIC - `silver_lab_test_events`
# MAGIC - `silver_bed_transfer_events`
# MAGIC - `silver_patient_master`
# MAGIC - `silver_department_master`
# MAGIC - `silver_bed_master`
# MAGIC
# MAGIC ### Gold outputs
# MAGIC **Dimensions:** patient, department, bed, date  
# MAGIC **Facts:** encounter, lab event, bed transfer  
# MAGIC **Marts:** daily hospital KPIs, department daily KPIs, lab daily KPIs, bed-transfer daily KPIs
# MAGIC
# MAGIC ### Important modelling decisions
# MAGIC - Encounter fact grain: one row per `encounter_id`.
# MAGIC - Lab fact grain: one row per `lab_event_id`.
# MAGIC - Bed-transfer fact grain: one row per `transfer_id`.
# MAGIC - Patient identifiers are SHA-256 hashed before entering Gold. Hashing is pseudonymization, not encryption.
# MAGIC - No old status-history or secondary-event tables are used; they are not part of the updated Day 2 pipeline.
# MAGIC - No `wait_minutes` or `bill_amount` metrics are invented; those columns do not exist in the updated source.
# MAGIC - Bed-transfer `from_bed_id` and `to_bed_id` use different source prefixes from the bed master IDs. They are retained as source identifiers and are **not** falsely joined to the bed dimension.
# MAGIC - The notebook does not drop or recreate the Gold schema. It overwrites only the Gold output tables it owns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports, catalog and table configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from delta.tables import DeltaTable

CATALOG = "capstone_8hospital"
SILVER_SCHEMA = "silver_schema"
GOLD_SCHEMA = "gold_schema"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{GOLD_SCHEMA}`")

def table(schema, name):
    return f"{CATALOG}.{schema}.{name}"

# Day 2 Silver inputs
SILVER_ENCOUNTERS = table(SILVER_SCHEMA, "silver_patient_encounters")
SILVER_LAB_EVENTS = table(SILVER_SCHEMA, "silver_lab_test_events")
SILVER_BED_TRANSFERS = table(SILVER_SCHEMA, "silver_bed_transfer_events")
SILVER_PATIENTS = table(SILVER_SCHEMA, "silver_patient_master")
SILVER_DEPARTMENTS = table(SILVER_SCHEMA, "silver_department_master")
SILVER_BEDS = table(SILVER_SCHEMA, "silver_bed_master")

# Gold dimensions
DIM_PATIENT = table(GOLD_SCHEMA, "dim_patient")
DIM_DEPARTMENT = table(GOLD_SCHEMA, "dim_department")
DIM_BED = table(GOLD_SCHEMA, "dim_bed")
DIM_DATE = table(GOLD_SCHEMA, "dim_date")

# Gold facts
FACT_ENCOUNTER = table(GOLD_SCHEMA, "fact_encounter")
FACT_LAB_EVENT = table(GOLD_SCHEMA, "fact_lab_event")
FACT_BED_TRANSFER = table(GOLD_SCHEMA, "fact_bed_transfer")

# Gold analytical marts
MART_DAILY = table(GOLD_SCHEMA, "mart_hospital_daily")
MART_DEPARTMENT_DAILY = table(GOLD_SCHEMA, "mart_department_daily")
MART_LAB_DAILY = table(GOLD_SCHEMA, "mart_lab_daily")
MART_BED_TRANSFER_DAILY = table(GOLD_SCHEMA, "mart_bed_transfer_daily")

print("Catalog:", CATALOG)
print("Silver schema:", SILVER_SCHEMA)
print("Gold schema:", GOLD_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify that all Day 2 Silver tables exist

# COMMAND ----------

silver_sources = [
    SILVER_ENCOUNTERS, SILVER_LAB_EVENTS, SILVER_BED_TRANSFERS,
    SILVER_PATIENTS, SILVER_DEPARTMENTS, SILVER_BEDS
]

missing_tables = [name for name in silver_sources if not spark.catalog.tableExists(name)]
if missing_tables:
    raise ValueError(
        "Required Day 2 Silver tables are missing: " + ", ".join(missing_tables) +
        ". Run Day2_Healthcare_Updated.ipynb successfully before Day 3."
    )

for name in silver_sources:
    df = spark.table(name)
    print(f"{name}: {df.count():,} rows")
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Silver data and verify required columns

# COMMAND ----------

encounters = spark.table(SILVER_ENCOUNTERS)
lab_events = spark.table(SILVER_LAB_EVENTS)
bed_transfers = spark.table(SILVER_BED_TRANSFERS)
patients = spark.table(SILVER_PATIENTS)
departments = spark.table(SILVER_DEPARTMENTS)
beds = spark.table(SILVER_BEDS)

def require_columns(df, required, dataset_name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{dataset_name} is missing required columns: {missing}")

require_columns(encounters, [
    "encounter_id", "patient_id", "department_id", "admission_ts",
    "discharge_ts", "encounter_type", "triage_priority",
    "encounter_status", "bed_id"
], "silver_patient_encounters")

require_columns(lab_events, [
    "lab_event_id", "encounter_id", "patient_id", "test_code",
    "ordered_ts", "collected_ts", "result_ts", "result_status",
    "department_id", "priority"
], "silver_lab_test_events")

require_columns(bed_transfers, [
    "transfer_id", "encounter_id", "patient_id", "from_bed_id",
    "to_bed_id", "transfer_ts", "department_id", "transfer_status"
], "silver_bed_transfer_events")

require_columns(patients, ["patient_id"], "silver_patient_master")
require_columns(departments, ["department_id"], "silver_department_master")
require_columns(beds, ["bed_id", "department_id"], "silver_bed_master")

print("PASS: all required Silver columns are available.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build the patient dimension

# COMMAND ----------

# Patient ID is converted to a one-way SHA-256 key before writing to Gold.
# Do not include the raw patient_id in the dimension.
patient_attributes = [
    c for c in [
        "synthetic_patient_code", "age_band", "sex", "city",
        "registration_date", "consent_flag", "insurance_type",
        "patient_status", "primary_language"
    ] if c in patients.columns
]

dim_patient = (
    patients
    .select(
        F.sha2(F.trim(F.col("patient_id").cast("string")), 256).alias("patient_key"),
        *[F.col(c) for c in patient_attributes]
    )
    .filter(F.col("patient_key").isNotNull())
    .dropDuplicates(["patient_key"])
    .withColumn("patient_sk", F.xxhash64("patient_key"))
)

(dim_patient.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(DIM_PATIENT))

print("Patient dimension rows:", spark.table(DIM_PATIENT).count())
display(spark.table(DIM_PATIENT).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Build the department dimension

# COMMAND ----------

department_attributes = [
    c for c in [
        "department_name", "specialty", "hospital_site", "floor",
        "service_level", "active_flag", "head_clinician_id",
        "capacity_beds", "region"
    ] if c in departments.columns
]

dim_department = (
    departments
    .select(
        F.trim(F.col("department_id").cast("string")).alias("department_id"),
        *[F.col(c) for c in department_attributes]
    )
    .filter(F.col("department_id").isNotNull())
    .dropDuplicates(["department_id"])
    .withColumn("department_sk", F.xxhash64("department_id"))
)

(dim_department.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(DIM_DEPARTMENT))

print("Department dimension rows:", spark.table(DIM_DEPARTMENT).count())
display(spark.table(DIM_DEPARTMENT).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build the bed dimension

# COMMAND ----------

bed_attributes = [
    c for c in [
        "department_id", "ward", "bed_type", "isolation_capable",
        "bed_status", "floor", "hospital_site", "active_flag",
        "effective_date"
    ] if c in beds.columns
]

dim_bed = (
    beds
    .select(
        F.trim(F.col("bed_id").cast("string")).alias("bed_id"),
        *[F.col(c) for c in bed_attributes]
    )
    .filter(F.col("bed_id").isNotNull())
    .dropDuplicates(["bed_id"])
    .withColumn("bed_sk", F.xxhash64("bed_id"))
)

(dim_bed.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(DIM_BED))

print("Bed dimension rows:", spark.table(DIM_BED).count())
display(spark.table(DIM_BED).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build the date dimension

# COMMAND ----------

# Collect all relevant business dates from encounters, lab orders and transfers.
date_frames = [
    encounters.select(F.to_date("admission_ts").alias("calendar_date")),
    encounters.select(F.to_date("discharge_ts").alias("calendar_date")),
    lab_events.select(F.to_date("ordered_ts").alias("calendar_date")),
    lab_events.select(F.to_date("collected_ts").alias("calendar_date")),
    lab_events.select(F.to_date("result_ts").alias("calendar_date")),
    bed_transfers.select(F.to_date("transfer_ts").alias("calendar_date"))
]

all_dates = date_frames[0]
for date_df in date_frames[1:]:
    all_dates = all_dates.unionByName(date_df)

date_bounds = all_dates.filter(F.col("calendar_date").isNotNull()).agg(
    F.min("calendar_date").alias("min_date"),
    F.max("calendar_date").alias("max_date")
).first()

if date_bounds["min_date"] is None:
    raise ValueError("No valid business dates found in Silver data.")

dim_date = (
    spark.range(1)
    .select(F.explode(
        F.sequence(F.lit(date_bounds["min_date"]), F.lit(date_bounds["max_date"]))
    ).alias("calendar_date"))
    .withColumn("date_sk", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("calendar_date"))
    .withColumn("quarter", F.quarter("calendar_date"))
    .withColumn("month", F.month("calendar_date"))
    .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("calendar_date"))
    .withColumn("day_of_week", F.dayofweek("calendar_date"))
    .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("calendar_date"))
)

(dim_date.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(DIM_DATE))

print("Date dimension rows:", spark.table(DIM_DATE).count())
display(spark.table(DIM_DATE).orderBy("calendar_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Build the encounter fact table

# COMMAND ----------

# Grain: one row per encounter_id.
patient_lookup = spark.table(DIM_PATIENT).select("patient_key", "patient_sk")
department_lookup = spark.table(DIM_DEPARTMENT).select("department_id", "department_sk")
bed_lookup = spark.table(DIM_BED).select("bed_id", "bed_sk")

encounter_fact_source = (
    encounters
    .withColumn("patient_key", F.sha2(F.trim(F.col("patient_id").cast("string")), 256))
    .withColumn("admission_date", F.to_date("admission_ts"))
    .withColumn("discharge_date", F.to_date("discharge_ts"))
    .join(patient_lookup, "patient_key", "left")
    .join(department_lookup, "department_id", "left")
    .join(bed_lookup, "bed_id", "left")
)

if "length_of_stay_minutes" not in encounter_fact_source.columns:
    encounter_fact_source = encounter_fact_source.withColumn(
        "length_of_stay_minutes",
        F.when(
            F.col("discharge_ts").isNotNull() & F.col("admission_ts").isNotNull(),
            (F.col("discharge_ts").cast("long") - F.col("admission_ts").cast("long")) / 60.0
        )
    )

encounter_fact_source = encounter_fact_source.withColumn(
    "admission_date_sk",
    F.date_format("admission_date", "yyyyMMdd").cast("int")
).withColumn(
    "discharge_date_sk",
    F.date_format("discharge_date", "yyyyMMdd").cast("int")
)

fact_encounter = encounter_fact_source.select(
    "encounter_id", "patient_sk", "department_sk", "bed_sk",
    "admission_date_sk", "discharge_date_sk",
    "admission_ts", "discharge_ts", "admission_date", "discharge_date",
    "encounter_type", "triage_priority", "encounter_status",
    "attending_clinician_id", "admission_source", "diagnosis_group",
    "length_of_stay_minutes", "dq_status", "missing_optional_attribute_flag"
).dropDuplicates(["encounter_id"])

(fact_encounter.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(FACT_ENCOUNTER))

print("Encounter fact rows:", spark.table(FACT_ENCOUNTER).count())
display(spark.table(FACT_ENCOUNTER).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Build the laboratory event fact table

# COMMAND ----------

# Grain: one row per lab_event_id.
lab_fact_source = (
    lab_events
    .withColumn("patient_key", F.sha2(F.trim(F.col("patient_id").cast("string")), 256))
    .withColumn("ordered_date", F.to_date("ordered_ts"))
    .withColumn("collected_date", F.to_date("collected_ts"))
    .withColumn("result_date", F.to_date("result_ts"))
    .join(patient_lookup, "patient_key", "left")
    .join(department_lookup, "department_id", "left")
)

lab_fact_source = (
    lab_fact_source
    .withColumn("ordered_date_sk", F.date_format("ordered_date", "yyyyMMdd").cast("int"))
    .withColumn("collected_date_sk", F.date_format("collected_date", "yyyyMMdd").cast("int"))
    .withColumn("result_date_sk", F.date_format("result_date", "yyyyMMdd").cast("int"))
)

fact_lab_event = lab_fact_source.select(
    "lab_event_id", "encounter_id", "patient_sk", "department_sk",
    "ordered_date_sk", "collected_date_sk", "result_date_sk",
    "ordered_ts", "collected_ts", "result_ts",
    "test_code", "result_status", "priority",
    "dq_status", "missing_optional_attribute_flag"
).dropDuplicates(["lab_event_id"])

(fact_lab_event.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(FACT_LAB_EVENT))

print("Lab event fact rows:", spark.table(FACT_LAB_EVENT).count())
display(spark.table(FACT_LAB_EVENT).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Build the bed-transfer fact table

# COMMAND ----------

# Grain: one row per transfer_id.
# Source from/to bed identifiers are intentionally kept as degenerate source dimensions:
# Day 2 found that their prefixes do not match bed_master.bed_id.
transfer_fact_source = (
    bed_transfers
    .withColumn("patient_key", F.sha2(F.trim(F.col("patient_id").cast("string")), 256))
    .withColumn("transfer_date", F.to_date("transfer_ts"))
    .join(patient_lookup, "patient_key", "left")
    .join(department_lookup, "department_id", "left")
    .withColumn("transfer_date_sk", F.date_format("transfer_date", "yyyyMMdd").cast("int"))
)

fact_bed_transfer = transfer_fact_source.select(
    "transfer_id", "encounter_id", "patient_sk", "department_sk",
    "transfer_date_sk", "transfer_date", "transfer_ts", "recorded_ts",
    "from_bed_id", "to_bed_id", "transfer_reason", "transfer_status",
    "dq_status", "missing_optional_attribute_flag"
).dropDuplicates(["transfer_id"])

(fact_bed_transfer.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(FACT_BED_TRANSFER))

print("Bed transfer fact rows:", spark.table(FACT_BED_TRANSFER).count())
display(spark.table(FACT_BED_TRANSFER).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Create the hospital daily KPI mart

# COMMAND ----------

# Daily metrics are based on encounter admission date.
enc_fact = spark.table(FACT_ENCOUNTER)

mart_hospital_daily = (
    enc_fact.groupBy("admission_date", "admission_date_sk")
    .agg(
        F.countDistinct("encounter_id").alias("total_encounters"),
        F.countDistinct(F.when(F.col("encounter_status") == "DISCHARGED", F.col("encounter_id"))).alias("discharged_encounters"),
        F.countDistinct(F.when(F.col("encounter_status") == "ADMITTED", F.col("encounter_id"))).alias("admitted_encounters"),
        F.countDistinct(F.when(F.col("encounter_status") == "TRANSFERRED", F.col("encounter_id"))).alias("transferred_encounters"),
        F.round(F.avg("length_of_stay_minutes"), 2).alias("avg_length_of_stay_minutes"),
        F.round(F.max("length_of_stay_minutes"), 2).alias("max_length_of_stay_minutes")
    )
)

(mart_hospital_daily.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(MART_DAILY))

display(spark.table(MART_DAILY).orderBy("admission_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Create the department daily KPI mart

# COMMAND ----------

mart_department_daily = (
    enc_fact.groupBy("admission_date", "admission_date_sk", "department_sk")
    .agg(
        F.countDistinct("encounter_id").alias("total_encounters"),
        F.countDistinct(F.when(F.col("encounter_status") == "DISCHARGED", F.col("encounter_id"))).alias("discharged_encounters"),
        F.round(F.avg("length_of_stay_minutes"), 2).alias("avg_length_of_stay_minutes"),
        F.countDistinct("patient_sk").alias("unique_patients")
    )
)

(mart_department_daily.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(MART_DEPARTMENT_DAILY))

display(
    spark.table(MART_DEPARTMENT_DAILY)
    .join(spark.table(DIM_DEPARTMENT).select("department_sk", "department_name"), "department_sk", "left")
    .orderBy("admission_date", "department_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Create the laboratory daily KPI mart

# COMMAND ----------

lab_fact = spark.table(FACT_LAB_EVENT)

mart_lab_daily = (
    lab_fact.groupBy(
        F.to_date("ordered_ts").alias("lab_order_date"),
        "ordered_date_sk"
    )
    .agg(
        F.countDistinct("lab_event_id").alias("total_lab_orders"),
        F.countDistinct(F.when(F.col("result_status") == "RESULTED", F.col("lab_event_id"))).alias("resulted_tests"),
        F.countDistinct(F.when(F.col("result_status") != "RESULTED", F.col("lab_event_id"))).alias("not_resulted_tests"),
        F.countDistinct("encounter_id").alias("encounters_with_lab_orders"),
        F.countDistinct("patient_sk").alias("patients_with_lab_orders")
    )
)

(mart_lab_daily.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(MART_LAB_DAILY))

display(spark.table(MART_LAB_DAILY).orderBy("lab_order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Create the bed-transfer daily KPI mart

# COMMAND ----------

transfer_fact = spark.table(FACT_BED_TRANSFER)

mart_bed_transfer_daily = (
    transfer_fact.groupBy("transfer_date", "transfer_date_sk", "department_sk")
    .agg(
        F.countDistinct("transfer_id").alias("total_transfers"),
        F.countDistinct("encounter_id").alias("encounters_with_transfers"),
        F.countDistinct("patient_sk").alias("patients_transferred"),
        F.countDistinct(F.when(F.col("transfer_status") == "ACTIVE", F.col("transfer_id"))).alias("active_transfers"),
        F.countDistinct(F.when(F.col("transfer_status") == "INACTIVE", F.col("transfer_id"))).alias("inactive_transfers")
    )
)

(mart_bed_transfer_daily.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable(MART_BED_TRANSFER_DAILY))

display(
    spark.table(MART_BED_TRANSFER_DAILY)
    .join(spark.table(DIM_DEPARTMENT).select("department_sk", "department_name"), "department_sk", "left")
    .orderBy("transfer_date", "department_name")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Gold referential-integrity checks

# COMMAND ----------

# A missing surrogate indicates that a Silver business key did not resolve to its dimension.
fact_enc = spark.table(FACT_ENCOUNTER)
fact_lab = spark.table(FACT_LAB_EVENT)
fact_transfer = spark.table(FACT_BED_TRANSFER)

fk_checks = [
    ("Encounter -> patient dimension", fact_enc.filter(F.col("patient_sk").isNull()).count()),
    ("Encounter -> department dimension", fact_enc.filter(F.col("department_sk").isNull()).count()),
    ("Encounter -> bed dimension for populated bed_id",
     fact_enc.filter(F.col("bed_sk").isNull() & F.col("bed_id").isNotNull() & (F.col("bed_id") != "")).count()),
    ("Lab -> patient dimension", fact_lab.filter(F.col("patient_sk").isNull()).count()),
    ("Lab -> department dimension", fact_lab.filter(F.col("department_sk").isNull()).count()),
    ("Transfer -> patient dimension", fact_transfer.filter(F.col("patient_sk").isNull()).count()),
    ("Transfer -> department dimension", fact_transfer.filter(F.col("department_sk").isNull()).count())
]

fk_result = spark.createDataFrame(
    [(name, count, "PASS" if count == 0 else "FAIL") for name, count in fk_checks],
    ["referential_check", "unmatched_rows", "result"]
)
display(fk_result)
assert fk_result.filter(F.col("result") == "FAIL").count() == 0, "Gold dimension lookup failed."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Reconcile Silver and Gold fact counts

# COMMAND ----------

count_checks = [
    ("Encounter rows", spark.table(SILVER_ENCOUNTERS).count(), spark.table(FACT_ENCOUNTER).count()),
    ("Lab event rows", spark.table(SILVER_LAB_EVENTS).count(), spark.table(FACT_LAB_EVENT).count()),
    ("Bed transfer rows", spark.table(SILVER_BED_TRANSFERS).count(), spark.table(FACT_BED_TRANSFER).count())
]

count_reconciliation = spark.createDataFrame(
    [(name, expected, actual, "PASS" if expected == actual else "FAIL")
     for name, expected, actual in count_checks],
    ["check_name", "silver_count", "gold_count", "result"]
)
display(count_reconciliation)
assert count_reconciliation.filter(F.col("result") == "FAIL").count() == 0, "Silver-to-Gold fact count reconciliation failed."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Reconcile daily mart totals against their fact tables

# COMMAND ----------

# Recompute the daily encounter aggregates directly from the fact and compare with the mart.
independent_hospital_daily = (
    spark.table(FACT_ENCOUNTER)
    .groupBy("admission_date", "admission_date_sk")
    .agg(
        F.countDistinct("encounter_id").alias("total_encounters"),
        F.countDistinct(F.when(F.col("encounter_status") == "DISCHARGED", F.col("encounter_id"))).alias("discharged_encounters"),
        F.countDistinct(F.when(F.col("encounter_status") == "ADMITTED", F.col("encounter_id"))).alias("admitted_encounters"),
        F.countDistinct(F.when(F.col("encounter_status") == "TRANSFERRED", F.col("encounter_id"))).alias("transferred_encounters"),
        F.round(F.avg("length_of_stay_minutes"), 2).alias("avg_length_of_stay_minutes")
    )
)

mart_hospital = spark.table(MART_DAILY)
daily_join = independent_hospital_daily.alias("i").join(
    mart_hospital.alias("m"),
    F.col("i.admission_date").eqNullSafe(F.col("m.admission_date")),
    "full_outer"
)

daily_mismatch = daily_join.filter(
    ~F.col("i.total_encounters").eqNullSafe(F.col("m.total_encounters")) |
    ~F.col("i.discharged_encounters").eqNullSafe(F.col("m.discharged_encounters")) |
    ~F.col("i.admitted_encounters").eqNullSafe(F.col("m.admitted_encounters")) |
    ~F.col("i.transferred_encounters").eqNullSafe(F.col("m.transferred_encounters")) |
    ~F.col("i.avg_length_of_stay_minutes").eqNullSafe(F.col("m.avg_length_of_stay_minutes"))
).count()

print("Hospital daily KPI mismatching dates:", daily_mismatch)
assert daily_mismatch == 0, "Hospital daily mart does not reconcile to encounter fact."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Business-key uniqueness and privacy checks

# COMMAND ----------

unique_key_checks = [
    ("dim_patient.patient_sk", spark.table(DIM_PATIENT).count(), spark.table(DIM_PATIENT).select("patient_sk").distinct().count()),
    ("dim_department.department_sk", spark.table(DIM_DEPARTMENT).count(), spark.table(DIM_DEPARTMENT).select("department_sk").distinct().count()),
    ("dim_bed.bed_sk", spark.table(DIM_BED).count(), spark.table(DIM_BED).select("bed_sk").distinct().count()),
    ("dim_date.date_sk", spark.table(DIM_DATE).count(), spark.table(DIM_DATE).select("date_sk").distinct().count()),
    ("fact_encounter.encounter_id", spark.table(FACT_ENCOUNTER).count(), spark.table(FACT_ENCOUNTER).select("encounter_id").distinct().count()),
    ("fact_lab_event.lab_event_id", spark.table(FACT_LAB_EVENT).count(), spark.table(FACT_LAB_EVENT).select("lab_event_id").distinct().count()),
    ("fact_bed_transfer.transfer_id", spark.table(FACT_BED_TRANSFER).count(), spark.table(FACT_BED_TRANSFER).select("transfer_id").distinct().count())
]

unique_df = spark.createDataFrame(
    [(name, total, distinct, "PASS" if total == distinct else "FAIL")
     for name, total, distinct in unique_key_checks],
    ["key_check", "row_count", "distinct_key_count", "result"]
)
display(unique_df)
assert unique_df.filter(F.col("result") == "FAIL").count() == 0, "A Gold dimension or fact business key is not unique."

# Ensure raw patient_id does not appear in any Gold output.
gold_tables = [
    DIM_PATIENT, DIM_DEPARTMENT, DIM_BED, DIM_DATE,
    FACT_ENCOUNTER, FACT_LAB_EVENT, FACT_BED_TRANSFER,
    MART_DAILY, MART_DEPARTMENT_DAILY, MART_LAB_DAILY, MART_BED_TRANSFER_DAILY
]
raw_patient_columns = [
    f"{name.split('.')[-1]}.{column}"
    for name in gold_tables for column in spark.table(name).columns
    if column == "patient_id"
]
print("Gold columns containing raw patient_id:", raw_patient_columns)
assert len(raw_patient_columns) == 0, "Raw patient_id found in Gold."

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Incremental MERGE replay test for the encounter fact

# COMMAND ----------

# The upstream Day 2 Silver notebook currently writes a complete validated snapshot.
# This section demonstrates an idempotent Delta MERGE for the encounter fact target.
# It does not claim that the source ingestion itself is incremental.

merge_source = spark.table(FACT_ENCOUNTER)
merge_target = DeltaTable.forName(spark, FACT_ENCOUNTER)

update_columns = [c for c in merge_source.columns if c != "encounter_id"]
update_mapping = {c: f"s.`{c}`" for c in update_columns}
insert_mapping = {c: f"s.`{c}`" for c in merge_source.columns}

merge_target.alias("t").merge(
    merge_source.alias("s"),
    "t.encounter_id = s.encounter_id"
).whenMatchedUpdate(set=update_mapping).whenNotMatchedInsert(values=insert_mapping).execute()

after_merge_count = spark.table(FACT_ENCOUNTER).count()
after_merge_distinct = spark.table(FACT_ENCOUNTER).select("encounter_id").distinct().count()

print("Rows after replay:", after_merge_count)
print("Distinct encounter IDs:", after_merge_distinct)
assert after_merge_count == after_merge_distinct, "MERGE replay produced duplicate encounter rows."
print("PASS: encounter MERGE replay is idempotent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20. Delta history and query-plan inspection

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {FACT_ENCOUNTER}")
    .select("version", "timestamp", "operation", "operationMetrics")
)

daily_query = f'''
SELECT admission_date, SUM(total_encounters) AS encounters,
       SUM(discharged_encounters) AS discharged
FROM {MART_DAILY}
GROUP BY admission_date
ORDER BY admission_date
'''

print(spark.sql("EXPLAIN FORMATTED " + daily_query).first()[0])
display(spark.sql(daily_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 21. Gold output inventory and final handoff

# COMMAND ----------

gold_outputs = [
    DIM_PATIENT, DIM_DEPARTMENT, DIM_BED, DIM_DATE,
    FACT_ENCOUNTER, FACT_LAB_EVENT, FACT_BED_TRANSFER,
    MART_DAILY, MART_DEPARTMENT_DAILY, MART_LAB_DAILY, MART_BED_TRANSFER_DAILY
]

inventory = []
for output_table in gold_outputs:
    output_df = spark.table(output_table)
    inventory.append((output_table, output_df.count(), len(output_df.columns), "CREATED"))

display(spark.createDataFrame(
    inventory,
    ["gold_table", "row_count", "column_count", "status"]
))

print("DAY 3 GOLD LAYER COMPLETE")
print("Dimensions: patient, department, bed, date")
print("Facts: encounter, lab event, bed transfer")
print("Marts: hospital daily, department daily, lab daily, bed-transfer daily")
print("Validation: referential integrity, counts, daily KPI reconciliation, uniqueness, privacy and MERGE replay.")
print("Day 4 can use these Gold tables for dashboards and business analysis.")