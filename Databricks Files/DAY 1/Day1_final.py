# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Patient Journey – Day 1
# MAGIC ## Source profiling, data quality assessment and Bronze ingestion
# MAGIC
# MAGIC **Dataset version:** `08_Healthcare` (updated source pack)  
# MAGIC **Notebook purpose:** inspect the supplied source files, profile the six logical datasets, validate source relationships, and ingest ADLS datasets into Bronze Delta tables.
# MAGIC
# MAGIC ### Source architecture
# MAGIC - **Transaction data:** patient encounter CSV batches (Auto Loader).
# MAGIC - **Event data:** lab test events and bed transfer events (Auto Loader).
# MAGIC - **Master data:** patient, department and bed seed extracts. These are CSV seed inputs intended for Azure SQL staging; this notebook labels their Bronze outputs as seed snapshots and does **not** claim to connect to Azure SQL.
# MAGIC
# MAGIC ### Before running
# MAGIC 1. Upload/extract the contents of the ZIP so that the `datasets/` folder is directly under the configured Unity Catalog Volume.
# MAGIC 2. Confirm the catalog, schema and volume names in the configuration cell.
# MAGIC 3. Ensure the schema and Volume already exist and that you have read/write permissions.
# MAGIC 4. Run cells from top to bottom. The notebook uses observed counts and does not assume defect totals from the older dataset.
# MAGIC
# MAGIC **Important:** Bronze preserves source records. Deduplication, quarantine and business-rule remediation belong in the Silver layer, not in this Day 1 ingestion notebook.

# COMMAND ----------

# 1. Imports and configuration

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from functools import reduce

# Unity Catalog configuration — update only if your workspace uses different names
CATALOG = "capstone_8hospital"
SCHEMA = "hospital_schema"
VOLUME = "ext_capstone8_vol"

BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Updated dataset folder structure inside the Volume
ENCOUNTERS_PATH = f"{BASE_PATH}/datasets/transaction_data/patient_encounters/"
EVENTS_PATH = f"{BASE_PATH}/datasets/event_data/"
MASTER_SEED_PATH = f"{BASE_PATH}/datasets/master_data/azure_sql_seed/"

# Auto Loader state folders (keep these separate from source data)
STATE_PATH = f"{BASE_PATH}/_state/day1"
ENCOUNTERS_SCHEMA_PATH = f"{STATE_PATH}/schema/patient_encounters"
LAB_SCHEMA_PATH = f"{STATE_PATH}/schema/lab_test_events"
TRANSFER_SCHEMA_PATH = f"{STATE_PATH}/schema/bed_transfer_events"

ENCOUNTERS_CHECKPOINT = f"{STATE_PATH}/checkpoints/patient_encounters"
LAB_CHECKPOINT = f"{STATE_PATH}/checkpoints/lab_test_events"
TRANSFER_CHECKPOINT = f"{STATE_PATH}/checkpoints/bed_transfer_events"

# Bronze Delta table names
BRONZE_ENCOUNTERS = f"{CATALOG}.{SCHEMA}.bronze_patient_encounters"
BRONZE_LAB_EVENTS = f"{CATALOG}.{SCHEMA}.bronze_lab_test_events"
BRONZE_BED_TRANSFERS = f"{CATALOG}.{SCHEMA}.bronze_bed_transfer_events"

# Clearly labelled seed snapshots — not a live Azure SQL connection
BRONZE_PATIENT_SEED = f"{CATALOG}.{SCHEMA}.bronze_patient_master_seed"
BRONZE_DEPARTMENT_SEED = f"{CATALOG}.{SCHEMA}.bronze_department_master_seed"
BRONZE_BED_SEED = f"{CATALOG}.{SCHEMA}.bronze_bed_master_seed"

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print("Project: Healthcare Patient Journey")
print("Catalog:", CATALOG)
print("Schema:", SCHEMA)
print("Volume root:", BASE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify the updated source folders
# MAGIC
# MAGIC The expected layout is `datasets/transaction_data/patient_encounters`, `datasets/event_data`, and `datasets/master_data/azure_sql_seed`. This cell is read-only apart from creating the Auto Loader state directories.

# COMMAND ----------

required_folders = {
    "patient_encounters": ENCOUNTERS_PATH,
    "event_data": EVENTS_PATH,
    "master_data_seed": MASTER_SEED_PATH
}

for name, path in required_folders.items():
    print(f"\\n{name}: {path}")
    entries = dbutils.fs.ls(path)
    csvs = sorted([x for x in entries if x.name.lower().endswith(".csv")], key=lambda x: x.name)
    print("CSV files:", len(csvs))
    for item in csvs:
        print(f"  {item.name} | {item.size:,} bytes")
    if not csvs:
        raise ValueError(f"No CSV files found in {path}")

for state_dir in [
    ENCOUNTERS_SCHEMA_PATH, LAB_SCHEMA_PATH, TRANSFER_SCHEMA_PATH,
    ENCOUNTERS_CHECKPOINT, LAB_CHECKPOINT, TRANSFER_CHECKPOINT
]:
    dbutils.fs.mkdirs(state_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read all encounter batches for source profiling
# MAGIC
# MAGIC The updated pack contains ten `patient_encounters_batch_*.csv` files. Each file is read independently so that file-level row counts and schemas can be reviewed. `unionByName` protects the profiling step if a later batch adds a column.

# COMMAND ----------

encounter_file_infos = sorted(
    [x for x in dbutils.fs.ls(ENCOUNTERS_PATH) if x.name.lower().endswith(".csv")],
    key=lambda x: x.name
)

encounter_dfs = []
encounter_file_stats = []

for info in encounter_file_infos:
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load(info.path)
    )
    encounter_dfs.append(df)
    encounter_file_stats.append((info.name, df.count(), len(df.columns)))

if not encounter_dfs:
    raise ValueError(f"No encounter CSV files found in {ENCOUNTERS_PATH}")

encounters_raw = reduce(
    lambda left, right: left.unionByName(right, allowMissingColumns=True),
    encounter_dfs
)

print("Encounter batch files:", len(encounter_file_infos))
print("Encounter source rows:", encounters_raw.count())
print("Encounter columns:", encounters_raw.columns)
display(encounters_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Encounter file-level counts and schema inspection

# COMMAND ----------

encounter_file_count_df = spark.createDataFrame(
    encounter_file_stats, ["file_name", "row_count", "column_count"]
).orderBy("file_name")
display(encounter_file_count_df)

print("Combined encounter schema:")
encounters_raw.printSchema()

required_encounter_columns = [
    "encounter_id", "patient_id", "department_id", "admission_ts",
    "discharge_ts", "encounter_type", "triage_priority", "encounter_status",
    "attending_clinician_id", "admission_source", "diagnosis_group", "bed_id"
]
missing = [c for c in required_encounter_columns if c not in encounters_raw.columns]
if missing:
    raise ValueError(f"Encounter source is missing required columns: {missing}")
print("Required encounter columns: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Profile encounter keys, duplicate records and nulls

# COMMAND ----------

encounter_total_rows = encounters_raw.count()
encounter_distinct_ids = encounters_raw.select("encounter_id").distinct().count()

encounter_key_duplicates = (
    encounters_raw.groupBy("encounter_id").count()
    .filter(F.col("count") > 1)
    .orderBy("encounter_id")
)

# Exact duplicate check across the source business columns
encounter_exact_duplicate_groups = (
    encounters_raw.groupBy(*required_encounter_columns)
    .count().filter(F.col("count") > 1)
)
exact_duplicate_extra_rows = (
    encounter_exact_duplicate_groups
    .select(F.sum(F.col("count") - 1).alias("n")).first()["n"] or 0
)

print("Encounter rows:", encounter_total_rows)
print("Distinct encounter IDs:", encounter_distinct_ids)
print("Duplicate encounter ID groups:", encounter_key_duplicates.count())
print("Exact duplicate groups:", encounter_exact_duplicate_groups.count())
print("Extra exact duplicate rows:", int(exact_duplicate_extra_rows))
display(encounter_key_duplicates)
display(encounter_exact_duplicate_groups)

encounter_null_profile = encounters_raw.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in encounters_raw.columns
])
display(encounter_null_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Profile encounter categories and timestamp relationships

# COMMAND ----------

for column_name in ["encounter_type", "triage_priority", "encounter_status", "admission_source", "diagnosis_group"]:
    print(f"Distribution: {column_name}")
    display(
        encounters_raw.groupBy(column_name).count()
        .orderBy(F.desc("count"), column_name)
    )

encounters_typed = (
    encounters_raw
    .withColumn("admission_ts_parsed", F.to_timestamp("admission_ts"))
    .withColumn("discharge_ts_parsed", F.to_timestamp("discharge_ts"))
)

print("Admission timestamp parse failures:",
      encounters_typed.filter(F.col("admission_ts").isNotNull() & F.col("admission_ts_parsed").isNull()).count())
print("Discharge timestamp parse failures:",
      encounters_typed.filter(F.col("discharge_ts").isNotNull() & F.col("discharge_ts_parsed").isNull()).count())
print("Discharge before admission:",
      encounters_typed.filter(
          F.col("admission_ts_parsed").isNotNull() &
          F.col("discharge_ts_parsed").isNotNull() &
          (F.col("discharge_ts_parsed") < F.col("admission_ts_parsed"))
      ).count())
display(
    encounters_typed.select("encounter_id", "admission_ts", "discharge_ts",
                            "admission_ts_parsed", "discharge_ts_parsed").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Read and profile the three master seed extracts
# MAGIC
# MAGIC These files are supplied under `azure_sql_seed`. They are seed extracts for Azure SQL staging. Reading them here supports Day 1 profiling and seed-snapshot validation; it is not a JDBC connection to Azure SQL.

# COMMAND ----------

def read_seed_csv(file_name):
    path = MASTER_SEED_PATH + file_name
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .load(path)
    )

patient_seed_raw = read_seed_csv("patient_master.csv")
department_seed_raw = read_seed_csv("department_master.csv")
bed_seed_raw = read_seed_csv("bed_master.csv")

seed_sources = [
    ("patient_master", patient_seed_raw, "patient_id"),
    ("department_master", department_seed_raw, "department_id"),
    ("bed_master", bed_seed_raw, "bed_id")
]

for name, df, key in seed_sources:
    print(f"{name}: {df.count()} rows | {len(df.columns)} columns | key={key}")
    df.printSchema()
    display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate master keys and profile master attributes

# COMMAND ----------

for name, df, key in seed_sources:
    print(f"\\n{name} key checks")
    print("Null keys:", df.filter(F.col(key).isNull()).count())
    print("Distinct keys:", df.select(key).distinct().count())
    print("Duplicate key groups:",
          df.groupBy(key).count().filter(F.col("count") > 1).count())
    nulls = df.select([
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ])
    display(nulls)

print("Patient status distribution")
display(patient_seed_raw.groupBy("patient_status").count().orderBy(F.desc("count")))
print("Department region distribution")
display(department_seed_raw.groupBy("region").count().orderBy(F.desc("count")))
print("Bed status distribution")
display(bed_seed_raw.groupBy("bed_status").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Read and profile lab test events

# COMMAND ----------

LAB_FILE = "lab_test_events.csv"
lab_path = EVENTS_PATH + LAB_FILE

lab_events_raw = (
    spark.read.format("csv").option("header", "true")
    .option("inferSchema", "true").option("mode", "PERMISSIVE")
    .load(lab_path)
)

print("Lab event rows:", lab_events_raw.count())
print("Lab event columns:", lab_events_raw.columns)
lab_events_raw.printSchema()
display(lab_events_raw.limit(10))

required_lab_columns = [
    "lab_event_id", "encounter_id", "patient_id", "test_code",
    "ordered_ts", "collected_ts", "result_ts", "result_status",
    "department_id", "priority"
]
missing = [c for c in required_lab_columns if c not in lab_events_raw.columns]
if missing:
    raise ValueError(f"Lab event source is missing required columns: {missing}")

print("Duplicate lab_event_id groups:",
      lab_events_raw.groupBy("lab_event_id").count().filter(F.col("count") > 1).count())
display(lab_events_raw.groupBy("test_code").count().orderBy(F.desc("count")))
display(lab_events_raw.groupBy("result_status").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Read and profile bed transfer events

# COMMAND ----------

TRANSFER_FILE = "bed_transfer_events.csv"
transfer_path = EVENTS_PATH + TRANSFER_FILE

bed_transfers_raw = (
    spark.read.format("csv").option("header", "true")
    .option("inferSchema", "true").option("mode", "PERMISSIVE")
    .load(transfer_path)
)

print("Bed transfer rows:", bed_transfers_raw.count())
print("Bed transfer columns:", bed_transfers_raw.columns)
bed_transfers_raw.printSchema()
display(bed_transfers_raw.limit(10))

required_transfer_columns = [
    "transfer_id", "encounter_id", "patient_id", "from_bed_id",
    "to_bed_id", "transfer_ts", "transfer_reason", "department_id",
    "transfer_status", "recorded_ts"
]
missing = [c for c in required_transfer_columns if c not in bed_transfers_raw.columns]
if missing:
    raise ValueError(f"Bed transfer source is missing required columns: {missing}")

print("Duplicate transfer_id groups:",
      bed_transfers_raw.groupBy("transfer_id").count().filter(F.col("count") > 1).count())
display(bed_transfers_raw.groupBy("transfer_reason").count().orderBy(F.desc("count")))
display(bed_transfers_raw.groupBy("transfer_status").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validate cross-source key coverage and event chronology

# COMMAND ----------

# Referential coverage checks are profiling outputs. Do not drop unmatched rows in Bronze.
encounter_ids = encounters_raw.select("encounter_id").distinct()
patient_ids = patient_seed_raw.select("patient_id").distinct()
department_ids = department_seed_raw.select("department_id").distinct()
bed_ids = bed_seed_raw.select("bed_id").distinct()

def orphan_count(source_df, source_key, reference_df, reference_key):
    return (
        source_df.select(source_key).filter(F.col(source_key).isNotNull()).distinct()
        .join(reference_df, F.col(source_key) == F.col(reference_key), "left_anti")
        .count()
    )

coverage_checks = [
    ("encounters.patient_id -> patient_master", orphan_count(encounters_raw, "patient_id", patient_ids, "patient_id")),
    ("encounters.department_id -> department_master", orphan_count(encounters_raw, "department_id", department_ids, "department_id")),
    ("encounters.bed_id -> bed_master", orphan_count(encounters_raw, "bed_id", bed_ids, "bed_id")),
    ("lab.encounter_id -> encounters", orphan_count(lab_events_raw, "encounter_id", encounter_ids, "encounter_id")),
    ("lab.patient_id -> patient_master", orphan_count(lab_events_raw, "patient_id", patient_ids, "patient_id")),
    ("transfer.encounter_id -> encounters", orphan_count(bed_transfers_raw, "encounter_id", encounter_ids, "encounter_id")),
    ("transfer.patient_id -> patient_master", orphan_count(bed_transfers_raw, "patient_id", patient_ids, "patient_id")),
    ("transfer.from_bed_id -> bed_master", orphan_count(bed_transfers_raw, "from_bed_id", bed_ids, "bed_id")),
    ("transfer.to_bed_id -> bed_master", orphan_count(bed_transfers_raw, "to_bed_id", bed_ids, "bed_id"))
]
display(spark.createDataFrame(coverage_checks, ["relationship", "orphan_distinct_keys"]))

lab_typed = (
    lab_events_raw
    .withColumn("ordered_ts_parsed", F.to_timestamp("ordered_ts"))
    .withColumn("collected_ts_parsed", F.to_timestamp("collected_ts"))
    .withColumn("result_ts_parsed", F.to_timestamp("result_ts"))
)
print("Lab collected before ordered:",
      lab_typed.filter(F.col("ordered_ts_parsed").isNotNull() & F.col("collected_ts_parsed").isNotNull() &
                       (F.col("collected_ts_parsed") < F.col("ordered_ts_parsed"))).count())
print("Lab result before collected:",
      lab_typed.filter(F.col("collected_ts_parsed").isNotNull() & F.col("result_ts_parsed").isNotNull() &
                       (F.col("result_ts_parsed") < F.col("collected_ts_parsed"))).count())

transfer_typed = (
    bed_transfers_raw
    .withColumn("transfer_ts_parsed", F.to_timestamp("transfer_ts"))
    .withColumn("recorded_ts_parsed", F.to_timestamp("recorded_ts"))
)
print("Transfer recorded before transfer event:",
      transfer_typed.filter(F.col("transfer_ts_parsed").isNotNull() & F.col("recorded_ts_parsed").isNotNull() &
                            (F.col("recorded_ts_parsed") < F.col("transfer_ts_parsed"))).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Source profiling summary and reconciliation

# COMMAND ----------

source_summary = [
    ("patient_encounters", encounter_total_rows, encounter_distinct_ids, encounter_key_duplicates.count()),
    ("lab_test_events", lab_events_raw.count(), lab_events_raw.select("lab_event_id").distinct().count(),
     lab_events_raw.groupBy("lab_event_id").count().filter(F.col("count") > 1).count()),
    ("bed_transfer_events", bed_transfers_raw.count(), bed_transfers_raw.select("transfer_id").distinct().count(),
     bed_transfers_raw.groupBy("transfer_id").count().filter(F.col("count") > 1).count()),
    ("patient_master_seed", patient_seed_raw.count(), patient_seed_raw.select("patient_id").distinct().count(),
     patient_seed_raw.groupBy("patient_id").count().filter(F.col("count") > 1).count()),
    ("department_master_seed", department_seed_raw.count(), department_seed_raw.select("department_id").distinct().count(),
     department_seed_raw.groupBy("department_id").count().filter(F.col("count") > 1).count()),
    ("bed_master_seed", bed_seed_raw.count(), bed_seed_raw.select("bed_id").distinct().count(),
     bed_seed_raw.groupBy("bed_id").count().filter(F.col("count") > 1).count())
]

source_summary_df = spark.createDataFrame(
    source_summary,
    ["source_dataset", "source_rows", "distinct_business_keys", "duplicate_key_groups"]
)
display(source_summary_df)

print("Profiling is observation-based. Record actual findings in the team's data-quality register.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Bronze ingestion — patient encounters using Auto Loader
# MAGIC
# MAGIC Auto Loader reads all encounter batch files from the encounter folder. `availableNow=True` processes files currently available and then stops. The Bronze table preserves duplicate and potentially invalid source records for downstream Silver handling.

# COMMAND ----------

encounter_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", ENCOUNTERS_SCHEMA_PATH)
    .option("rescuedDataColumn", "_rescued_data")
    .load(ENCOUNTERS_PATH)
)

encounter_query = (
    encounter_stream.writeStream.format("delta")
    .option("checkpointLocation", ENCOUNTERS_CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(BRONZE_ENCOUNTERS)
)
encounter_query.awaitTermination()

bronze_encounters = spark.table(BRONZE_ENCOUNTERS)
print("Bronze encounter table:", BRONZE_ENCOUNTERS)
print("Bronze encounter rows:", bronze_encounters.count())
print("Distinct encounter IDs:", bronze_encounters.select("encounter_id").distinct().count())
bronze_encounters.printSchema()
display(bronze_encounters.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Bronze ingestion — lab and bed transfer event feeds

# COMMAND ----------

def ingest_autoloader_csv(source_path, target_table, schema_path, checkpoint_path):
    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
    )
    query = (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()
    result_df = spark.table(target_table)
    print(f"{target_table}: {result_df.count()} rows")
    return result_df

bronze_lab_events = ingest_autoloader_csv(
    lab_path, BRONZE_LAB_EVENTS, LAB_SCHEMA_PATH, LAB_CHECKPOINT
)

bronze_bed_transfers = ingest_autoloader_csv(
    transfer_path, BRONZE_BED_TRANSFERS, TRANSFER_SCHEMA_PATH, TRANSFER_CHECKPOINT
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Bronze landing — master seed snapshots
# MAGIC
# MAGIC The three master CSVs are seed extracts supplied for Azure SQL staging. The tables created below are explicitly named `*_seed` to avoid implying that this notebook connected to Azure SQL. In a production implementation, the governed Azure SQL ingestion path should be configured separately.

# COMMAND ----------

def write_seed_snapshot(source_df, target_table):
    (
        source_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    result = spark.table(target_table)
    print(f"{target_table}: {result.count()} rows")
    return result

bronze_patient_seed = write_seed_snapshot(patient_seed_raw, BRONZE_PATIENT_SEED)
bronze_department_seed = write_seed_snapshot(department_seed_raw, BRONZE_DEPARTMENT_SEED)
bronze_bed_seed = write_seed_snapshot(bed_seed_raw, BRONZE_BED_SEED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Final Bronze validation and Day 1 evidence

# COMMAND ----------

bronze_validation = [
    (BRONZE_ENCOUNTERS, spark.table(BRONZE_ENCOUNTERS).count(), encounter_total_rows),
    (BRONZE_LAB_EVENTS, spark.table(BRONZE_LAB_EVENTS).count(), lab_events_raw.count()),
    (BRONZE_BED_TRANSFERS, spark.table(BRONZE_BED_TRANSFERS).count(), bed_transfers_raw.count()),
    (BRONZE_PATIENT_SEED, spark.table(BRONZE_PATIENT_SEED).count(), patient_seed_raw.count()),
    (BRONZE_DEPARTMENT_SEED, spark.table(BRONZE_DEPARTMENT_SEED).count(), department_seed_raw.count()),
    (BRONZE_BED_SEED, spark.table(BRONZE_BED_SEED).count(), bed_seed_raw.count())
]

bronze_validation_df = spark.createDataFrame(
    [(name, int(actual), int(source), "PASS" if actual == source else "CHECK")
     for name, actual, source in bronze_validation],
    ["bronze_table", "bronze_rows", "source_rows", "reconciliation"]
)
display(bronze_validation_df)

print("Day 1 completed: source profiling and Bronze landing.")
print("Do not deduplicate or delete suspect records in Bronze.")
print("Retain screenshots of: source inventory, encounter profiling, DQ/relationship checks, and Bronze reconciliation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Day 1 handoff notes
# MAGIC
# MAGIC Capture the observed results from the notebook in the team's data-quality register:
# MAGIC
# MAGIC - Source inventory and file-level encounter batch counts.
# MAGIC - Key uniqueness, exact duplicates and null profile.
# MAGIC - Category distributions and timestamp chronology checks.
# MAGIC - Foreign-key coverage and orphan key counts.
# MAGIC - Bronze table row-count reconciliation.
# MAGIC - Any ingestion errors, rescued data, or schema changes.
# MAGIC
# MAGIC **Silver-layer work for Day 2:** standardize data types, apply documented business rules, deduplicate using a justified business key and ordering rule, validate references, and write rejected records to a traceable quarantine output. Do not treat every repeated key as an exact duplicate without investigating the records.