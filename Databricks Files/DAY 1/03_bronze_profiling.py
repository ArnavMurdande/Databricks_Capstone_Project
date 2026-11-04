# Databricks notebook source
# Databricks notebook source

from pyspark.sql.functions import (
    col,
    trim,
    sum,
    when,
    count,
    desc,
    to_timestamp
)


# COMMAND ----------

# DBTITLE 1,2. Configure Catalog and Schema

CATALOG = "capstone_8hospital"
SCHEMA = "bronze_schema"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")


# COMMAND ----------

# DBTITLE 1,3. Read Bronze Tables

ENCOUNTER_TABLE = CATALOG + "." + SCHEMA + ".bronze_patient_encounters"
LAB_TABLE = CATALOG + "." + SCHEMA + ".bronze_lab_test_events"
TRANSFER_TABLE = CATALOG + "." + SCHEMA + ".bronze_bed_transfer_events"

PATIENT_TABLE = CATALOG + "." + SCHEMA + ".bronze_patients"
DEPARTMENT_TABLE = CATALOG + "." + SCHEMA + ".bronze_departments"
BED_TABLE = CATALOG + "." + SCHEMA + ".bronze_beds"

encounters = spark.table(ENCOUNTER_TABLE)
lab_events = spark.table(LAB_TABLE)
transfer_events = spark.table(TRANSFER_TABLE)

encounters.printSchema()
lab_events.printSchema()
transfer_events.printSchema()


# COMMAND ----------

# DBTITLE 1,4. Bronze Row Count Profiling

print("Patient Encounters:", encounters.count())
print("Lab Test Events:", lab_events.count())
print("Bed Transfer Events:", transfer_events.count())

print("Patient Master:", spark.table(PATIENT_TABLE).count())
print("Department Master:", spark.table(DEPARTMENT_TABLE).count())
print("Bed Master:", spark.table(BED_TABLE).count())


# COMMAND ----------

# DBTITLE 1,5. Null and Blank Value Profiling

def profile_nulls(dataframe, table_name):

    total_rows = dataframe.count()

    expressions = [
        sum(
            when(
                col(column_name).isNull()
                | (trim(col(column_name).cast("string")) == ""),
                1
            ).otherwise(0)
        ).alias(column_name)
        for column_name in dataframe.columns
        if column_name not in ["source_file", "ingestion_timestamp"]
    ]

    null_counts = dataframe.agg(*expressions).first().asDict()

    print("Table:", table_name)
    print("Total Rows:", total_rows)

    for column_name, null_count in null_counts.items():
        print(column_name, ":", null_count or 0)


profile_nulls(encounters, "bronze_patient_encounters")
profile_nulls(lab_events, "bronze_lab_test_events")
profile_nulls(transfer_events, "bronze_bed_transfer_events")


# COMMAND ----------

# DBTITLE 1,6. Patient Encounter Key Profiling

display(
    encounters
    .groupBy("encounter_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .orderBy(desc("record_count"))
)

print("Total Encounter Rows:", encounters.count())
print("Distinct Encounter IDs:", encounters.select("encounter_id").distinct().count())
print("Null Encounter IDs:", encounters.filter(col("encounter_id").isNull()).count())


# COMMAND ----------

# DBTITLE 1,7. Lab Test Event Key Profiling

display(
    lab_events
    .groupBy("lab_event_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .orderBy(desc("record_count"))
)

print("Total Lab Event Rows:", lab_events.count())
print("Distinct Lab Event IDs:", lab_events.select("lab_event_id").distinct().count())
print("Null Lab Event IDs:", lab_events.filter(col("lab_event_id").isNull()).count())


# COMMAND ----------

# DBTITLE 1,8. Bed Transfer Event Key Profiling

display(
    transfer_events
    .groupBy("transfer_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
    .orderBy(desc("record_count"))
)

print("Total Transfer Rows:", transfer_events.count())
print("Distinct Transfer IDs:", transfer_events.select("transfer_id").distinct().count())
print("Null Transfer IDs:", transfer_events.filter(col("transfer_id").isNull()).count())


# COMMAND ----------

# DBTITLE 1,9. Categorical Value Profiling

display(
    encounters.groupBy("encounter_type")
    .count()
    .orderBy(desc("count"))
)

display(
    encounters.groupBy("triage_priority")
    .count()
    .orderBy(desc("count"))
)

display(
    encounters.groupBy("encounter_status")
    .count()
    .orderBy(desc("count"))
)

display(
    lab_events.groupBy("test_code")
    .count()
    .orderBy(desc("count"))
)

display(
    lab_events.groupBy("result_status")
    .count()
    .orderBy(desc("count"))
)

display(
    transfer_events.groupBy("transfer_reason")
    .count()
    .orderBy(desc("count"))
)

display(
    transfer_events.groupBy("transfer_status")
    .count()
    .orderBy(desc("count"))
)


# COMMAND ----------

# DBTITLE 1,11. Patient Reference Key Profiling

patients = spark.table(PATIENT_TABLE)

display(
    encounters
    .select("patient_id")
    .filter(col("patient_id").isNotNull())
    .join(
        patients.select("patient_id").dropDuplicates(),
        "patient_id",
        "left_anti"
    )
)


# COMMAND ----------

# DBTITLE 1,12. Department Reference Key Profiling

departments = spark.table(DEPARTMENT_TABLE)

display(
    encounters
    .select("department_id")
    .filter(col("department_id").isNotNull())
    .join(
        departments.select("department_id").dropDuplicates(),
        "department_id",
        "left_anti"
    )
)


# COMMAND ----------

# DBTITLE 1,13. Bed Reference Key Profiling

beds = spark.table(BED_TABLE)

display(
    encounters
    .select("bed_id")
    .filter(col("bed_id").isNotNull())
    .join(
        beds.select("bed_id").dropDuplicates(),
        "bed_id",
        "left_anti"
    )
)

display(
    transfer_events
    .select("from_bed_id")
    .filter(col("from_bed_id").isNotNull())
    .join(
        beds.select("bed_id").dropDuplicates(),
        col("from_bed_id") == col("bed_id"),
        "left_anti"
    )
)

display(
    transfer_events
    .select("to_bed_id")
    .filter(col("to_bed_id").isNotNull())
    .join(
        beds.select("bed_id").dropDuplicates(),
        col("to_bed_id") == col("bed_id"),
        "left_anti"
    )
)


# COMMAND ----------

# DBTITLE 1,14. Source File Profiling

display(
    encounters
    .groupBy("source_file")
    .count()
    .orderBy("source_file")
)


# COMMAND ----------

# DBTITLE 1,15. Bronze Profiling Summary

encounter_total = encounters.count()
encounter_distinct = encounters.select("encounter_id").distinct().count()
encounter_duplicates = encounter_total - encounter_distinct
encounter_nulls = encounters.filter(col("encounter_id").isNull()).count()

encounter_null_counts = {}
for c in encounters.columns:
    if c in ("_rescued_data", "source_file", "ingestion_timestamp"):
        continue
    null_count = encounters.filter(col(c).isNull()).count()
    if null_count > 0:
        encounter_null_counts[c] = null_count

lab_total = lab_events.count()
lab_distinct = lab_events.select("lab_event_id").distinct().count()
lab_duplicates = lab_total - lab_distinct
lab_nulls = lab_events.filter(col("lab_event_id").isNull()).count()

lab_null_counts = {}
for c in lab_events.columns:
    null_count = lab_events.filter(col(c).isNull()).count()
    if null_count > 0:
        lab_null_counts[c] = null_count

transfer_total = transfer_events.count()
transfer_distinct = transfer_events.select("transfer_id").distinct().count()
transfer_duplicates = transfer_total - transfer_distinct
transfer_nulls = transfer_events.filter(col("transfer_id").isNull()).count()

transfer_null_counts = {}
for c in transfer_events.columns:
    null_count = transfer_events.filter(col(c).isNull()).count()
    if null_count > 0:
        transfer_null_counts[c] = null_count
print("\n=== Null Count Per Column ===")

if encounter_null_counts:
    for col_name, cnt in encounter_null_counts.items():
        print(f"  {col_name}: {cnt}")
else:
    print("No null values found")

print("\n=== Duplicate Summary ===")
print(f"Patient Encounters - Total: {encounter_total} | Distinct: {encounter_distinct} | Duplicates: {encounter_duplicates}")
print(f"Lab Test Events    - Total: {lab_total} | Distinct: {lab_distinct} | Duplicates: {lab_duplicates}")
print(f"Bed Transfer Events- Total: {transfer_total} | Distinct: {transfer_distinct} | Duplicates: {transfer_duplicates}")

print("\nBronze Profiling Completed")
