# Databricks notebook source
# Databricks notebook source

# DBTITLE 1,1. Configure Source Paths

CATALOG = "capstone_8hospital"
SCHEMA = "bronze_schema"
VOLUME = "ext_capstone8_vol"

ROOT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

EVENT_PATH = ROOT_PATH + "/event_data"
ENCOUNTER_PATH = ROOT_PATH + "/patient_encounters"

display(dbutils.fs.ls(ROOT_PATH))


# COMMAND ----------

# DBTITLE 1,2. Check Source Folders

print("Event Files")
display(dbutils.fs.ls(EVENT_PATH))

print("Patient Encounter Files")
display(dbutils.fs.ls(ENCOUNTER_PATH))


# COMMAND ----------

# DBTITLE 1,3. Create Required Folders
folders = [
    "checkpoints/bed_transfer_events",
    "checkpoints/lab_test_events",
    "checkpoints/patient_encounters",
    "schema_tracking/bed_transfer_events",
    "schema_tracking/lab_test_events",
    "schema_tracking/patient_encounters",
    "logs",
    "quarantine"
]

for folder in folders:
    dbutils.fs.mkdirs(ROOT_PATH + "/" + folder)

print("Required folders created successfully")


# COMMAND ----------

# DBTITLE 1,4. Import Required Libraries
from pyspark.sql.functions import (
    col,
    count,
    when,
    trim
)

# DBTITLE 1,5. Read Bed Transfer Events Data

bed_transfer_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(EVENT_PATH + "/bed_transfer_events.csv")
)

print("Bed Transfer Events Record Count:", bed_transfer_df.count())

display(bed_transfer_df)


# COMMAND ----------

# DBTITLE 1,6. Check Bed Transfer Events Schema

bed_transfer_df.printSchema()


# COMMAND ----------

# DBTITLE 1,7. Check Bed Transfer Events Columns

print("Bed Transfer Events Columns:")
print(bed_transfer_df.columns)


# COMMAND ----------

# DBTITLE 1,8. Check Bed Transfer Events Missing Values

bed_transfer_missing = bed_transfer_df.select([
    count(
        when(
            col(c).isNull() | (trim(col(c)) == ""),
            c
        )
    ).alias(c)
    for c in bed_transfer_df.columns
])

display(bed_transfer_missing)


# COMMAND ----------

# DBTITLE 1,9. Check Duplicate Bed Transfer Records

print("Total Bed Transfer Records:", bed_transfer_df.count())

print(
    "Distinct Transfer IDs:",
    bed_transfer_df.select("transfer_id").distinct().count()
)

duplicate_bed_transfers = (
    bed_transfer_df.groupBy("transfer_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Transfer IDs:")
display(duplicate_bed_transfers)


# COMMAND ----------

# DBTITLE 1,10. Check Bed Transfer Status Distribution

display(
    bed_transfer_df.groupBy("transfer_status")
    .count()
)


# COMMAND ----------

# DBTITLE 1,11. Check Bed Transfer Reasons

display(
    bed_transfer_df.groupBy("transfer_reason")
    .count()
)


# COMMAND ----------

# DBTITLE 1,12. Read Lab Test Events Data

lab_test_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(EVENT_PATH + "/lab_test_events.csv")
)

print("Lab Test Events Record Count:", lab_test_df.count())

display(lab_test_df)


# COMMAND ----------

# DBTITLE 1,13. Check Lab Test Events Schema

lab_test_df.printSchema()


# COMMAND ----------

# DBTITLE 1,14. Check Lab Test Events Columns

print("Lab Test Events Columns:")
print(lab_test_df.columns)


# COMMAND ----------

# DBTITLE 1,15. Check Lab Test Events Missing Values

lab_test_missing = lab_test_df.select([
    count(
        when(
            col(c).isNull() | (trim(col(c)) == ""),
            c
        )
    ).alias(c)
    for c in lab_test_df.columns
])

display(lab_test_missing)


# COMMAND ----------

# DBTITLE 1,16. Check Duplicate Lab Test Records

print("Total Lab Test Records:", lab_test_df.count())

print(
    "Distinct Lab Event IDs:",
    lab_test_df.select("lab_event_id").distinct().count()
)

duplicate_lab_tests = (
    lab_test_df.groupBy("lab_event_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Lab Event IDs:")
display(duplicate_lab_tests)


# COMMAND ----------

# DBTITLE 1,17. Check Lab Test Result Status

display(
    lab_test_df.groupBy("result_status")
    .count()
)


# COMMAND ----------

# DBTITLE 1,18. Check Lab Test Priority

display(
    lab_test_df.groupBy("priority")
    .count()
)


# COMMAND ----------

# DBTITLE 1,19. Read Patient Encounters Data

encounters_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(ENCOUNTER_PATH)
)

print("Patient Encounters Record Count:", encounters_df.count())

display(encounters_df)


# COMMAND ----------

# DBTITLE 1,20. Check Patient Encounters Schema

encounters_df.printSchema()


# COMMAND ----------

# DBTITLE 1,21. Check Patient Encounters Columns

print("Patient Encounters Columns:")
print(encounters_df.columns)


# COMMAND ----------

# DBTITLE 1,22. Check Patient Encounters Missing Values

encounters_missing = encounters_df.select([
    count(
        when(
            col(c).isNull() | (trim(col(c)) == ""),
            c
        )
    ).alias(c)
    for c in encounters_df.columns
])

display(encounters_missing)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,23. Check Duplicate Patient Encounter Records

print("Total Patient Encounter Records:", encounters_df.count())

print(
    "Distinct Encounter IDs:",
    encounters_df.select("encounter_id").distinct().count()
)

duplicate_encounters = (
    encounters_df.groupBy("encounter_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Encounter IDs:")
display(duplicate_encounters)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,24. Check Patient Encounter Status

display(
    encounters_df.groupBy("encounter_status")
    .count()
)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,25. Check Patient Encounter Types

display(
    encounters_df.groupBy("encounter_type")
    .count()
)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,26. Source Data Summary

print("BED TRANSFER EVENTS COUNT:", bed_transfer_df.count())
print("LAB TEST EVENTS COUNT:", lab_test_df.count())
print("PATIENT ENCOUNTERS COUNT:", encounters_df.count())

print("Source Data Profiling Completed")
