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

print("Reference Files")
display(dbutils.fs.ls(REFERENCE_PATH))


# COMMAND ----------

# DBTITLE 1,3. Create Required Folders
folders = [
    "checkpoints",
    "schema_tracking",
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

# DBTITLE 1,5. Read Patient Master Data

patient_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(PATIENT_PATH)
)

print("Patient Master Record Count:", patient_df.count())

display(patient_df)


# COMMAND ----------

# DBTITLE 1,6. Check Patient Master Schema

patient_df.printSchema()


# COMMAND ----------

# DBTITLE 1,7. Check Patient Master Columns

print("Patient Master Columns:")
print(patient_df.columns)


# COMMAND ----------

# DBTITLE 1,8. Check Patient Master Missing Values

patient_missing = patient_df.select([
    count(
        when(
            col(c).isNull() | (trim(col(c)) == ""),
            c
        )
    ).alias(c)
    for c in patient_df.columns
])

display(patient_missing)


# COMMAND ----------

# DBTITLE 1,9. Check Duplicate Patient Records

print("Total Patient Records:", patient_df.count())

print(
    "Distinct Patient IDs:",
    patient_df.select("patient_id").distinct().count()
)

duplicate_patients = (
    patient_df.groupBy("patient_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Patient IDs:")
display(duplicate_patients)


# COMMAND ----------

# DBTITLE 1,10. Check Patient Data Distribution
display(
    patient_df.groupBy("patient_status")
    .count()
)


# COMMAND ----------

# DBTITLE 1,11. Check Patient Master Data Distribution

display(
    patient_df.groupBy("insurance_type")
    .count()
)

display(
    patient_df.groupBy("age_band")
    .count()
)


# COMMAND ----------

# DBTITLE 1,12. Read Department Master Data
department_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(DEPARTMENT_PATH)
)

print("Department Master Record Count:", department_df.count())

display(department_df)


# COMMAND ----------

# DBTITLE 1,13. Check Department Master Schema

department_df.printSchema()


# COMMAND ----------

# DBTITLE 1,14. Check Department Master Columns

print("Department Master Columns:")
print(department_df.columns)


# COMMAND ----------

# DBTITLE 1,15. Check Department Master Missing Values
department_missing = department_df.select([
    count(
        when(
            col(c).isNull() | (trim(col(c)) == ""),
            c
        )
    ).alias(c)
    for c in department_df.columns
])

display(department_missing)


# COMMAND ----------

# DBTITLE 1,16. Check Duplicate Department Records

print("Total Department Records:", department_df.count())

print(
    "Distinct Department IDs:",
    department_df.select("department_id").distinct().count()
)

duplicate_departments = (
    department_df.groupBy("department_id")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_departments)


# COMMAND ----------

# DBTITLE 1,17. Check Department Data Distribution
display(
    department_df.groupBy("service_level")
    .count()
)

display(
    department_df.groupBy("active_flag")
    .count()
)


# COMMAND ----------

# DBTITLE 1,18. Read Bed Master Data

bed_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(BED_PATH)
)

print("Bed Master Record Count:", bed_df.count())

display(bed_df)


# COMMAND ----------

# DBTITLE 1,19. Check Bed Master Schema

bed_df.printSchema()


# COMMAND ----------

# DBTITLE 1,20. Check Bed Master Columns

print("Bed Master Columns:")
print(bed_df.columns)


# COMMAND ----------

# DBTITLE 1,21. Check Bed Master Missing Values

bed_missing = bed_df.select([
    count(
        when(
            col(c).isNull() | (trim(col(c)) == ""),
            c
        )
    ).alias(c)
    for c in bed_df.columns
])

display(bed_missing)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,22. Check Duplicate Bed Records

print("Total Bed Records:", bed_df.count())

print(
    "Distinct Bed IDs:",
    bed_df.select("bed_id").distinct().count()
)

duplicate_beds = (
    bed_df.groupBy("bed_id")
    .count()
    .filter(col("count") > 1)
)

display(duplicate_beds)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,23. Check Bed Data Distribution

display(
    bed_df.groupBy("bed_status")
    .count()
)

display(
    bed_df.groupBy("bed_type")
    .count()
)


# COMMAND ----------

# COMMAND ----------

# DBTITLE 1,24. Source Data Summary

print("PATIENT MASTER COUNT:", patient_df.count())
print("DEPARTMENT MASTER COUNT:", department_df.count())
print("BED MASTER COUNT:", bed_df.count())

print("Source Data Profiling Completed")
