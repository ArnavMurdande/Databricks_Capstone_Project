# Databricks notebook source
# Databricks notebook source

from pyspark.sql.functions import (
    col,
    current_timestamp
)


# COMMAND ----------

# DBTITLE 1,2. Configure Catalog and Schema

CATALOG = "capstone_8hospital"
SCHEMA = "bronze_schema"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")


# COMMAND ----------

# DBTITLE 1,3. Configure Source Paths

ROOT_PATH = "/Volumes/capstone_8hospital/bronze_schema/ext_capstone8_vol/"

PATIENT_ENCOUNTERS_PATH = ROOT_PATH + "patient_encounters"
LAB_TEST_EVENTS_PATH = ROOT_PATH + "event_data/lab_test_events.csv"
BED_TRANSFER_EVENTS_PATH = ROOT_PATH + "event_data/bed_transfer_events.csv"

CHECKPOINT_ROOT = ROOT_PATH + "checkpoints"
SCHEMA_ROOT = ROOT_PATH + "logs/schema_tracking"


# COMMAND ----------

# DBTITLE 1,4. Create Auto Loader Directories

dbutils.fs.mkdirs(CHECKPOINT_ROOT)
dbutils.fs.mkdirs(SCHEMA_ROOT)

print("Checkpoint and schema directories are ready")


# COMMAND ----------

# DBTITLE 1,5. Create Auto Loader Ingestion Function

def ingest_with_autoloader(
    source_path,
    table_name,
    checkpoint_name,
    schema_name
):

    checkpoint_path = CHECKPOINT_ROOT + "/" + checkpoint_name
    schema_path = SCHEMA_ROOT + "/" + schema_name

    print("Starting ingestion:", table_name)

    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .load(source_path)
    )

    bronze_df = (
        source_df
        .withColumn(
            "source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
    )

    query = (
        bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(CATALOG + "." + SCHEMA + "." + table_name)
    )

    query.awaitTermination()

    print("Completed ingestion:", table_name)


# COMMAND ----------

# DBTITLE 1,6. Ingest Patient Encounter Data

ingest_with_autoloader(
    PATIENT_ENCOUNTERS_PATH,
    "bronze_patient_encounters",
    "patient_encounters",
    "patient_encounters"
)


# COMMAND ----------

# DBTITLE 1,7. Ingest Lab Test Events
# DBTITLE 1,7. Ingest Lab Test Events

ingest_with_autoloader(
    ROOT_PATH + "event_data/lab_test_events*.csv",
    "bronze_lab_test_events",
    "lab_test_events",
    "lab_test_events"
)


# COMMAND ----------

# DBTITLE 1,8. Ingest Bed Transfer Events

ingest_with_autoloader(
    ROOT_PATH + "event_data/bed_transfer_events*.csv",
    "bronze_bed_transfer_events",
    "bed_transfer_events",
    "bed_transfer_events"
)

# COMMAND ----------

# DBTITLE 1,9. Display Bronze Patient Encounters

display(
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_patient_encounters"
    )
)


# COMMAND ----------

# DBTITLE 1,10. Display Bronze Lab Test Events

display(
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_lab_test_events"
    )
)


# COMMAND ----------

# DBTITLE 1,11. Display Bronze Bed Transfer Events

display(
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_bed_transfer_events"
    )
)


# COMMAND ----------

# DBTITLE 1,12. Bronze Ingestion Summary

print(
    "Patient Encounters Bronze Count:",
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_patient_encounters"
    ).count()
)

print(
    "Lab Test Events Bronze Count:",
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_lab_test_events"
    ).count()
)

print(
    "Bed Transfer Events Bronze Count:",
    spark.table(
        CATALOG + "." + SCHEMA + ".bronze_bed_transfer_events"
    ).count()
)

print("Bronze ADLS Auto Loader Ingestion Completed")


# COMMAND ----------

