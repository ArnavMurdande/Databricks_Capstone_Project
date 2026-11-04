# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone 08 - Day 3 - Gold layer (dimensions, facts, KPI marts, reconciliation)
# MAGIC
# MAGIC Run the cells **top to bottom, once**, after `02_silver_quality_capstone08` has passed its final validation. It reads **only** the Silver tables (and the same department / patient reference tables Silver used) and writes everything to `capstone_8healthcare.gold_schema`. Same structure as the Capstone 03 Gold notebook.
# MAGIC
# MAGIC **Gold KPIs required by the client:** waiting time, length of stay, bed utilization.
# MAGIC
# MAGIC **Tables this notebook creates**
# MAGIC
# MAGIC | Table | Grain (one row per) | Purpose |
# MAGIC |---|---|---|
# MAGIC | `dim_department` | department_id (+1 UNKNOWN, sk = -1) | Conformed department dimension, hash surrogate key `department_sk` |
# MAGIC | `dim_patient` | patient_token (+1 UNKNOWN, sk = -1) | Conformed patient dimension - **no raw patient_id and no name** (restricted identifiers) |
# MAGIC | `dim_date` | calendar date | Calendar for encounter, status-change and census dates |
# MAGIC | `dim_encounter_status` | status code | Status groups and the flags used by every KPI |
# MAGIC | `fact_encounter_activity` | encounter_id | Current state, waiting time, bill, stay interval and length of stay |
# MAGIC | `fact_status_transitions` | encounter_id + source_sequence | ADT state intervals (admission, discharge, cancellation, reversal) |
# MAGIC | `fact_secondary_events` | event_id | Auxiliary operational events linked to the encounter |
# MAGIC | `mart_encounter_daily` | encounter_date | Network waiting time, length of stay, cancellation rate |
# MAGIC | `mart_department_daily` | encounter_date + department | Waiting time and length of stay by department |
# MAGIC | `mart_bed_occupancy_daily` | census_date + department | **Bed utilization** (occupied bed-hours / available bed-hours) |
# MAGIC | `gold_kpi_definitions` | kpi_id | Business definition + formula of every KPI |
# MAGIC | `gold_reconciliation` | batch_id + check_id | Expected-vs-actual evidence (TC07, TC08) |
# MAGIC | `gold_run_log` | batch_id + table | Inserted / updated rows per run (freshness, monitoring) |
# MAGIC | `gold_benchmark` | run | Before / after OPTIMIZE timings (TC10) |
# MAGIC
# MAGIC **Expected results on your data** (these follow from the Silver numbers)
# MAGIC
# MAGIC | Item | Expected |
# MAGIC |---|---|
# MAGIC | `dim_department` / `dim_patient` | 111 / 151 (110 / 150 + UNKNOWN) |
# MAGIC | `dim_date` | 41 days (2026-07-01 to 2026-08-10) |
# MAGIC | `fact_encounter_activity` | 11,500 (unmatched department / patient = 0) |
# MAGIC | `fact_status_transitions` | 12,020 (11,500 ADMISSION + 520 CDC) |
# MAGIC | `fact_secondary_events` | 310 |
# MAGIC | `mart_encounter_daily` / `mart_department_daily` / `mart_bed_occupancy_daily` | 32 / 3,520 / 4,510 (41 days x 110 departments) |
# MAGIC | Average waiting time (all encounters) | 81.25 min (long-wait rate 63.18 %) |
# MAGIC | Valid stays / average length of stay | 118 / 27.61 days |
# MAGIC | Cancelled encounters / cancellation rate | 185 / 1.61 % |
# MAGIC | Bed utilization (whole period, 10 beds per department assumed) | 2.86 % |
# MAGIC | Replay of the same input | 0 rows inserted, 0 rows updated |

# COMMAND ----------

# =====================================================
# CAPSTONE 08 - DAY 3 : GOLD LAYER CONFIGURATION
# =====================================================
import datetime as dt
import time
import uuid
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# >>> Same names as the Silver notebook - change them there and here together <<<
CATALOG = 'capstone_8healthcare'
BRONZE_SCHEMA = 'encounter_schema'
SILVER_SCHEMA = 'silver_schema'
GOLD_SCHEMA = 'gold_schema'

# True  = drop the Gold tables this notebook creates and rebuild from a blank schema (first run / demo).
# False = incremental run: MERGE only new or changed rows (use this in the Workflows job).
RESET_GOLD = True
RUN_BENCHMARK = True            # Step I: time a KPI query before / after OPTIMIZE
BENCH_RAN = False               # set to True by Step I when the benchmark is recorded
STRICT_EXPECTED_COUNTS = True   # assert the capstone row counts; set False once new source batches arrive

# Job parameters (Databricks Workflows can override these; ignored outside Databricks)
try:
    dbutils.widgets.text('reset_gold', str(RESET_GOLD).lower())
    dbutils.widgets.text('run_benchmark', str(RUN_BENCHMARK).lower())
    RESET_GOLD = dbutils.widgets.get('reset_gold').strip().lower() == 'true'
    RUN_BENCHMARK = dbutils.widgets.get('run_benchmark').strip().lower() == 'true'
except NameError:
    pass

# Business rules (documented assumptions - need client sign-off)
LONG_WAIT_THRESHOLD_MIN = 60        # an encounter waited "long" when wait_minutes > 60
CANCELLED_STATUSES = ['CANCELLED']  # REVERSED is NOT a cancellation (same decision as Silver)
BED_VISIT_TYPES = ['INPATIENT']     # visit types that occupy a bed
BEDS_PER_DEPARTMENT = 10            # NO capacity source is supplied - placeholder capacity per department
INCLUDE_OPEN_STAYS = False          # True = also count IN_PROGRESS inpatient stays as occupying a bed until the census end

# Same identifier salt as Silver (dim_patient tokens must match silver patient_token)
SECRET_SCOPE, SECRET_KEY = 'capstone08', 'id_salt'
try:
    ID_SALT = dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY)
except Exception:
    ID_SALT = 'capstone08-training-salt'
    print('WARNING: secret scope not found - using the training salt (must match Silver).')

# Reference tables (same as Silver)
DEPARTMENT_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.department'
PATIENT_TABLE = f'{CATALOG}.{BRONZE_SCHEMA}.patient'

# Silver source tables (same names as 02_silver_quality_capstone08)
SILVER_ENCOUNTER_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.silver_encounter_events'
SILVER_ENCOUNTER_CURRENT = f'{CATALOG}.{SILVER_SCHEMA}.silver_encounter_current'
SILVER_STATUS_HISTORY = f'{CATALOG}.{SILVER_SCHEMA}.silver_status_history'
SILVER_SECONDARY_TABLE = f'{CATALOG}.{SILVER_SCHEMA}.silver_secondary_events'
SILVER_STATE_INTERVALS = f'{CATALOG}.{SILVER_SCHEMA}.silver_encounter_state_intervals'
SILVER_STAY_INTERVALS = f'{CATALOG}.{SILVER_SCHEMA}.silver_stay_intervals'

# Gold target tables
def gold(name):
    return f'{CATALOG}.{GOLD_SCHEMA}.{name}'

TARGETS = {
    'dim_department': gold('dim_department'),
    'dim_patient': gold('dim_patient'),
    'dim_date': gold('dim_date'),
    'dim_status': gold('dim_encounter_status'),
    'fact_encounter': gold('fact_encounter_activity'),
    'fact_transitions': gold('fact_status_transitions'),
    'fact_secondary': gold('fact_secondary_events'),
    'mart_daily': gold('mart_encounter_daily'),
    'mart_department': gold('mart_department_daily'),
    'mart_beds': gold('mart_bed_occupancy_daily'),
    'run_log': gold('gold_run_log'),
}
KPI_DEFS_TABLE = gold('gold_kpi_definitions')
RECON_TABLE = gold('gold_reconciliation')
BENCH_TABLE = gold('gold_benchmark')
ALL_GOLD_TABLES = list(TARGETS.values()) + [KPI_DEFS_TABLE, RECON_TABLE, BENCH_TABLE]

spark.conf.set('spark.sql.session.timeZone', 'UTC')     # same as Silver: every timestamp is UTC
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}')

if RESET_GOLD:
    for tbl in ALL_GOLD_TABLES + [gold('tmp_' + t.split('.')[-1]) for t in TARGETS.values()]:
        spark.sql(f'DROP TABLE IF EXISTS {tbl}')
    print('Gold tables dropped - starting from a blank schema.')

# Verify the Silver inputs exist and have the expected row counts
expected_inputs = {SILVER_ENCOUNTER_TABLE: 11500, SILVER_ENCOUNTER_CURRENT: 11500, SILVER_STATUS_HISTORY: 12020,
                   SILVER_SECONDARY_TABLE: 310, SILVER_STATE_INTERVALS: 12020, SILVER_STAY_INTERVALS: 11500,
                   DEPARTMENT_TABLE: 110, PATIENT_TABLE: 150}
for table, expected in expected_inputs.items():
    assert spark.catalog.tableExists(table), f'{table} is missing - run the Silver notebook first'
    actual = spark.table(table).count()
    print(table, ':', actual, 'rows', '(OK)' if actual == expected else f'(EXPECTED {expected})')
    if STRICT_EXPECTED_COUNTS:
        assert actual == expected, f'{table} has {actual} rows, expected {expected}'

BATCH_ID = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S') + '_' + uuid.uuid4().hex[:6]
RUN_TS = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
print('Gold configuration verified | batch_id:', BATCH_ID, '| reset:', RESET_GOLD)

# COMMAND ----------

# =====================================================
# KPI DEFINITIONS  (saved as a table - every KPI has a business definition and a formula)
# =====================================================
kpi_rows = [
    # kpi_id, kpi_name, business_definition, formula, grain, gold_column
    ('K01', 'Total encounters', 'Number of unique encounters admitted on the encounter date',
     'COUNT(encounter_id)', 'encounter_date [+ department]', 'mart_encounter_daily / mart_department_daily.total_encounters'),
    ('K02', 'Average waiting time (min)', 'Mean wait_minutes of all encounters (cancelled ones also waited)',
     'SUM(wait_minutes) / total_encounters', 'encounter_date [+ department]', 'avg_wait_minutes'),
    ('K03', 'Long-wait rate %', f'Share of encounters that waited more than {LONG_WAIT_THRESHOLD_MIN} minutes',
     f'100 * SUM(wait_minutes > {LONG_WAIT_THRESHOLD_MIN}) / total_encounters', 'encounter_date [+ department]', 'long_wait_rate_pct'),
    ('K04', 'Valid stays', 'Encounters discharged through a CDC update with discharge >= admission (Silver rule DQI2)',
     'SUM(is_valid_stay)', 'encounter_date [+ department]', 'valid_stays'),
    ('K05', 'Average length of stay (days)', 'Mean (discharge - admission) of valid stays, attributed to the admission date',
     'SUM(length_of_stay_minutes) / valid_stays / 1440', 'encounter_date [+ department]', 'avg_length_of_stay_days'),
    ('K06', 'Bed utilization %', f'Occupied bed-hours of {", ".join(BED_VISIT_TYPES)} stays / available bed-hours ({BEDS_PER_DEPARTMENT} beds x 24 h per department per day)',
     '100 * occupied_bed_hours / available_bed_hours', 'census_date + department', 'mart_bed_occupancy_daily.bed_utilization_pct'),
    ('K07', 'Cancellation rate %', 'Share of encounters whose latest valid status is CANCELLED (REVERSED is not a cancellation)',
     '100 * cancelled_encounters / total_encounters', 'encounter_date [+ department]', 'cancellation_rate_pct'),
    ('K08', 'Bill outlier encounters', 'Encounters whose bill is above the Silver outlier threshold (2,000) - kept, flagged',
     'SUM(is_bill_outlier)', 'encounter_date [+ department]', 'bill_outlier_encounters'),
]
kpi_df = spark.createDataFrame(kpi_rows, 'kpi_id string, kpi_name string, business_definition string, formula string, grain string, gold_column string')
(kpi_df.withColumn('long_wait_threshold_min', F.lit(LONG_WAIT_THRESHOLD_MIN))
       .withColumn('beds_per_department', F.lit(BEDS_PER_DEPARTMENT))
       .write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable(KPI_DEFS_TABLE))
print('KPI definitions saved:', KPI_DEFS_TABLE, '| KPIs:', spark.table(KPI_DEFS_TABLE).count())
display(spark.table(KPI_DEFS_TABLE).orderBy('kpi_id'))

# COMMAND ----------

# =====================================================
# GOLD BUILD FUNCTIONS  (pure DataFrame logic - the load AND the tests use these)
# =====================================================
UNKNOWN_SK = -1
AUDIT_COLS = ['_gold_batch_id', '_gold_inserted_at', '_gold_updated_at']


def with_row_hash(df, exclude=()):
    """sha256 of every business column -> MERGE updates a row only when something really changed."""
    cols = [c for c in df.columns if c not in set(exclude) | set(AUDIT_COLS) | {'row_hash'}]
    return df.withColumn('row_hash', F.sha2(F.concat_ws('||', *[F.coalesce(F.col(c).cast('string'), F.lit('<NULL>')) for c in cols]), 256))


def date_key(col):
    return F.date_format(col, 'yyyyMMdd').cast('int')


def tokenize(col):
    """Identical to Silver: salted SHA-256 of the trimmed identifier."""
    return F.when(col.isNull(), F.lit(None).cast('string')).otherwise(F.sha2(F.concat(F.lit(ID_SALT), col), 256))


# ---------------- dimensions ----------------
def _clean_reference(ref_df, key):
    need = [key, 'display_name', 'region', 'active_flag', 'effective_date', 'risk_tier']
    missing = [c for c in need if c not in ref_df.columns]
    assert not missing, f'reference table is missing columns {missing}'
    w = Window.partitionBy(key).orderBy(F.col('effective_date').desc_nulls_last(), F.col('display_name').asc())
    return (ref_df.select(F.trim(F.col(key)).alias(key), F.trim('display_name').alias('display_name'),
                          F.upper(F.trim('region')).alias('region'), F.upper(F.trim('active_flag')).alias('active_flag'),
                          F.col('effective_date').cast('date').alias('effective_date'), F.upper(F.trim('risk_tier')).alias('risk_tier'))
                  .filter(F.col(key).isNotNull())
                  .withColumn('_rn', F.row_number().over(w)).filter('_rn = 1').drop('_rn'))


def build_dim_department(ref_df):
    """Conformed SCD1 dimension. Surrogate key = xxhash64(business key): deterministic, identical on every rerun."""
    d = _clean_reference(ref_df, 'department_id').withColumnRenamed('display_name', 'department_name')
    d = d.select(F.xxhash64('department_id').alias('department_sk'), '*',
                 (F.col('active_flag') == 'Y').alias('is_active'), F.lit(False).alias('is_unknown_member'))
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_SK).cast('bigint').alias('department_sk'), F.lit('UNKNOWN').alias('department_id'),
        F.lit('Unknown department').alias('department_name'), F.lit('UNKNOWN').alias('region'), F.lit('N').alias('active_flag'),
        F.lit(None).cast('date').alias('effective_date'), F.lit('UNKNOWN').alias('risk_tier'),
        F.lit(False).alias('is_active'), F.lit(True).alias('is_unknown_member'))
    return with_row_hash(d.unionByName(unknown))


def build_dim_patient(ref_df):
    """Restricted: business key = patient_token (same salt as Silver). No raw patient_id and no patient name in Gold."""
    d = _clean_reference(ref_df, 'patient_id')
    d = d.select(tokenize(F.col('patient_id')).alias('patient_token'),
                 F.concat(F.regexp_extract('patient_id', '^([A-Z_]+-)', 1), F.lit('****')).alias('patient_id_masked'),
                 'region', 'active_flag', 'effective_date', 'risk_tier')
    d = d.select(F.xxhash64('patient_token').alias('patient_sk'), '*',
                 (F.col('active_flag') == 'Y').alias('is_active'), F.lit(False).alias('is_unknown_member'))
    unknown = spark.range(1).select(
        F.lit(UNKNOWN_SK).cast('bigint').alias('patient_sk'), F.lit('UNKNOWN').alias('patient_token'),
        F.lit('UNKNOWN').alias('patient_id_masked'), F.lit('UNKNOWN').alias('region'), F.lit('N').alias('active_flag'),
        F.lit(None).cast('date').alias('effective_date'), F.lit('UNKNOWN').alias('risk_tier'),
        F.lit(False).alias('is_active'), F.lit(True).alias('is_unknown_member'))
    return with_row_hash(d.unionByName(unknown))


def build_dim_date(start_date, end_date):
    d = (spark.createDataFrame([(start_date, end_date)], 'start_date date, end_date date')
              .select(F.explode(F.sequence('start_date', 'end_date')).alias('calendar_date')))
    d = d.select(date_key('calendar_date').alias('date_key'), 'calendar_date',
                 F.year('calendar_date').alias('year'), F.quarter('calendar_date').alias('quarter'),
                 F.month('calendar_date').alias('month'), F.date_format('calendar_date', 'MMMM').alias('month_name'),
                 F.weekofyear('calendar_date').alias('week_of_year'), F.dayofmonth('calendar_date').alias('day_of_month'),
                 F.dayofweek('calendar_date').alias('day_of_week'), F.date_format('calendar_date', 'EEEE').alias('day_name'),
                 F.dayofweek('calendar_date').isin(1, 7).alias('is_weekend'))
    return with_row_hash(d)


def build_dim_status():
    rows = [('OPEN', 'IN_PROGRESS', 'Encounter open'),
            ('PENDING', 'IN_PROGRESS', 'Encounter pending'),
            ('COMPLETED', 'COMPLETED', 'Encounter completed (discharge when reached through CDC)'),
            ('FAILED', 'EXCEPTION', 'Encounter failed (source meaning not documented)'),
            ('CANCELLED', 'CANCELLED', 'Encounter cancelled - counted in cancellation rate'),
            ('REVERSED', 'EXCEPTION', 'Reversed (source meaning not documented) - not a cancellation')]
    d = spark.createDataFrame(rows, 'status_code string, status_group string, description string')
    d = (d.withColumn('is_cancelled', F.col('status_code').isin(CANCELLED_STATUSES))
          .withColumn('is_completed', F.col('status_code') == 'COMPLETED'))
    return with_row_hash(d)


def _dim_keys(dim_department, dim_patient):
    dd = dim_department.filter(~F.col('is_unknown_member')).select('department_id', 'department_sk')
    dp = dim_patient.filter(~F.col('is_unknown_member')).select('patient_token', 'patient_sk')
    return dd, dp


# ---------------- facts ----------------
def build_fact_encounter_activity(events, current, stays, secondary, dim_department, dim_patient):
    """Grain: one row per encounter_id (current state + stay interval)."""
    e = events.select('encounter_id', 'event_ts_utc', 'ingest_ts_utc', 'department_id', 'patient_token', 'doctor_token',
                      'visit_type', 'visit_type_was_missing', 'priority', 'priority_is_valid', 'bill_amount', 'wait_minutes',
                      'is_bill_outlier', 'is_wait_outlier', 'source_schema_version',
                      F.col('encounter_status').alias('reported_status'), F.col('_source_file').alias('source_file'))
    c = current.select('encounter_id', 'initial_status', 'current_status', 'status_updated_at', 'last_source_sequence', 'status_source')
    st = stays.select('encounter_id', 'stay_status', 'admit_ts_utc', 'discharge_ts_utc', 'is_valid_stay', 'interval_issue',
                      'length_of_stay_minutes', 'length_of_stay_hours', 'length_of_stay_days')
    s = secondary.groupBy('encounter_id').agg(F.count('*').alias('secondary_event_count'),
                                              F.sum(F.when(F.col('event_type') == 'ALERT', 1).otherwise(0)).alias('alert_event_count'))
    dd, dp = _dim_keys(dim_department, dim_patient)
    f = (e.join(c, 'encounter_id', 'left').join(st, 'encounter_id', 'left').join(s, 'encounter_id', 'left')
          .join(F.broadcast(dd), 'department_id', 'left').join(F.broadcast(dp), 'patient_token', 'left'))
    cur_status = F.coalesce(F.col('current_status'), F.col('reported_status'))
    f = f.select(
        'encounter_id',
        date_key(F.to_date('event_ts_utc')).alias('encounter_date_key'),
        F.to_date('event_ts_utc').alias('encounter_date'),
        F.coalesce('department_sk', F.lit(UNKNOWN_SK)).cast('bigint').alias('department_sk'),
        F.coalesce('patient_sk', F.lit(UNKNOWN_SK)).cast('bigint').alias('patient_sk'),
        'department_id', 'doctor_token', 'visit_type', 'priority',
        F.coalesce('admit_ts_utc', 'event_ts_utc').alias('admit_ts_utc'), 'ingest_ts_utc',
        'reported_status',
        F.coalesce('initial_status', 'reported_status').alias('initial_status'),
        cur_status.alias('current_status'),
        F.coalesce('status_updated_at', 'event_ts_utc').alias('status_updated_at'),
        F.coalesce('last_source_sequence', F.lit(0)).cast('bigint').alias('last_source_sequence'),
        F.coalesce('status_source', F.lit('EVENT')).alias('status_source'),
        'wait_minutes',
        (F.col('wait_minutes') > LONG_WAIT_THRESHOLD_MIN).alias('is_long_wait'),
        'bill_amount',
        cur_status.isin(CANCELLED_STATUSES).alias('is_cancelled'),
        F.col('visit_type').isin(BED_VISIT_TYPES).alias('is_bed_visit'),
        'stay_status', 'discharge_ts_utc',
        F.coalesce('is_valid_stay', F.lit(False)).alias('is_valid_stay'), 'interval_issue',
        'length_of_stay_minutes', 'length_of_stay_hours', 'length_of_stay_days',
        'is_bill_outlier', 'is_wait_outlier', 'visit_type_was_missing', 'priority_is_valid',
        F.coalesce('secondary_event_count', F.lit(0)).cast('int').alias('secondary_event_count'),
        F.coalesce('alert_event_count', F.lit(0)).cast('int').alias('alert_event_count'),
        'source_schema_version', 'source_file')
    return with_row_hash(f)


def build_fact_status_transitions(state_intervals, events, dim_department, dim_patient):
    """Grain: one row per encounter_id + source_sequence (sequence 0 = ADMISSION)."""
    e = events.select('encounter_id', 'event_ts_utc', 'department_id', 'patient_token')
    dd, dp = _dim_keys(dim_department, dim_patient)
    t = (state_intervals.join(e, 'encounter_id', 'left')
         .join(F.broadcast(dd), 'department_id', 'left').join(F.broadcast(dp), 'patient_token', 'left'))
    t = t.select(
        'encounter_id', F.col('source_sequence').cast('bigint').alias('source_sequence'), 'state_order',
        F.col('record_type').alias('transition_type'), 'adt_event_type', 'previous_status', 'new_status', 'change_reason',
        'valid_from_utc', 'valid_to_utc',
        F.to_date('valid_from_utc').alias('effective_date'), date_key(F.to_date('valid_from_utc')).alias('effective_date_key'),
        date_key(F.to_date('event_ts_utc')).alias('encounter_date_key'),
        F.coalesce('department_sk', F.lit(UNKNOWN_SK)).cast('bigint').alias('department_sk'),
        F.coalesce('patient_sk', F.lit(UNKNOWN_SK)).cast('bigint').alias('patient_sk'),
        'is_current', 'state_minutes', 'is_valid_interval', 'is_before_admission', 'is_status_change', 'is_noop',
        ((F.col('record_type') == 'CDC') & F.col('new_status').isin(CANCELLED_STATUSES)).alias('is_cancellation'),
        ((F.unix_timestamp('valid_from_utc') - F.unix_timestamp('event_ts_utc')) / 60).cast('bigint').alias('minutes_after_admission'),
        'source_file')
    return with_row_hash(t)


def build_fact_secondary_events(secondary, events, dim_department, dim_patient):
    """Grain: one row per event_id."""
    e = events.select('encounter_id', 'department_id', 'patient_token')
    dd, dp = _dim_keys(dim_department, dim_patient)
    s = (secondary.join(e, 'encounter_id', 'left').join(F.broadcast(dd), 'department_id', 'left')
                  .join(F.broadcast(dp), 'patient_token', 'left'))
    s = s.select('event_id', 'encounter_id', 'event_ts_utc', F.to_date('event_ts_utc').alias('event_date'),
                 date_key(F.to_date('event_ts_utc')).alias('event_date_key'),
                 F.coalesce('department_sk', F.lit(UNKNOWN_SK)).cast('bigint').alias('department_sk'),
                 F.coalesce('patient_sk', F.lit(UNKNOWN_SK)).cast('bigint').alias('patient_sk'),
                 'event_type', F.col('event_value').cast('decimal(12,2)').alias('event_value'), 'related_entity_id',
                 F.regexp_extract('related_entity_id', '^([A-Z_]+?)(_ID)?-', 1).alias('related_entity_type'),
                 F.col('_source_file').alias('source_file'))
    return with_row_hash(s)


# ---------------- KPI marts ----------------
def kpi_aggregate(fact_df, group_cols):
    """Additive measures first (they reconcile exactly); ratios are derived from them."""
    agg = fact_df.groupBy(*group_cols).agg(
        F.count('*').cast('bigint').alias('total_encounters'),
        F.sum(F.col('is_cancelled').cast('int')).cast('bigint').alias('cancelled_encounters'),
        F.sum(F.when(F.col('current_status') == 'REVERSED', 1).otherwise(0)).cast('bigint').alias('reversed_encounters'),
        F.sum('wait_minutes').cast('bigint').alias('total_wait_minutes'),
        F.max('wait_minutes').alias('max_wait_minutes'),
        F.sum(F.col('is_long_wait').cast('int')).cast('bigint').alias('long_wait_encounters'),
        F.sum(F.col('is_valid_stay').cast('int')).cast('bigint').alias('valid_stays'),
        F.coalesce(F.sum('length_of_stay_minutes'), F.lit(0)).cast('bigint').alias('total_length_of_stay_minutes'),
        F.sum(F.col('is_bed_visit').cast('int')).cast('bigint').alias('bed_visit_encounters'),
        F.sum('bill_amount').cast('decimal(18,2)').alias('total_bill_amount'),
        F.sum(F.col('is_bill_outlier').cast('int')).cast('bigint').alias('bill_outlier_encounters'),
        F.sum('secondary_event_count').cast('bigint').alias('secondary_event_count'))

    def pct(n, d):
        return F.when(F.col(d) > 0, F.round(100.0 * F.col(n) / F.col(d), 2))
    return (agg.withColumn('avg_wait_minutes', F.when(F.col('total_encounters') > 0,
                                                       F.round(F.col('total_wait_minutes') / F.col('total_encounters'), 2)))
               .withColumn('long_wait_rate_pct', pct('long_wait_encounters', 'total_encounters'))
               .withColumn('cancellation_rate_pct', pct('cancelled_encounters', 'total_encounters'))
               .withColumn('avg_length_of_stay_days', F.when(F.col('valid_stays') > 0,
                           F.round(F.col('total_length_of_stay_minutes') / F.col('valid_stays') / 1440, 2))))


def build_encounter_marts(fact_df, dim_department, dim_date, dates=None):
    """Recompute mart_encounter_daily and mart_department_daily for the given encounter dates (None = every date)."""
    f = fact_df if dates is None else fact_df.filter(F.col('encounter_date').isin(list(dates)))
    dd = dim_date.select(F.col('date_key').alias('encounter_date_key'), 'day_name', 'is_weekend')
    daily = kpi_aggregate(f, ['encounter_date', 'encounter_date_key']).join(dd, 'encounter_date_key', 'left')
    dept = (kpi_aggregate(f, ['encounter_date', 'encounter_date_key', 'department_sk'])
            .join(dim_department.select('department_sk', 'department_id', 'department_name',
                                        F.col('region').alias('department_region')), 'department_sk', 'left'))
    return with_row_hash(daily), with_row_hash(dept)


def build_bed_occupancy(fact_df, dim_department, dim_date, census_end_ts):
    """Grain: census_date x department (every department, every calendar date).
    A stay occupies a bed from admit_ts_utc to discharge_ts_utc (or census_end_ts for open stays when INCLUDE_OPEN_STAYS)."""
    stay_end = F.when(F.col('is_valid_stay'), F.col('discharge_ts_utc'))
    if INCLUDE_OPEN_STAYS:
        stay_end = stay_end.when(F.col('stay_status') == 'IN_PROGRESS', F.lit(census_end_ts).cast('timestamp'))
    stays = (fact_df.filter(F.col('is_bed_visit'))
             .select('encounter_id', 'department_sk', F.col('admit_ts_utc').alias('s'), stay_end.alias('e'))
             .filter(F.col('e').isNotNull() & (F.col('e') >= F.col('s'))))
    days = stays.withColumn('census_date', F.explode(F.sequence(F.to_date('s'), F.to_date('e'))))
    day_start = F.col('census_date').cast('timestamp')
    day_end = F.date_add('census_date', 1).cast('timestamp')
    overlap_sec = F.unix_timestamp(F.least(F.col('e'), day_end)) - F.unix_timestamp(F.greatest(F.col('s'), day_start))
    occ = (days.withColumn('_sec', overlap_sec)
               .groupBy('census_date', 'department_sk')
               .agg(F.round(F.sum(F.when(F.col('_sec') > 0, F.col('_sec')).otherwise(0)) / 3600.0, 4).alias('occupied_bed_hours'),
                    F.countDistinct('encounter_id').alias('stays_in_bed'),
                    F.sum(F.when(F.to_date('s') == F.col('census_date'), 1).otherwise(0)).alias('bed_admissions'),
                    F.sum(F.when(F.to_date('e') == F.col('census_date'), 1).otherwise(0)).alias('bed_discharges')))
    grid = (dim_date.select(F.col('calendar_date').alias('census_date'), F.col('date_key').alias('census_date_key'))
            .crossJoin(dim_department.filter(~F.col('is_unknown_member'))
                       .select('department_sk', 'department_id', 'department_name', F.col('region').alias('department_region'))))
    m = grid.join(occ, ['census_date', 'department_sk'], 'left')
    m = m.select('census_date', 'census_date_key', 'department_sk', 'department_id', 'department_name', 'department_region',
                 F.coalesce('occupied_bed_hours', F.lit(0.0)).cast('double').alias('occupied_bed_hours'),
                 F.lit(float(BEDS_PER_DEPARTMENT * 24)).alias('available_bed_hours'),
                 F.lit(BEDS_PER_DEPARTMENT).alias('beds_available'),
                 F.coalesce('stays_in_bed', F.lit(0)).cast('bigint').alias('stays_in_bed'),
                 F.coalesce('bed_admissions', F.lit(0)).cast('bigint').alias('bed_admissions'),
                 F.coalesce('bed_discharges', F.lit(0)).cast('bigint').alias('bed_discharges'))
    m = m.withColumn('bed_utilization_pct', F.round(100.0 * F.col('occupied_bed_hours') / F.col('available_bed_hours'), 2))
    return with_row_hash(m)


print('Gold build functions defined.')

# COMMAND ----------

# =====================================================
# DELTA LOAD HELPERS  (idempotent MERGE - Databricks only)
# =====================================================
def merge_upsert(target, src_df, key_cols, batch_id, run_ts, delete_dates=None, date_col='encounter_date'):
    """Insert new keys, update a key only when its row_hash changed, leave unchanged rows alone.
    delete_dates (marts only): keys inside these dates that no longer exist in the source are deleted.
    Returns {'inserted': n, 'updated': n} for this batch."""
    run_ts_lit = F.lit(run_ts).cast('timestamp')
    src = (src_df.withColumn('_gold_batch_id', F.lit(batch_id))
                 .withColumn('_gold_inserted_at', run_ts_lit)
                 .withColumn('_gold_updated_at', run_ts_lit))
    if not spark.catalog.tableExists(target):
        src.limit(0).write.format('delta').saveAsTable(target)
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')")
    view = '_src_' + target.split('.')[-1]
    src.createOrReplaceTempView(view)
    cols = src.columns
    on = ' AND '.join(f't.`{k}` = s.`{k}`' for k in key_cols)
    upd = ', '.join(f't.`{c}` = s.`{c}`' for c in cols if c != '_gold_inserted_at')
    ins_cols = ', '.join(f'`{c}`' for c in cols)
    ins_vals = ', '.join(f's.`{c}`' for c in cols)
    sql = (f'MERGE INTO {target} t USING {view} s ON {on} '
           f'WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET {upd} '
           f'WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})')
    if delete_dates:
        in_list = ', '.join(f"DATE'{d}'" for d in sorted(delete_dates))
        sql += f' WHEN NOT MATCHED BY SOURCE AND t.`{date_col}` IN ({in_list}) THEN DELETE'
    spark.sql(sql)
    changed = spark.table(target).filter(F.col('_gold_batch_id') == batch_id)
    return {'inserted': changed.filter(F.col('_gold_inserted_at') == run_ts_lit).count(),
            'updated': changed.filter(F.col('_gold_inserted_at') < run_ts_lit).count()}


def load_sources():
    return {'events': spark.table(SILVER_ENCOUNTER_TABLE), 'current': spark.table(SILVER_ENCOUNTER_CURRENT),
            'history': spark.table(SILVER_STATUS_HISTORY), 'secondary': spark.table(SILVER_SECONDARY_TABLE),
            'intervals': spark.table(SILVER_STATE_INTERVALS), 'stays': spark.table(SILVER_STAY_INTERVALS),
            'department': spark.table(DEPARTMENT_TABLE), 'patient': spark.table(PATIENT_TABLE)}


def run_gold_load(targets, src, batch_id, run_ts, full_refresh_marts=False):
    """The whole Gold load. Re-running it on unchanged input inserts and updates nothing."""
    log = []

    def step(name, df, keys, delete_dates=None, date_col='encounter_date'):
        m = merge_upsert(targets[name], df, keys, batch_id, run_ts, delete_dates, date_col)
        log.append((batch_id, run_ts, targets[name].split('.')[-1], m['inserted'], m['updated'],
                    spark.table(targets[name]).count()))
        return m

    # 1. dimensions
    m_dep = step('dim_department', build_dim_department(src['department']), ['department_id'])
    m_pat = step('dim_patient', build_dim_patient(src['patient']), ['patient_token'])
    bounds = (src['events'].select(F.to_date('event_ts_utc').alias('d'))
              .unionByName(src['intervals'].select(F.to_date('valid_from_utc').alias('d')))
              .agg(F.min('d').alias('lo'), F.max('d').alias('hi')).first())
    step('dim_date', build_dim_date(bounds.lo, bounds.hi), ['date_key'])
    step('dim_status', build_dim_status(), ['status_code'])
    dim_department, dim_patient = spark.table(targets['dim_department']), spark.table(targets['dim_patient'])
    dim_date = spark.table(targets['dim_date'])

    # 2. facts
    step('fact_encounter', build_fact_encounter_activity(src['events'], src['current'], src['stays'], src['secondary'],
                                                         dim_department, dim_patient), ['encounter_id'])
    step('fact_transitions', build_fact_status_transitions(src['intervals'], src['events'], dim_department, dim_patient),
         ['encounter_id', 'source_sequence'])
    step('fact_secondary', build_fact_secondary_events(src['secondary'], src['events'], dim_department, dim_patient), ['event_id'])

    # 3. encounter marts - recompute only the encounter dates touched by this batch (all if a dimension changed)
    fact = spark.table(targets['fact_encounter'])
    dims_changed = m_dep['updated'] + m_pat['updated'] > 0
    if full_refresh_marts or dims_changed or not spark.catalog.tableExists(targets['mart_daily']):
        dates = [r[0] for r in fact.select('encounter_date').distinct().collect()]
    else:
        dates = [r[0] for r in fact.filter(F.col('_gold_batch_id') == batch_id).select('encounter_date').distinct().collect()]
    if dates:
        daily, dept = build_encounter_marts(fact, dim_department, dim_date, dates)
        step('mart_daily', daily, ['encounter_date'], dates)
        step('mart_department', dept, ['encounter_date', 'department_sk'], dates)
    else:
        for name in ['mart_daily', 'mart_department']:
            log.append((batch_id, run_ts, targets[name].split('.')[-1], 0, 0, spark.table(targets[name]).count()))

    # 4. bed occupancy - a stay spans many days, so it is recomputed in full; the hash MERGE still touches only changed rows
    census_end = src['intervals'].agg(F.max('valid_from_utc')).first()[0]
    beds = build_bed_occupancy(fact, dim_department, dim_date, census_end)
    all_census_dates = [r[0] for r in dim_date.select('calendar_date').collect()]
    step('mart_beds', beds, ['census_date', 'department_sk'], all_census_dates, 'census_date')

    # 5. run log (freshness + monitoring)
    log_df = spark.createDataFrame(log, 'batch_id string, run_ts timestamp, table_name string, rows_inserted long, '
                                        'rows_updated long, target_rows long')
    log_df = log_df.withColumn('affected_encounter_dates', F.lit(len(dates)))
    log_df.write.format('delta').mode('append').saveAsTable(targets['run_log'])
    return log_df


print('Load helpers defined.')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step A - Run the Gold load (dimensions -> facts -> marts)

# COMMAND ----------

sources = load_sources()
run_metrics = run_gold_load(TARGETS, sources, BATCH_ID, RUN_TS)
display(run_metrics)

print('Gold table row counts')
for key, name in TARGETS.items():
    print(f'{name.split(".")[-1]:28s}', spark.table(name).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step B - Dimensions: keys, unknown member, join coverage (TC07) and restricted identifiers

# COMMAND ----------

dim_department = spark.table(TARGETS['dim_department'])
dim_patient = spark.table(TARGETS['dim_patient'])
dim_date = spark.table(TARGETS['dim_date'])
fact = spark.table(TARGETS['fact_encounter'])

for name, d, sk in [('dim_department', dim_department, 'department_sk'), ('dim_patient', dim_patient, 'patient_sk')]:
    n, n_sk = d.count(), d.select(sk).distinct().count()
    print(f'{name}: rows {n} | distinct {sk} {n_sk} | unknown member rows {d.filter("is_unknown_member").count()}')
    assert n == n_sk, f'{name}: surrogate key is not unique'

print('dim_date:', dim_date.agg(F.min('calendar_date'), F.max('calendar_date'), F.count('*')).first())

# TC07 - every fact row must find its dimension row (UNKNOWN = -1 means an unmatched business key)
unmatched = fact.agg(F.sum((F.col('department_sk') == UNKNOWN_SK).cast('int')).alias('unmatched_department'),
                     F.sum((F.col('patient_sk') == UNKNOWN_SK).cast('int')).alias('unmatched_patient')).first()
print('Unmatched department keys:', unmatched.unmatched_department, '| unmatched patient keys:', unmatched.unmatched_patient)
print('(an unmatched patient would mean the Gold salt differs from the Silver salt)')

print('dim_patient columns (no raw patient_id, no name):', dim_patient.columns)
display(dim_patient.filter('NOT is_unknown_member').orderBy('patient_id_masked').limit(5))
display(dim_department.orderBy('department_id').limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step C - Facts: grain, status mix, stays and one encounter traced Silver -> Gold

# COMMAND ----------

fact = spark.table(TARGETS['fact_encounter'])
trans = spark.table(TARGETS['fact_transitions'])
sec_f = spark.table(TARGETS['fact_secondary'])

print('fact_encounter_activity rows:', fact.count(), '| distinct encounter_id:', fact.select('encounter_id').distinct().count())
print('fact_status_transitions rows:', trans.count(), '| distinct (encounter_id, source_sequence):',
      trans.select('encounter_id', 'source_sequence').distinct().count())
print('fact_secondary_events rows:', sec_f.count(), '| distinct event_id:', sec_f.select('event_id').distinct().count())

print('Current status mix (fact) - must equal silver_encounter_current')
display(fact.groupBy('current_status', 'is_cancelled').count().orderBy('current_status'))
print('Stay status mix')
display(fact.groupBy('stay_status', 'is_valid_stay').count().orderBy('stay_status'))
print('ADT transitions')
display(trans.groupBy('transition_type', 'adt_event_type', 'is_status_change').count().orderBy('transition_type', 'adt_event_type'))

fid = 'ENCOUNTER-0000018'     # OPEN -> COMPLETED through CDC (a discharge)
print(f'Trace {fid}: Silver current -> Silver stay -> Gold fact -> Gold transitions')
display(spark.table(SILVER_ENCOUNTER_CURRENT).filter(F.col('encounter_id') == fid))
display(spark.table(SILVER_STAY_INTERVALS).filter(F.col('encounter_id') == fid))
display(fact.filter(F.col('encounter_id') == fid).select('encounter_id', 'encounter_date', 'department_sk', 'patient_sk', 'visit_type',
        'reported_status', 'current_status', 'wait_minutes', 'stay_status', 'admit_ts_utc', 'discharge_ts_utc', 'length_of_stay_days'))
display(trans.filter(F.col('encounter_id') == fid).orderBy('state_order'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step D - KPI marts: waiting time, length of stay, bed utilization

# COMMAND ----------

mart_daily = spark.table(TARGETS['mart_daily'])
mart_dept = spark.table(TARGETS['mart_department'])
mart_beds = spark.table(TARGETS['mart_beds'])
print('mart_encounter_daily:', mart_daily.count(), '| mart_department_daily:', mart_dept.count(),
      '| mart_bed_occupancy_daily:', mart_beds.count())

print('Daily network KPIs')
display(mart_daily.select('encounter_date', 'day_name', 'total_encounters', 'avg_wait_minutes', 'long_wait_rate_pct',
                          'valid_stays', 'avg_length_of_stay_days', 'cancelled_encounters', 'cancellation_rate_pct')
                  .orderBy('encounter_date'))

print('Whole-period KPIs (re-derived from the additive measures)')
overall = mart_daily.agg(F.sum('total_encounters').alias('encounters'), F.sum('total_wait_minutes').alias('wait_min'),
                         F.sum('long_wait_encounters').alias('long_waits'), F.sum('valid_stays').alias('valid_stays'),
                         F.sum('total_length_of_stay_minutes').alias('los_min'), F.sum('cancelled_encounters').alias('cancelled'))
overall = (overall.withColumn('avg_wait_minutes', F.round(F.col('wait_min') / F.col('encounters'), 2))
                  .withColumn('long_wait_rate_pct', F.round(100 * F.col('long_waits') / F.col('encounters'), 2))
                  .withColumn('avg_length_of_stay_days', F.round(F.col('los_min') / F.col('valid_stays') / 1440, 2))
                  .withColumn('cancellation_rate_pct', F.round(100 * F.col('cancelled') / F.col('encounters'), 2)))
display(overall)

print('Bed utilization - network by census date')
display(mart_beds.groupBy('census_date')
        .agg(F.round(F.sum('occupied_bed_hours'), 2).alias('occupied_bed_hours'), F.sum('available_bed_hours').alias('available_bed_hours'),
             F.sum('stays_in_bed').alias('stays_in_bed'))
        .withColumn('bed_utilization_pct', F.round(100 * F.col('occupied_bed_hours') / F.col('available_bed_hours'), 2))
        .orderBy('census_date'))

print('Departments with the longest average wait (whole period)')
display(mart_dept.groupBy('department_id', 'department_name')
        .agg(F.sum('total_encounters').alias('encounters'), F.sum('total_wait_minutes').alias('wait_min'))
        .withColumn('avg_wait_minutes', F.round(F.col('wait_min') / F.col('encounters'), 2))
        .orderBy(F.col('avg_wait_minutes').desc(), 'department_id').limit(10))

print('Departments with the highest bed utilization (whole period)')
display(mart_beds.groupBy('department_id', 'department_name')
        .agg(F.round(F.sum('occupied_bed_hours'), 2).alias('occupied'), F.sum('available_bed_hours').alias('available'))
        .withColumn('bed_utilization_pct', F.round(100 * F.col('occupied') / F.col('available'), 2))
        .orderBy(F.col('bed_utilization_pct').desc(), 'department_id').limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step E - Reconciliation: Silver vs Gold, expected vs actual (TC07, TC08)

# COMMAND ----------

# Expected values are computed INDEPENDENTLY from Silver with plain SQL (not with the Gold functions).
LW = LONG_WAIT_THRESHOLD_MIN
cancel_list = ', '.join(f"'{s}'" for s in CANCELLED_STATUSES)
bed_list = ', '.join(f"'{v}'" for v in BED_VISIT_TYPES)
silver_flat = f"""
    SELECT e.encounter_id, to_date(e.event_ts_utc) AS encounter_date, e.department_id, e.visit_type, e.bill_amount, e.wait_minutes,
           c.current_status, c.current_status IN ({cancel_list}) AS cancelled,
           s.is_valid_stay, s.length_of_stay_minutes, s.admit_ts_utc, s.discharge_ts_utc
    FROM {SILVER_ENCOUNTER_TABLE} e
    JOIN {SILVER_ENCOUNTER_CURRENT} c ON e.encounter_id = c.encounter_id
    JOIN {SILVER_STAY_INTERVALS} s ON e.encounter_id = s.encounter_id"""
exp = spark.sql(f"""
    SELECT COUNT(*) AS encounters, SUM(CAST(cancelled AS INT)) AS cancelled, SUM(wait_minutes) AS wait,
           SUM(CASE WHEN wait_minutes > {LW} THEN 1 ELSE 0 END) AS long_waits,
           SUM(CAST(is_valid_stay AS INT)) AS valid_stays, SUM(COALESCE(length_of_stay_minutes, 0)) AS los_min,
           CAST(SUM(fare) AS DECIMAL(18,2)) AS bill,
           ROUND(SUM(CASE WHEN is_valid_stay AND visit_type IN ({bed_list}) THEN length_of_stay_minutes ELSE 0 END) / 60.0, 2) AS bed_hours
    FROM (SELECT *, bill_amount AS fare FROM ({silver_flat}))""").first()

fact = spark.table(TARGETS['fact_encounter'])
trans = spark.table(TARGETS['fact_transitions'])
mart_daily, mart_dept, mart_beds = (spark.table(TARGETS[k]) for k in ['mart_daily', 'mart_department', 'mart_beds'])
dim_department, dim_patient, dim_date = (spark.table(TARGETS[k]) for k in ['dim_department', 'dim_patient', 'dim_date'])
si = spark.table(SILVER_STATE_INTERVALS)

act = fact.agg(F.count('*').alias('encounters'), F.sum(F.col('is_cancelled').cast('int')).alias('cancelled'),
               F.sum('wait_minutes').alias('wait'), F.sum(F.col('is_long_wait').cast('int')).alias('long_waits'),
               F.sum(F.col('is_valid_stay').cast('int')).alias('valid_stays'),
               F.coalesce(F.sum('length_of_stay_minutes'), F.lit(0)).alias('los_min'),
               F.sum('bill_amount').cast('decimal(18,2)').alias('bill')).first()


def mart_sums(m):
    return m.agg(*[F.sum(c).alias(c) for c in ['total_encounters', 'cancelled_encounters', 'total_wait_minutes',
                                               'long_wait_encounters', 'valid_stays', 'total_length_of_stay_minutes']],
                 F.sum('total_bill_amount').cast('decimal(18,2)').alias('bill')).first()


md_, mdep_ = mart_sums(mart_daily), mart_sums(mart_dept)
bed_hours = round(mart_beds.agg(F.sum('occupied_bed_hours')).first()[0], 2)
n_departments = dim_department.filter('NOT is_unknown_member').count()

# TC08 - independent per-date KPI query vs mart_encounter_daily (additive measures exact, ratios to 0.01)
indep = spark.sql(f"""
    SELECT encounter_date, COUNT(*) AS total_encounters, SUM(CAST(cancelled AS INT)) AS cancelled_encounters,
           SUM(CAST(is_valid_stay AS INT)) AS valid_stays,
           ROUND(SUM(wait_minutes) / COUNT(*), 2) AS avg_wait_minutes,
           ROUND(100.0 * SUM(CAST(cancelled AS INT)) / COUNT(*), 2) AS cancellation_rate_pct,
           ROUND(SUM(COALESCE(length_of_stay_minutes, 0)) / NULLIF(SUM(CAST(is_valid_stay AS INT)), 0) / 1440, 2) AS avg_length_of_stay_days
    FROM ({silver_flat}) GROUP BY encounter_date""")
cmp = indep.alias('i').join(mart_daily.alias('m'), 'encounter_date', 'full_outer')
date_mismatch = cmp.filter(
    F.col('m.total_encounters').isNull() | F.col('i.total_encounters').isNull()
    | (F.col('i.total_encounters') != F.col('m.total_encounters')) | (F.col('i.cancelled_encounters') != F.col('m.cancelled_encounters'))
    | (F.col('i.valid_stays') != F.col('m.valid_stays'))
    | (F.abs(F.col('i.avg_wait_minutes') - F.col('m.avg_wait_minutes')) > 0.01)
    | (F.abs(F.col('i.cancellation_rate_pct') - F.col('m.cancellation_rate_pct')) > 0.01)
    | (F.coalesce(F.abs(F.col('i.avg_length_of_stay_days') - F.col('m.avg_length_of_stay_days')), F.lit(0)) > 0.01)
    | (F.col('i.avg_length_of_stay_days').isNull() != F.col('m.avg_length_of_stay_days').isNull())).count()

# TC08 - independent per-department waiting time vs mart_department_daily
indep_dep = spark.sql(f"""
    SELECT encounter_date, department_id, COUNT(*) AS total_encounters, SUM(wait_minutes) AS total_wait_minutes
    FROM ({silver_flat}) GROUP BY encounter_date, department_id""")
dep_mismatch = (indep_dep.alias('i').join(mart_dept.alias('m'), ['encounter_date', 'department_id'], 'full_outer')
                .filter(F.col('m.total_encounters').isNull() | F.col('i.total_encounters').isNull()
                        | (F.col('i.total_encounters') != F.col('m.total_encounters'))
                        | (F.col('i.total_wait_minutes') != F.col('m.total_wait_minutes'))).count())

fact_dates = (fact.select(F.col('encounter_date_key').alias('date_key'))
              .union(trans.select(F.col('effective_date_key').alias('date_key'))).distinct())
missing_dates = fact_dates.join(dim_date, 'date_key', 'left_anti').count()
unknown_status = fact.select(F.col('current_status').alias('status_code')).distinct().join(
    spark.table(TARGETS['dim_status']), 'status_code', 'left_anti').count()
silver_tokens = spark.table(SILVER_ENCOUNTER_TABLE).select('patient_token').distinct()
tokens_not_in_dim = silver_tokens.join(dim_patient.select('patient_token'), 'patient_token', 'left_anti').count()
raw_id_cols = [f'{t.split(".")[-1]}.{c}' for t in TARGETS.values() for c in spark.table(t).columns if c in ('patient_id', 'doctor_id')]

checks = [
    # check_id, test_id, description, expected, actual
    ('R01', 'TC08', 'fact rows = silver_encounter_current rows', spark.table(SILVER_ENCOUNTER_CURRENT).count(), act.encounters),
    ('R02', 'TC08', 'fact rows = silver_encounter_events rows', spark.table(SILVER_ENCOUNTER_TABLE).count(), act.encounters),
    ('R03', 'TC03', 'fact encounter_id is unique', act.encounters, fact.select('encounter_id').distinct().count()),
    ('R04', 'TC08', 'sum wait minutes Silver = Gold', exp.wait, act.wait),
    ('R05', 'TC08', 'sum bill Silver = Gold', str(exp.bill), str(act.bill)),
    ('R06', 'TC08', 'cancelled encounters Silver = Gold', exp.cancelled, act.cancelled),
    ('R07', 'TC08', 'long waits Silver = Gold', exp.long_waits, act.long_waits),
    ('R08', 'TC08', 'valid stays Silver = Gold', exp.valid_stays, act.valid_stays),
    ('R09', 'TC08', 'sum length of stay minutes Silver = Gold', exp.los_min, act.los_min),
    ('R10', 'TC07', 'fact rows with unmatched department (sk = -1)', 0, fact.filter(F.col('department_sk') == UNKNOWN_SK).count()),
    ('R11', 'TC07', 'fact rows with unmatched patient (sk = -1)', 0, fact.filter(F.col('patient_sk') == UNKNOWN_SK).count()),
    ('R12', 'TC07', 'dim_department business rows = reference rows', spark.table(DEPARTMENT_TABLE).count(), dim_department.filter('NOT is_unknown_member').count()),
    ('R13', 'TC07', 'dim_patient business rows = reference rows', spark.table(PATIENT_TABLE).count(), dim_patient.filter('NOT is_unknown_member').count()),
    ('R14', 'TC07', 'Silver patient tokens missing from dim_patient (salt mismatch)', 0, tokens_not_in_dim),
    ('R15', 'TC07', 'fact / transition dates missing from dim_date', 0, missing_dates),
    ('R16', 'TC07', 'fact statuses missing from dim_encounter_status', 0, unknown_status),
    ('R17', 'TC04', 'transitions rows = silver state interval rows', si.count(), trans.count()),
    ('R18', 'TC04', 'CDC transitions = silver CDC interval rows', si.filter("record_type = 'CDC'").count(), trans.filter("transition_type = 'CDC'").count()),
    ('R19', 'TC04', 'transitions marked current = encounters', act.encounters, trans.filter('is_current').count()),
    ('R20', 'TC08', 'fact_secondary_events = silver_secondary_events', spark.table(SILVER_SECONDARY_TABLE).count(), spark.table(TARGETS['fact_secondary']).count()),
    ('R21', 'TC08', 'mart_encounter_daily totals = fact (encounters|cancelled|wait|valid stays|LOS min)',
     f'{act.encounters}|{act.cancelled}|{act.wait}|{act.valid_stays}|{act.los_min}',
     f'{md_.total_encounters}|{md_.cancelled_encounters}|{md_.total_wait_minutes}|{md_.valid_stays}|{md_.total_length_of_stay_minutes}'),
    ('R22', 'TC08', 'mart_encounter_daily bill = fact', str(act.bill), str(md_.bill)),
    ('R23', 'TC08', 'mart_department_daily totals = fact (encounters|cancelled|wait|valid stays|LOS min)',
     f'{act.encounters}|{act.cancelled}|{act.wait}|{act.valid_stays}|{act.los_min}',
     f'{mdep_.total_encounters}|{mdep_.cancelled_encounters}|{mdep_.total_wait_minutes}|{mdep_.valid_stays}|{mdep_.total_length_of_stay_minutes}'),
    ('R24', 'TC08', 'mart_encounter_daily one row per encounter_date', mart_daily.count(), mart_daily.select('encounter_date').distinct().count()),
    ('R25', 'TC08', 'mart_department_daily one row per date + department', mart_dept.count(), mart_dept.select('encounter_date', 'department_sk').distinct().count()),
    ('R26', 'TC08', 'mart_bed_occupancy_daily one row per census date + department', mart_beds.count(), mart_beds.select('census_date', 'department_sk').distinct().count()),
    ('R27', 'TC08', 'mart_bed_occupancy_daily rows = dates x departments', dim_date.count() * n_departments, mart_beds.count()),
    ('R28', 'TC08', 'independent daily KPI query vs mart_encounter_daily: mismatching dates', 0, date_mismatch),
    ('R29', 'TC08', 'independent department wait query vs mart_department_daily: mismatching rows', 0, dep_mismatch),
    ('R30', 'TC08', 'mart_bed_occupancy_daily bed-hours = sum of bed-stay LOS from Silver (closed stays inside the census window)',
     '%.2f' % exp.bed_hours if not INCLUDE_OPEN_STAYS else 'n/a', '%.2f' % bed_hours if not INCLUDE_OPEN_STAYS else 'n/a'),
    ('R31', 'DQ15', 'Gold tables with raw patient_id / doctor_id columns', '[]', str(raw_id_cols)),
]
if STRICT_EXPECTED_COUNTS:   # capstone constants (from the Silver expected results)
    checks += [('R32', 'TC01', 'capstone: 11,500 encounters in Gold', 11500, act.encounters),
               ('R33', 'TC04', 'capstone: 185 cancelled encounters', 185, act.cancelled),
               ('R34', 'TC04', 'capstone: 118 valid stays', 118, act.valid_stays),
               ('R35', 'TC08', 'capstone: overall cancellation rate %', '1.61', f'{100 * act.cancelled / act.encounters:.2f}')]

recon = spark.createDataFrame([(BATCH_ID, c[0], c[1], c[2], str(c[3]), str(c[4]), 'PASS' if str(c[3]) == str(c[4]) else 'FAIL')
                               for c in checks],
                              'batch_id string, check_id string, test_id string, description string, expected string, actual string, result string')
recon = recon.withColumn('checked_at', F.current_timestamp())
recon.write.format('delta').mode('append').option('mergeSchema', 'true').saveAsTable(RECON_TABLE)
display(spark.table(RECON_TABLE).filter(F.col('batch_id') == BATCH_ID).orderBy('check_id'))

recon_failed = recon.filter("result = 'FAIL'").count()
print('Reconciliation checks:', len(checks), '| failed:', recon_failed)
assert recon_failed == 0, 'reconciliation failed - see gold_reconciliation'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step F - Replay test: running the same load again must change nothing (TC03, TC06)

# COMMAND ----------

MART_KEYS = ['mart_daily', 'mart_department', 'mart_beds']

def kpi_snapshot():
    return {k: sorted(str(r) for r in spark.table(TARGETS[k]).drop(*AUDIT_COLS).collect()) for k in MART_KEYS}

before = kpi_snapshot()
before_counts = {k: spark.table(v).count() for k, v in TARGETS.items() if k != 'run_log'}

REPLAY_BATCH = BATCH_ID + '_replay'
REPLAY_TS = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
replay = run_gold_load(TARGETS, load_sources(), REPLAY_BATCH, REPLAY_TS)
display(replay)

after = kpi_snapshot()
after_counts = {k: spark.table(v).count() for k, v in TARGETS.items() if k != 'run_log'}
changed_rows = replay.agg(F.sum('rows_inserted') + F.sum('rows_updated')).first()[0]
print('rows inserted + updated by the replay:', changed_rows)
print('table counts unchanged:', before_counts == after_counts)
print('KPI marts unchanged:', before == after)
assert changed_rows == 0 and before_counts == after_counts and before == after
print('PASS - replaying the same input does not double count')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step G - Incremental test on temporary tables (late discharge + new encounter)
# MAGIC
# MAGIC Uses the **same** `run_gold_load` function on `tmp_*` copies, so the real Gold tables are not touched. A simulated batch discharges one OPEN inpatient encounter (CDC COMPLETED) and adds one new encounter on the same date. Only that encounter date may change in the encounter marts, and the bed-hours must grow by exactly that stay.

# COMMAND ----------

TMP = {k: gold('tmp_' + v.split('.')[-1]) for k, v in TARGETS.items()}
inc_results = []

def inc_check(test_id, description, condition):
    inc_results.append((test_id, description, 'PASS' if condition else 'FAIL'))

try:
    for t in TMP.values():
        spark.sql(f'DROP TABLE IF EXISTS {t}')
    base = load_sources()
    ts1 = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    run_gold_load(TMP, base, 'inc_1', ts1)
    daily_1 = {r.encounter_date: r for r in spark.table(TMP['mart_daily']).collect()}
    beds_1 = spark.table(TMP['mart_beds']).agg(F.sum('occupied_bed_hours')).first()[0]

    # simulated batch: encounter X (OPEN, inpatient, no CDC yet) is discharged; a new encounter is added on X's date
    x = (spark.table(TMP['fact_encounter'])
         .filter("current_status = 'OPEN' AND visit_type IN ('INPATIENT') AND status_source = 'EVENT'")
         .orderBy('encounter_id').first())
    new_id = 'ENCOUNTER-9000001'
    change_ts = dt.datetime(2026, 8, 10, 12, 0, 0)
    los_min = int((change_ts - x.admit_ts_utc).total_seconds() // 60)
    is_x = F.col('encounter_id') == x.encounter_id
    ev, cur, iv, st = base['events'], base['current'], base['intervals'], base['stays']
    x_initial = iv.filter(is_x & (F.col('source_sequence') == 0))
    sim = dict(base)
    sim['events'] = ev.unionByName(ev.filter(is_x).withColumn('encounter_id', F.lit(new_id)).withColumn('_source_file', F.lit('simulated_batch.csv')))
    sim['current'] = (cur.withColumn('current_status', F.when(is_x, F.lit('COMPLETED')).otherwise(F.col('current_status')))
                         .withColumn('status_updated_at', F.when(is_x, F.lit(change_ts)).otherwise(F.col('status_updated_at')))
                         .withColumn('last_source_sequence', F.when(is_x, F.lit(9999).cast('bigint')).otherwise(F.col('last_source_sequence')))
                         .withColumn('status_source', F.when(is_x, F.lit('CDC')).otherwise(F.col('status_source')))
                         .unionByName(cur.filter(is_x).withColumn('encounter_id', F.lit(new_id))))
    sim['intervals'] = (iv.withColumn('valid_to_utc', F.when(is_x, F.lit(change_ts)).otherwise(F.col('valid_to_utc')))
                          .withColumn('is_current', F.when(is_x, F.lit(False)).otherwise(F.col('is_current')))
                          .unionByName(x_initial.withColumn('encounter_id', F.lit(new_id)))
                          .unionByName(x_initial.withColumn('source_sequence', F.lit(9999).cast('bigint')).withColumn('state_order', F.lit(2))
                                       .withColumn('record_type', F.lit('CDC')).withColumn('previous_status', F.lit('OPEN'))
                                       .withColumn('new_status', F.lit('COMPLETED')).withColumn('adt_event_type', F.lit('DISCHARGE'))
                                       .withColumn('change_reason', F.lit('SIMULATED_TEST')).withColumn('valid_from_utc', F.lit(change_ts))
                                       .withColumn('source_file', F.lit('simulated_cdc.csv'))))
    sim['stays'] = (st.withColumn('stay_status', F.when(is_x, F.lit('DISCHARGED')).otherwise(F.col('stay_status')))
                      .withColumn('current_status', F.when(is_x, F.lit('COMPLETED')).otherwise(F.col('current_status')))
                      .withColumn('discharge_ts_utc', F.when(is_x, F.lit(change_ts)).otherwise(F.col('discharge_ts_utc')))
                      .withColumn('is_valid_stay', F.when(is_x, F.lit(True)).otherwise(F.col('is_valid_stay')))
                      .withColumn('length_of_stay_minutes', F.when(is_x, F.lit(los_min).cast('bigint')).otherwise(F.col('length_of_stay_minutes')))
                      .withColumn('length_of_stay_hours', F.when(is_x, F.lit(round(los_min / 60, 2))).otherwise(F.col('length_of_stay_hours')))
                      .withColumn('length_of_stay_days', F.when(is_x, F.lit(round(los_min / 1440, 2))).otherwise(F.col('length_of_stay_days')))
                      .unionByName(st.filter(is_x).withColumn('encounter_id', F.lit(new_id))))

    ts2 = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    m2 = {r.table_name: r for r in run_gold_load(TMP, sim, 'inc_2', ts2).collect()}
    ff, tr = m2['tmp_fact_encounter_activity'], m2['tmp_fact_status_transitions']
    md2, mb2 = m2['tmp_mart_encounter_daily'], m2['tmp_mart_bed_occupancy_daily']
    inc_check('G01', 'fact: exactly 1 inserted (new encounter) and 1 updated (discharged encounter)', ff.rows_inserted == 1 and ff.rows_updated == 1)
    inc_check('G02', 'transitions: 2 inserted (DISCHARGE + new ADMISSION), 1 updated (closed admission interval)',
              tr.rows_inserted == 2 and tr.rows_updated == 1)
    inc_check('G03', 'encounter marts: only 1 encounter date recomputed', md2.affected_encounter_dates == 1 and md2.rows_updated == 1 and md2.rows_inserted == 0)
    daily_2 = {r.encounter_date: r for r in spark.table(TMP['mart_daily']).collect()}
    d = x.encounter_date
    inc_check('G04', 'that date: total_encounters +1 and valid_stays +1',
              daily_2[d].total_encounters == daily_1[d].total_encounters + 1 and daily_2[d].valid_stays == daily_1[d].valid_stays + 1)
    strip = lambda r: {k: v for k, v in r.asDict().items() if k not in AUDIT_COLS}
    inc_check('G05', 'every other encounter date is unchanged', all(strip(daily_2[k]) == strip(daily_1[k]) for k in daily_1 if k != d))
    beds_2 = spark.table(TMP['mart_beds']).agg(F.sum('occupied_bed_hours')).first()[0]
    inc_check('G06', 'bed-hours grow by exactly the new stay; only the stay days were updated',
              abs((beds_2 - beds_1) - los_min / 60) < 0.01 and mb2.rows_inserted == 0
              and mb2.rows_updated == (change_ts.date() - x.admit_ts_utc.date()).days + 1)

    ts3 = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    m3 = run_gold_load(TMP, sim, 'inc_3', ts3)
    inc_check('G07', 'replaying the simulated batch changes nothing', m3.agg(F.sum('rows_inserted') + F.sum('rows_updated')).first()[0] == 0)
finally:
    for t in TMP.values():
        spark.sql(f'DROP TABLE IF EXISTS {t}')

display(spark.createDataFrame(inc_results, 'test_id string, description string, result string').orderBy('test_id'))
inc_failed = [r for r in inc_results if r[2] != 'PASS']
assert not inc_failed, f'incremental tests failed: {inc_failed}'
print('ALL', len(inc_results), 'INCREMENTAL TESTS PASSED')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step H - Delta history, time travel, Spark plan and skew

# COMMAND ----------

# DELTA-ONLY: version history and time travel of the fact and the daily mart
for name in [TARGETS['fact_encounter'], TARGETS['mart_daily']]:
    print('History of', name)
    display(spark.sql(f'DESCRIBE HISTORY {name}')
            .select('version', 'timestamp', 'operation',
                    F.col('operationMetrics')['numTargetRowsInserted'].alias('rows_inserted'),
                    F.col('operationMetrics')['numTargetRowsUpdated'].alias('rows_updated'),
                    F.col('operationMetrics')['numTargetRowsDeleted'].alias('rows_deleted'))
            .orderBy('version'))

fact_name = TARGETS['fact_encounter']
hist_rows = spark.sql(f'DESCRIBE HISTORY {fact_name}').select('version', 'operation').collect()
v_first_load = min(r.version for r in hist_rows if r.operation == 'MERGE')     # first MERGE = first data load
v_latest = max(r.version for r in hist_rows)
biz_cols = [c for c in spark.table(fact_name).columns if c not in AUDIT_COLS]
old = spark.sql(f'SELECT * FROM {fact_name} VERSION AS OF {v_first_load}').select(*biz_cols)
new = spark.table(fact_name).select(*biz_cols)
print(f'Time travel: version {v_first_load} rows = {old.count()}, latest version {v_latest} rows = {new.count()}')
print('Rows that differ between the first load and now (replay must add none):', new.exceptAll(old).count() + old.exceptAll(new).count())
# Rollback (documentation only - do not run in the demo):  RESTORE TABLE <fact> TO VERSION AS OF <n>

# COMMAND ----------

# Spark plan of a representative dashboard query (department waiting time and stays for one week)
REP_QUERY = f"""
SELECT d.department_name, f.encounter_date,
       COUNT(*) AS encounters, ROUND(AVG(f.wait_minutes), 2) AS avg_wait_minutes,
       SUM(CAST(f.is_valid_stay AS INT)) AS valid_stays
FROM {TARGETS['fact_encounter']} f JOIN {TARGETS['dim_department']} d ON f.department_sk = d.department_sk
WHERE f.encounter_date BETWEEN DATE'2026-07-10' AND DATE'2026-07-16'
GROUP BY d.department_name, f.encounter_date"""
plan = spark.sql('EXPLAIN FORMATTED ' + REP_QUERY).first()[0]
print(plan[:3000])
print('Broadcast join used for the dimension:', 'BroadcastHashJoin' in plan)

# Skew: how evenly encounters spread over the join / group keys
for col_name in ['department_sk', 'patient_sk', 'encounter_date']:
    s = (spark.table(TARGETS['fact_encounter']).groupBy(col_name).count()
         .agg(F.min('count').alias('min'), F.max('count').alias('max'), F.round(F.avg('count'), 1).alias('avg')).first())
    print(f'{col_name:14s} rows per key: min {s["min"]} | max {s["max"]} | avg {s["avg"]} | max/avg {s["max"] / s["avg"]:.2f}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step I - Benchmark a representative query before / after OPTIMIZE (TC10)

# COMMAND ----------

# DELTA-ONLY: timings are recorded as measured - no improvement is claimed unless the numbers show it.
def time_query(sql, runs=3):
    out = []
    for _ in range(runs):
        t0 = time.perf_counter()
        spark.sql(sql).collect()
        out.append(round(time.perf_counter() - t0, 3))
    return sorted(out)[len(out) // 2], out      # median, all runs

if RUN_BENCHMARK:
    try:
        spark.conf.set('spark.databricks.io.cache.enabled', 'false')   # not allowed on some compute types
    except Exception as e:
        print('disk cache setting not changed:', type(e).__name__)
    files_before = spark.sql(f"DESCRIBE DETAIL {TARGETS['fact_encounter']}").first().numFiles
    med_before, runs_before = time_query(REP_QUERY)
    spark.sql(f"OPTIMIZE {TARGETS['fact_encounter']} ZORDER BY (encounter_date, department_sk)")
    files_after = spark.sql(f"DESCRIBE DETAIL {TARGETS['fact_encounter']}").first().numFiles
    med_after, runs_after = time_query(REP_QUERY)
    bench = spark.createDataFrame([(BATCH_ID, 'department wait and stays - one week', files_before, files_after, med_before, med_after,
                                    str(runs_before), str(runs_after))],
                                  'batch_id string, query string, files_before long, files_after long, median_sec_before double, '
                                  'median_sec_after double, runs_before string, runs_after string')
    bench = bench.withColumn('measured_at', F.current_timestamp())
    bench.write.format('delta').mode('append').saveAsTable(BENCH_TABLE)
    BENCH_RAN = True
    display(bench)
    verdict = 'faster' if med_after < med_before else 'not faster (data is small; startup cost dominates)'
    print(f'files {files_before} -> {files_after} | median {med_before}s -> {med_after}s | after OPTIMIZE the query was {verdict}')
else:
    print('Benchmark skipped (RUN_BENCHMARK = False)')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step J - Table comments for the dashboard and Genie, and KPI card queries (TC09)

# COMMAND ----------

comments = {
    'dim_department': 'Conformed department dimension (SCD1). department_sk = xxhash64(department_id); -1 = UNKNOWN member.',
    'dim_patient': 'Restricted patient dimension. Key = patient_token (salted SHA-256); no raw patient_id or name. -1 = UNKNOWN.',
    'dim_date': 'Calendar dimension. date_key = yyyyMMdd.',
    'dim_status': 'Encounter status codes. is_cancelled drives the cancellation rate; REVERSED is not a cancellation.',
    'fact_encounter': 'One row per encounter_id with its latest valid status, waiting time, bill and stay interval. '
                      'length_of_stay_* is set only for valid stays (discharged through CDC). Times are UTC.',
    'fact_transitions': 'One row per encounter_id + source_sequence: ADT state intervals (ADMISSION, DISCHARGE, CANCELLATION, REVERSAL).',
    'fact_secondary': 'One row per secondary operational event (ALERT, CHECK, REVIEW) linked to an encounter.',
    'mart_daily': 'Daily network KPIs by encounter (admission) date: encounters, average wait, long-wait rate, valid stays, average length of stay, cancellation rate.',
    'mart_department': 'Daily department KPIs by encounter date: waiting time and length of stay.',
    'mart_beds': f'Daily bed utilization per department: occupied bed-hours of {", ".join(BED_VISIT_TYPES)} stays / ({BEDS_PER_DEPARTMENT} beds x 24 h). Capacity is an assumption.',
    'run_log': 'Rows inserted / updated per Gold run and table (freshness monitoring).',
}
for key, text in comments.items():
    spark.sql(f"COMMENT ON TABLE {TARGETS[key]} IS '{text}'")
print('Table comments set on', len(comments), 'Gold tables.')

# KPI card queries - run these in the SQL editor / dashboard and compare with the values below (TC09)
KPI_CARDS = {
    'Total encounters': f"SELECT SUM(total_encounters) FROM {TARGETS['mart_daily']}",
    'Average waiting time (min)': f"SELECT ROUND(SUM(total_wait_minutes) / SUM(total_encounters), 2) FROM {TARGETS['mart_daily']}",
    'Long-wait rate %': f"SELECT ROUND(100.0 * SUM(long_wait_encounters) / SUM(total_encounters), 2) FROM {TARGETS['mart_daily']}",
    'Average length of stay (days)': f"SELECT ROUND(SUM(total_length_of_stay_minutes) / SUM(valid_stays) / 1440, 2) FROM {TARGETS['mart_daily']}",
    'Bed utilization %': f"SELECT ROUND(100.0 * SUM(occupied_bed_hours) / SUM(available_bed_hours), 2) FROM {TARGETS['mart_beds']}",
    'Cancellation rate %': f"SELECT ROUND(100.0 * SUM(cancelled_encounters) / SUM(total_encounters), 2) FROM {TARGETS['mart_daily']}",
}
display(spark.createDataFrame([(k, str(spark.sql(q).first()[0]), ' '.join(q.split())) for k, q in KPI_CARDS.items()],
                              'kpi string, value string, sql string'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step K - Final validation (read-only)

# COMMAND ----------

# ============================================================
# DAY 3 FINAL VALIDATION - read-only checks of the Gold layer
# ============================================================
vresults = []

def vcheck(name, condition, detail=''):
    vresults.append((name, 'PASS' if condition else 'FAIL', detail))
    print(('PASS' if condition else 'FAIL') + ' | ' + name + (f' | {detail}' if detail else ''))

def vinfo(name, detail):
    vresults.append((name, 'INFO', detail))
    print(f'INFO | {name} | {detail}')

for name in ALL_GOLD_TABLES:
    if name == BENCH_TABLE and not BENCH_RAN:
        continue
    vcheck('Table exists: ' + name.split('.')[-1], spark.catalog.tableExists(name))

fact = spark.table(TARGETS['fact_encounter'])
n_fact = fact.count()
vcheck('fact_encounter_activity unique at encounter_id', n_fact == fact.select('encounter_id').distinct().count(), f'{n_fact} rows')
vcheck('fact rows = silver_encounter_current rows', n_fact == spark.table(SILVER_ENCOUNTER_CURRENT).count())
vcheck('No unmatched department / patient keys', fact.filter((F.col('department_sk') == UNKNOWN_SK) | (F.col('patient_sk') == UNKNOWN_SK)).count() == 0)
tr = spark.table(TARGETS['fact_transitions'])
vcheck('fact_status_transitions unique at encounter_id + source_sequence', tr.count() == tr.select('encounter_id', 'source_sequence').distinct().count(), f'{tr.count()} rows')
fs = spark.table(TARGETS['fact_secondary'])
vcheck('fact_secondary_events unique at event_id', fs.count() == fs.select('event_id').distinct().count(), f'{fs.count()} rows')
for key, grain in [('mart_daily', ['encounter_date']), ('mart_department', ['encounter_date', 'department_sk'])]:
    m = spark.table(TARGETS[key])
    vcheck(f'{TARGETS[key].split(".")[-1]} unique at {"+".join(grain)}', m.count() == m.select(*grain).distinct().count(), f'{m.count()} rows')
    vcheck(f'{TARGETS[key].split(".")[-1]} total encounters = fact', m.agg(F.sum('total_encounters')).first()[0] == n_fact)
mb = spark.table(TARGETS['mart_beds'])
vcheck('mart_bed_occupancy_daily unique at census_date+department_sk', mb.count() == mb.select('census_date', 'department_sk').distinct().count(), f'{mb.count()} rows')
vcheck('Bed utilization between 0 and 100 %', mb.filter((F.col('bed_utilization_pct') < 0) | (F.col('bed_utilization_pct') > 100)).count() == 0)
vcheck('Length of stay only on valid stays', fact.filter(F.col('length_of_stay_minutes').isNotNull() & ~F.col('is_valid_stay')).count() == 0)
vcheck('No raw patient_id / doctor_id in any Gold table',
       not any(c in ('patient_id', 'doctor_id') for t in TARGETS.values() for c in spark.table(t).columns))
vcheck('Reconciliation (this batch) all PASS', spark.table(RECON_TABLE).filter((F.col('batch_id') == BATCH_ID) & (F.col('result') != 'PASS')).count() == 0)
vcheck('Replay batch changed 0 rows', spark.table(TARGETS['run_log']).filter(F.col('batch_id') == REPLAY_BATCH)
       .agg(F.sum('rows_inserted') + F.sum('rows_updated')).first()[0] == 0)
vcheck('Incremental tests all passed', len(inc_results) > 0 and all(r[2] == 'PASS' for r in inc_results), f'{len(inc_results)} tests')
vcheck('KPI definitions saved', spark.table(KPI_DEFS_TABLE).count() == len(kpi_rows))
if STRICT_EXPECTED_COUNTS:
    vcheck('Capstone counts: fact 11,500 / transitions 12,020 / secondary 310', (n_fact, tr.count(), fs.count()) == (11500, 12020, 310))
freshness = spark.table(TARGETS['run_log']).agg(F.max('run_ts')).first()[0]
vinfo('Freshness', f'last Gold run (UTC): {freshness}')
vinfo('Bed capacity', f'{BEDS_PER_DEPARTMENT} beds per department is a placeholder - no capacity source supplied')
vinfo('Length of stay', 'only encounters discharged through CDC have a discharge time (118); LOS KPIs rest on these')
vinfo('Long wait threshold', f'{LONG_WAIT_THRESHOLD_MIN} minutes - assumption, needs client approval')

passed = sum(1 for r in vresults if r[1] == 'PASS')
failed = sum(1 for r in vresults if r[1] == 'FAIL')
print('=' * 65)
print('DAY 3 GOLD VALIDATION SUMMARY | passed:', passed, '| failed:', failed, '| info:', sum(1 for r in vresults if r[1] == 'INFO'))
print('RESULT:', 'ALL EXECUTED VALIDATION CHECKS PASSED' if failed == 0 else 'VALIDATION NEEDS ATTENTION')
print('=' * 65)
for name, status, detail in vresults:
    if status == 'FAIL':
        print('-', name, detail)
assert failed == 0

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 3 - Gold layer: decisions and limitations
# MAGIC
# MAGIC 1. **Fact grain:** `fact_encounter_activity` = one row per `encounter_id` (current state from `silver_encounter_current`, stay from `silver_stay_intervals`); `fact_status_transitions` = one row per `encounter_id + source_sequence`; `fact_secondary_events` = one row per `event_id`. Each grain is asserted unique.
# MAGIC 2. **Surrogate keys:** `department_sk = xxhash64(department_id)`, `patient_sk = xxhash64(patient_token)`. They are deterministic, so a rerun gives the same keys. `-1` is the UNKNOWN member; TC07 expects no fact row on `-1`.
# MAGIC 3. **Restricted identifiers:** Gold never stores raw `patient_id`, `doctor_id` or the patient name. `dim_patient` is keyed on the same salted token as Silver (check R14 proves the salts match) and shows only `PATIENT_ID-****`.
# MAGIC 4. **Waiting time:** average of `wait_minutes` over **all** encounters (cancelled encounters also waited). Long wait = more than 60 minutes (parameter, needs approval).
# MAGIC 5. **Length of stay:** only valid stays (discharged through a CDC update, discharge >= admission) have a length of stay; it is attributed to the **admission date**. In this data that is 118 encounters, all discharged on 2026-08-10, so LOS values are long (days to weeks) and thinly populated.
# MAGIC 6. **Bed utilization:** occupied bed-hours of INPATIENT stays split across each census day / available bed-hours. **No bed capacity source exists**; `BEDS_PER_DEPARTMENT = 10` is a placeholder and the utilization % scales with it. Open stays are excluded by default because they have no discharge time (`INCLUDE_OPEN_STAYS` switches that on).
# MAGIC 7. **Cancellation:** only `CANCELLED` counts. `REVERSED` (172) is reported separately, same as Silver.
# MAGIC 8. **Incremental logic:** every table is loaded with a hash-guarded `MERGE`. Encounter marts recompute only the encounter dates touched by a batch; bed occupancy is recomputed in full (a stay spans many days) but the MERGE updates only rows that changed. Replay inserts and updates **0** rows (Step F); the simulated late discharge changes one fact row, one encounter date and exactly the stay's census days (Step G).
# MAGIC 9. **Reconciliation:** expected values come from independent SQL over Silver, stored per batch in `gold_reconciliation`.
# MAGIC 10. **Runtime:** the mart MERGE uses `WHEN NOT MATCHED BY SOURCE`, which needs Databricks Runtime 12.2 LTS or newer (or serverless).
# MAGIC 11. **Schema changes:** a new Silver column will not reach Gold until it is added to the build function; run once with `RESET_GOLD = True` after such a change.