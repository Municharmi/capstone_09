"""Silver-layer transformations for the Capstone 09 smart meter project (Day 2).

Every function is DataFrame -> DataFrame (no table writes) so it can be unit tested on
small inputs. Rules, interval settings and tie-breakers live in configs/silver_config.json.

Row outcome per source record: KEPT | DUPLICATE_DROPPED | QUARANTINED
  * QUARANTINED: at least one CRITICAL rule failed (never deduplicated, never dropped)
  * duplicates are resolved among DQ-valid rows only, so an invalid copy can never win
"""
import json
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "silver_config.json"

READING_COLS = ["reading_id", "event_timestamp", "ingest_timestamp", "substation_id", "meter_id", "tariff_type",
                "meter_status", "unit", "consumption_kwh", "voltage_v", "feeder_id", "source_system",
                "source_schema_version", "_input_file_name", "_ingestion_timestamp"]
SECONDARY_COLS = ["event_id", "reading_id", "event_timestamp", "related_entity_id", "event_type", "event_value",
                  "source_system", "_input_file_name", "_ingestion_timestamp"]
STATUS_COLS = ["reading_id", "change_timestamp", "previous_status", "new_status", "change_reason",
               "source_sequence", "source_system", "_input_file_name", "_ingestion_timestamp"]


# ---------------------------------------------------------------- config / generic helpers
def load_config(path=CONFIG_PATH) -> dict:
    with open(path) as f:
        return json.load(f)


def rules_for(config: dict, entity: str) -> list:
    """Executable rules (rules with a SQL check) for one entity."""
    return [r for r in config["dq_rules"] if r["entity"] == entity and r.get("check")]


def _select_raw(df: DataFrame, cols: list) -> DataFrame:
    """Select source columns; missing optional columns (e.g. source_schema_version) become NULL."""
    return df.select(*[(F.col(c) if c in df.columns else F.lit(None).cast("string")).alias(c) for c in cols])


def add_key_flags(df: DataFrame, flags: dict) -> DataFrame:
    """flags = {flag_col: (df_col, ref_df, ref_col)} -> boolean flag_col = key exists in ref_df."""
    for flag, (col, ref, ref_col) in flags.items():
        keys = ref.select(F.col(ref_col).alias("_k")).where("_k IS NOT NULL").distinct().withColumn(flag, F.lit(True))
        df = (df.join(F.broadcast(keys), df[col] == keys["_k"], "left").drop("_k")
                .withColumn(flag, F.coalesce(F.col(flag), F.lit(False))))
    return df


def evaluate_rules(df: DataFrame, rules: list) -> DataFrame:
    """Adds dq_failed_rules (QUARANTINE rules failed) and dq_warnings (FLAG rules failed).
    A check that evaluates to NULL counts as failed."""
    def failed(action):
        checks = [F.when(~F.coalesce(F.expr(r["check"]), F.lit(False)), F.lit(r["rule_id"]))
                  for r in rules if r["action"] == action]
        if not checks:
            return F.array().cast("array<string>")
        return F.filter(F.array(*checks), lambda x: x.isNotNull())
    return df.withColumns({"dq_failed_rules": failed("QUARANTINE"), "dq_warnings": failed("FLAG")})


def classify(df: DataFrame, key_cols: list, order_cols: list) -> DataFrame:
    """Deterministic dedup. Adds outcome and duplicate_copies.
    Within each key (DQ-valid rows only) the first row by order_cols is KEPT; a SHA-256 hash of the
    whole row is the final tie-breaker, so the result never depends on input order."""
    df = df.withColumn("_row_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)])), 256))
    df = df.withColumn("_valid", F.size("dq_failed_rules") == 0)
    part = [*key_cols, "_valid"]
    w = Window.partitionBy(*part).orderBy(*order_cols, F.col("_row_hash"))
    return (df.withColumn("_rn", F.row_number().over(w))
              .withColumn("duplicate_copies", (F.count(F.lit(1)).over(Window.partitionBy(*part)) - 1).cast("int"))
              .withColumn("outcome", F.when(~F.col("_valid"), "QUARANTINED")
                                       .when(F.col("_rn") == 1, "KEPT").otherwise("DUPLICATE_DROPPED"))
              .withColumn("duplicate_copies", F.when(F.col("_valid"), F.col("duplicate_copies")).otherwise(F.lit(0)))
              .drop("_rn", "_valid", "_row_hash"))


def quarantine_records(ev: DataFrame, source_table: str, key_cols: list, raw_cols: list) -> DataFrame:
    """Rejected rows with failed rule IDs and the untouched raw record (JSON) for traceability."""
    return (ev.filter("outcome = 'QUARANTINED'")
              .select(F.lit(source_table).alias("source_table"),
                      F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("<null>")) for k in key_cols]).alias("record_key"),
                      "dq_failed_rules", "dq_warnings",
                      F.to_json(F.struct(*[F.col(c) for c in raw_cols])).alias("raw_record"),
                      "_input_file_name", "_ingestion_timestamp"))


def duplicate_audit(ev: DataFrame, source_table: str, key_cols: list) -> DataFrame:
    """One row per duplicated key: copies received/dropped, kept file and dropped files."""
    kept, dropped = F.col("outcome") == "KEPT", F.col("outcome") == "DUPLICATE_DROPPED"
    return (ev.filter("outcome IN ('KEPT','DUPLICATE_DROPPED') AND duplicate_copies > 0")
              .groupBy(*key_cols)
              .agg(F.count("*").alias("copies_received"),
                   F.sum(dropped.cast("int")).alias("copies_dropped"),
                   F.max(F.when(kept, F.col("_input_file_name"))).alias("kept_file"),
                   F.sort_array(F.collect_list(F.when(dropped, F.col("_input_file_name")))).alias("dropped_files"))
              .select(F.lit(source_table).alias("source_table"),
                      F.concat_ws("|", *[F.col(k).cast("string") for k in key_cols]).alias("record_key"),
                      "copies_received", "copies_dropped", "kept_file", "dropped_files"))


# ---------------------------------------------------------------- meter readings
def process_readings(bronze: DataFrame, meters: DataFrame, substations: DataFrame, config: dict) -> DataFrame:
    """Typed + DQ-evaluated + classified readings (all Bronze rows, one outcome each)."""
    df = _select_raw(bronze, READING_COLS).withColumns({
        "event_ts": F.expr("try_to_timestamp(event_timestamp)"),
        "ingest_ts": F.expr("try_to_timestamp(ingest_timestamp)"),
        "kwh": F.expr("try_cast(consumption_kwh AS DOUBLE)"),
        "voltage": F.expr("try_cast(voltage_v AS DOUBLE)"),
    })
    df = add_key_flags(df, {"_meter_known": ("meter_id", meters, "meter_id"),
                            "_substation_known": ("substation_id", substations, "substation_id")})
    df = evaluate_rules(df, rules_for(config, "meter_readings"))
    return classify(df, ["reading_id"], [F.col("ingest_ts").desc_nulls_last(), F.col("_input_file_name").desc_nulls_last(),
                                         F.col("_ingestion_timestamp").desc_nulls_last()])


def silver_readings(ev: DataFrame) -> DataFrame:
    """Typed, deduplicated readings; raw source strings kept as raw_* columns."""
    return ev.filter("outcome = 'KEPT'").select(
        "reading_id", "meter_id", "substation_id", "feeder_id", "event_ts", "ingest_ts",
        F.to_date("event_ts").alias("reading_date"),
        F.col("kwh").alias("consumption_kwh"), F.col("voltage").alias("voltage_v"),
        F.coalesce(F.upper(F.trim("tariff_type")), F.lit("UNKNOWN")).alias("tariff_type"),
        F.upper(F.trim("meter_status")).alias("meter_status"), F.upper(F.trim("unit")).alias("unit"),
        "source_system", "source_schema_version", "dq_warnings", "duplicate_copies",
        F.col("event_timestamp").alias("raw_event_timestamp"), F.col("ingest_timestamp").alias("raw_ingest_timestamp"),
        F.col("consumption_kwh").alias("raw_consumption_kwh"), F.col("voltage_v").alias("raw_voltage_v"),
        F.col("tariff_type").alias("raw_tariff_type"), "_input_file_name", "_ingestion_timestamp")


# ---------------------------------------------------------------- secondary events
def process_secondary(bronze: DataFrame, readings: DataFrame, substations: DataFrame, config: dict) -> DataFrame:
    df = _select_raw(bronze, SECONDARY_COLS).withColumns({
        "event_ts": F.expr("try_to_timestamp(event_timestamp)"),
        "value": F.expr("try_cast(event_value AS DOUBLE)"),
    })
    df = add_key_flags(df, {"_reading_known": ("reading_id", readings, "reading_id"),
                            "_entity_known": ("related_entity_id", substations, "substation_id")})
    df = evaluate_rules(df, rules_for(config, "secondary_events"))
    return classify(df, ["event_id"], [F.col("_input_file_name").desc_nulls_last(), F.col("_ingestion_timestamp").desc_nulls_last()])


def silver_secondary(ev: DataFrame) -> DataFrame:
    return ev.filter("outcome = 'KEPT'").select(
        "event_id", "reading_id", "related_entity_id", "event_ts", F.to_date("event_ts").alias("event_date"),
        F.upper(F.trim("event_type")).alias("event_type"), F.col("value").alias("event_value"), "source_system",
        "dq_warnings", "duplicate_copies", F.col("event_timestamp").alias("raw_event_timestamp"),
        F.col("event_value").alias("raw_event_value"), "_input_file_name", "_ingestion_timestamp")


# ---------------------------------------------------------------- status CDC
def process_status_updates(bronze: DataFrame, readings: DataFrame, config: dict) -> DataFrame:
    df = _select_raw(bronze, STATUS_COLS).withColumns({
        "change_ts": F.expr("try_to_timestamp(change_timestamp)"),
        "seq": F.expr("try_cast(source_sequence AS INT)"),
    })
    df = add_key_flags(df, {"_reading_known": ("reading_id", readings, "reading_id")})
    df = evaluate_rules(df, rules_for(config, "status_updates"))
    return classify(df, ["reading_id", "seq"], [F.col("_input_file_name").desc_nulls_last(), F.col("_ingestion_timestamp").desc_nulls_last()])


def build_status_history(readings: DataFrame, status_ev: DataFrame) -> DataFrame:
    """Status versions per reading ordered by source_sequence (authoritative), then change time.
    Version 1 = status on the reading itself (source_sequence 0). The last version is current, so a
    late-arriving lower sequence lands in history but never overwrites a newer status.
    Flags: arrived_late (a higher sequence had already been ingested), is_out_of_order (change time
    earlier than the previous version's), status_chain_ok (reported previous_status matches, SM-DQ-034)."""
    initial = readings.select(
        "reading_id", F.lit(0).alias("source_sequence"), F.col("event_ts").alias("effective_from"),
        F.lit(None).cast("string").alias("reported_previous_status"), F.col("meter_status").alias("status"),
        F.lit("INITIAL_READING").alias("change_reason"), F.lit("meter_readings").alias("record_source"),
        "_input_file_name", "_ingestion_timestamp")
    updates = status_ev.filter("outcome = 'KEPT'").select(
        "reading_id", F.col("seq").alias("source_sequence"), F.col("change_ts").alias("effective_from"),
        F.upper(F.trim("previous_status")).alias("reported_previous_status"), F.upper(F.trim("new_status")).alias("status"),
        "change_reason", F.lit("status_updates").alias("record_source"), "_input_file_name", "_ingestion_timestamp")
    w = Window.partitionBy("reading_id").orderBy("source_sequence", "effective_from", "_ingestion_timestamp")
    later = w.rowsBetween(1, Window.unboundedFollowing)
    return (initial.unionByName(updates)
            .withColumns({
                "version": F.row_number().over(w),
                "expected_previous_status": F.lag("status").over(w),
                "effective_to": F.lead("effective_from").over(w),
                "is_current": F.lead("source_sequence").over(w).isNull(),
                "is_out_of_order": F.coalesce(F.col("effective_from") < F.lag("effective_from").over(w), F.lit(False)),
                "arrived_late": F.coalesce(F.col("_ingestion_timestamp") > F.min("_ingestion_timestamp").over(later), F.lit(False)),
            })
            .withColumn("status_chain_ok", F.col("reported_previous_status").isNull()
                        | (F.col("reported_previous_status") == F.col("expected_previous_status"))))


def status_current(history: DataFrame) -> DataFrame:
    return history.filter("is_current").select(
        "reading_id", F.col("status").alias("current_status"), "source_sequence",
        F.col("effective_from").alias("status_effective_from"), F.col("version").alias("status_versions"),
        "change_reason", "record_source", "_input_file_name")


# ---------------------------------------------------------------- intervals, gaps, energy, reconciliation
def build_expected_intervals(readings_ev: DataFrame, readings: DataFrame, interval_minutes: int,
                             tolerance_minutes: int, period_end=None) -> DataFrame:
    """One row per expected meter interval. Schedule per meter = first parsed reading (anchor) +
    k * interval up to period_end (default: latest event in the data). A valid Silver reading within
    +/- tolerance of a slot fills it; otherwise the slot is missing (gap). A slot is only expected once
    it is due (expected_ts <= period_end), so the data cut-off never creates a false trailing gap."""
    iv, tol = interval_minutes * 60, tolerance_minutes * 60
    sched = (readings_ev.filter("event_ts IS NOT NULL AND _meter_known").groupBy("meter_id")
             .agg(F.min("event_ts").alias("anchor_ts"), F.max("substation_id").alias("substation_id")))
    end = (readings_ev.agg(F.max("event_ts").alias("period_end_ts")) if period_end is None
           else readings_ev.sparkSession.range(1).select(F.lit(period_end).cast("timestamp").alias("period_end_ts")))
    anchor_s = F.unix_timestamp("anchor_ts")
    slots = (sched.crossJoin(end)
             .withColumn("slot", F.explode(F.sequence(F.lit(0), F.greatest(F.lit(0), F.floor((F.unix_timestamp("period_end_ts") - anchor_s + tol) / iv).cast("int")))))
             .withColumn("expected_ts", F.timestamp_seconds(anchor_s + F.col("slot") * iv)))
    act = (readings.select("meter_id", "reading_id", "event_ts", "consumption_kwh")
           .join(sched.select("meter_id", "anchor_ts"), "meter_id")
           .withColumn("slot", F.round((F.unix_timestamp("event_ts") - F.unix_timestamp("anchor_ts")) / iv).cast("int"))
           .withColumn("offset_seconds", (F.unix_timestamp("event_ts") - F.unix_timestamp("anchor_ts") - F.col("slot") * iv).cast("int"))
           .filter(F.abs("offset_seconds") <= tol).drop("anchor_ts"))
    first = Window.partitionBy("meter_id", "slot").orderBy("event_ts", "reading_id")
    act = (act.withColumn("readings_in_slot", F.count(F.lit(1)).over(Window.partitionBy("meter_id", "slot")))
              .withColumn("_rn", F.row_number().over(first)).filter("_rn = 1").drop("_rn"))
    return (slots.join(act, ["meter_id", "slot"], "left")
            .filter(F.col("reading_id").isNotNull() | (F.col("expected_ts") <= F.col("period_end_ts")))
            .select("meter_id", "substation_id", "slot", "anchor_ts", "expected_ts",
                    F.to_date(F.coalesce("event_ts", "expected_ts")).alias("interval_date"),
                    "reading_id", "event_ts", "offset_seconds",
                    F.coalesce("readings_in_slot", F.lit(0)).alias("readings_in_slot"),
                    "consumption_kwh", F.col("reading_id").isNull().alias("is_missing")))


def meter_daily_energy(intervals: DataFrame, readings: DataFrame) -> DataFrame:
    """Per meter and day: expected / received / missing intervals and energy from all valid readings."""
    cnt = (intervals.groupBy("meter_id", "substation_id", F.col("interval_date").alias("reading_date"))
           .agg(F.count(F.lit(1)).alias("expected_intervals"),
                F.sum((~F.col("is_missing")).cast("int")).alias("received_intervals"),
                F.sum(F.col("is_missing").cast("int")).alias("missing_intervals")))
    en = (readings.groupBy("meter_id", "reading_date")
          .agg(F.count(F.lit(1)).alias("valid_readings"), F.sum("consumption_kwh").alias("energy_kwh")))
    return (cnt.join(en, ["meter_id", "reading_date"], "full")
            .fillna(0, ["expected_intervals", "received_intervals", "missing_intervals", "valid_readings"])
            .fillna(0.0, ["energy_kwh"])
            .withColumn("missing_interval_rate",
                        F.when(F.col("expected_intervals") > 0, F.col("missing_intervals") / F.col("expected_intervals"))))


def reconcile_daily(readings_ev: DataFrame, daily: DataFrame) -> DataFrame:
    """Per day: Bronze control totals = KEPT + DUPLICATE_DROPPED + QUARANTINED (rows and kWh),
    and Silver kWh = sum of meter daily energy. status PASS when all three differences are 0."""
    kwh = F.coalesce(F.col("kwh"), F.lit(0.0))
    def part(o):
        return [F.count(F.when(F.col("outcome") == o, 1)).alias(f"{o.lower()}_rows"),
                F.sum(F.when(F.col("outcome") == o, kwh).otherwise(0.0)).alias(f"{o.lower()}_kwh")]
    ctrl = (readings_ev.groupBy(F.to_date("event_ts").alias("reading_date"))
            .agg(F.count(F.lit(1)).alias("bronze_rows"), F.sum(kwh).alias("bronze_kwh"),
                 *part("KEPT"), *part("DUPLICATE_DROPPED"), *part("QUARANTINED")))
    tot = daily.groupBy("reading_date").agg(F.sum("energy_kwh").alias("daily_energy_kwh"),
                                            F.sum("missing_intervals").alias("missing_intervals"))
    r = (ctrl.join(tot, "reading_date", "left").fillna(0.0, ["daily_energy_kwh"]).fillna(0, ["missing_intervals"])
         .withColumns({
             "row_diff": F.col("bronze_rows") - F.col("kept_rows") - F.col("duplicate_dropped_rows") - F.col("quarantined_rows"),
             "kwh_diff": F.round(F.col("bronze_kwh") - F.col("kept_kwh") - F.col("duplicate_dropped_kwh") - F.col("quarantined_kwh"), 6),
             "daily_total_diff": F.round(F.col("kept_kwh") - F.col("daily_energy_kwh"), 6)}))
    ok = (F.col("row_diff") == 0) & (F.abs("kwh_diff") < 1e-6) & (F.abs("daily_total_diff") < 1e-6)
    return r.withColumn("status", F.when(ok, "PASS").otherwise("FAIL"))
