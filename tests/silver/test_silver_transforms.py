"""Unit tests for src/silver/silver_transforms.py (Day 2).
Cases: valid, invalid (quarantine), duplicate (deterministic tie-breaker), late and out-of-order status CDC,
interval gaps (incl. data cut-off) and daily reconciliation. Run from notebooks/silver/02_silver_quality_cdc."""
import datetime as dt

import pytest

from src.silver import silver_transforms as st

CONFIG = st.load_config()
TS = dt.datetime.fromisoformat

READING_SCHEMA = ("reading_id string, event_timestamp string, ingest_timestamp string, substation_id string, meter_id string, "
                  "tariff_type string, meter_status string, unit string, consumption_kwh string, voltage_v string, "
                  "feeder_id string, source_system string, source_schema_version string, _input_file_name string, "
                  "_ingestion_timestamp timestamp")
STATUS_SCHEMA = ("reading_id string, change_timestamp string, previous_status string, new_status string, change_reason string, "
                 "source_sequence string, source_system string, _input_file_name string, _ingestion_timestamp timestamp")


@pytest.fixture(scope="module")
def spark():
    try:
        from databricks.sdk.runtime import spark as s
        return s
    except Exception:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="module")
def refs(spark):
    return (spark.createDataFrame([("M1",), ("M2",)], "meter_id string"),
            spark.createDataFrame([("S1",)], "substation_id string"))


def reading(rid="R1", ts="2026-07-01T00:00:00", kwh="1.5", meter="M1", sub="S1", tariff="RESIDENTIAL", unit="KWH",
            volt="230", status="OPEN", file="b01.csv", ingested="2026-11-02 10:00:00", version=None, ingest_ts=None):
    return (rid, ts, ingest_ts or ts, sub, meter, tariff, status, unit, kwh, volt, "FD1", "ADLS_EVENT", version, file, TS(ingested))


def status(rid="R1", seq="1", ts="2026-08-10T00:00:00", prev="OPEN", new="COMPLETED", file="s01.csv", ingested="2026-11-02 11:00:00"):
    return (rid, ts, prev, new, "SYSTEM_REVIEW", seq, "ADLS_CDC", file, TS(ingested))


def run_readings(spark, refs, rows):
    return st.process_readings(spark.createDataFrame(rows, READING_SCHEMA), *refs, CONFIG)


def run_status(spark, refs, updates):
    readings = st.silver_readings(run_readings(spark, refs, [reading()]))
    ev = st.process_status_updates(spark.createDataFrame(updates, STATUS_SCHEMA), readings, CONFIG)
    hist = st.build_status_history(readings, ev)
    return ev, hist, st.status_current(hist)


def history_rows(hist):
    return [(r.source_sequence, r.status, r.arrived_late, r.is_out_of_order, r.status_chain_ok, r.is_current)
            for r in hist.orderBy("version").collect()]


# ---------------------------------------------------------------- valid / invalid
def test_valid_reading_is_typed_and_keeps_raw(spark, refs):
    rows = st.silver_readings(run_readings(spark, refs, [reading()])).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.consumption_kwh == 1.5 and r.voltage_v == 230.0 and r.event_ts == TS("2026-07-01T00:00:00")
    assert r.raw_consumption_kwh == "1.5" and r.raw_event_timestamp == "2026-07-01T00:00:00"
    assert r.dq_warnings == [] and r.duplicate_copies == 0 and r.reading_date == dt.date(2026, 7, 1)


@pytest.mark.parametrize("row, rule", [
    (reading(rid=None), "SM-DQ-001"),
    (reading(rid="R2", meter=None), "SM-DQ-002"),
    (reading(rid="R3", sub="S9"), "SM-DQ-003"),
    (reading(rid="R4", ts="not-a-time"), "SM-DQ-004"),
    (reading(rid="R5", kwh="abc"), "SM-DQ-005"),
    (reading(rid="R6", kwh="250.0"), "SM-DQ-006"),
    (reading(rid="R7", kwh="-1"), "SM-DQ-006"),
    (reading(rid="R8", unit="MWH"), "SM-DQ-007"),
])
def test_invalid_mandatory_field_is_quarantined(spark, refs, row, rule):
    ev = run_readings(spark, refs, [row])
    r = ev.collect()[0]
    assert r.outcome == "QUARANTINED" and rule in r.dq_failed_rules
    assert st.silver_readings(ev).count() == 0
    q = st.quarantine_records(ev, "primary_events_raw", ["reading_id"], st.READING_COLS).collect()
    assert len(q) == 1 and rule in q[0].dq_failed_rules
    assert f'"consumption_kwh":"{row[8]}"' in q[0].raw_record          # raw value preserved untouched


def test_optional_and_warning_fields_are_flagged_not_quarantined(spark, refs):
    ev = run_readings(spark, refs, [reading(tariff=None, volt="999")])
    r = st.silver_readings(ev).collect()[0]
    assert sorted(r.dq_warnings) == ["SM-DQ-008", "SM-DQ-009"]
    assert r.tariff_type == "UNKNOWN" and r.raw_tariff_type is None


# ---------------------------------------------------------------- duplicates
def test_duplicates_resolved_deterministically(spark, refs):
    rows = [reading(file="b01.csv"), reading(file="b10.csv", version="v2"), reading(file="b05.csv")]
    results = []
    for order in (rows, rows[::-1], [rows[1], rows[0], rows[2]]):
        ev = run_readings(spark, refs, order)
        s = st.silver_readings(ev).collect()
        assert len(s) == 1 and s[0]._input_file_name == "b10.csv" and s[0].duplicate_copies == 2
        a = st.duplicate_audit(ev, "primary_events_raw", ["reading_id"]).collect()[0]
        assert (a.copies_received, a.copies_dropped, a.kept_file, a.dropped_files) == (3, 2, "b10.csv", ["b01.csv", "b05.csv"])
        results.append(s[0].asDict())
    assert results[0] == results[1] == results[2]


def test_later_source_ingest_wins_over_file_order(spark, refs):
    rows = [reading(file="b01.csv", kwh="2.0", ingest_ts="2026-07-01T00:20:00"),
            reading(file="b02.csv", kwh="1.0", ingest_ts="2026-07-01T00:05:00")]
    s = st.silver_readings(run_readings(spark, refs, rows)).collect()
    assert len(s) == 1 and s[0].consumption_kwh == 2.0 and s[0]._input_file_name == "b01.csv"


def test_invalid_copy_never_wins_dedup(spark, refs):
    ev = run_readings(spark, refs, [reading(file="b09.csv", kwh="500"), reading(file="b01.csv")])
    assert sorted(r.outcome for r in ev.collect()) == ["KEPT", "QUARANTINED"]
    s = st.silver_readings(ev).collect()[0]
    assert s.consumption_kwh == 1.5 and s._input_file_name == "b01.csv"


# ---------------------------------------------------------------- status CDC
def test_later_revision_becomes_current_and_history_kept(spark, refs):
    _, hist, cur = run_status(spark, refs, [status(seq="1", prev="OPEN", new="COMPLETED"),
                                            status(seq="2", prev="COMPLETED", new="REVERSED", ts="2026-08-10T05:00:00")])
    assert history_rows(hist) == [(0, "OPEN", False, False, True, False),
                                  (1, "COMPLETED", False, False, True, False),
                                  (2, "REVERSED", False, False, True, True)]
    c = cur.collect()[0]
    assert (c.current_status, c.source_sequence, c.status_versions) == ("REVERSED", 2, 3)


def test_late_update_does_not_overwrite_newer_status(spark, refs):
    # seq 2 is ingested first; seq 1 arrives an hour later
    _, hist, cur = run_status(spark, refs, [
        status(seq="2", prev="PENDING", new="COMPLETED", ts="2026-08-10T05:00:00", ingested="2026-11-02 11:00:00"),
        status(seq="1", prev="OPEN", new="PENDING", ts="2026-08-10T01:00:00", ingested="2026-11-02 12:00:00", file="s02.csv")])
    assert history_rows(hist) == [(0, "OPEN", False, False, True, False),
                                  (1, "PENDING", True, False, True, False),
                                  (2, "COMPLETED", False, False, True, True)]
    assert cur.collect()[0].current_status == "COMPLETED"


def test_out_of_order_input_gives_same_result_and_sequence_wins(spark, refs):
    ups = [status(seq="1", prev="OPEN", new="PENDING", ts="2026-08-10T01:00:00"),
           status(seq="2", prev="PENDING", new="FAILED", ts="2026-08-10T05:00:00"),
           status(seq="3", prev="FAILED", new="CANCELLED", ts="2026-08-10T03:00:00")]   # change time earlier than seq 2
    h1, h2 = (history_rows(run_status(spark, refs, u)[1]) for u in (ups, ups[::-1]))
    assert h1 == h2
    assert h1[-1] == (3, "CANCELLED", False, True, True, True)          # flagged out of order, still current


def test_status_chain_mismatch_is_flagged(spark, refs):
    _, hist, _ = run_status(spark, refs, [status(seq="1", prev="FAILED", new="COMPLETED")])   # reading status is OPEN
    assert [r.status_chain_ok for r in hist.orderBy("version").collect()] == [True, False]


def test_invalid_and_duplicate_status_updates(spark, refs):
    ev, hist, cur = run_status(spark, refs, [
        status(rid="R9"), status(seq="x"), status(seq="3", new="BOGUS"),
        status(seq="1", file="s01.csv"), status(seq="1", file="s02.csv")])
    got = {(r.reading_id, r.source_sequence, r._input_file_name): (r.outcome, r.dq_failed_rules) for r in ev.collect()}
    assert got[("R9", "1", "s01.csv")] == ("QUARANTINED", ["SM-DQ-030"])
    assert got[("R1", "x", "s01.csv")] == ("QUARANTINED", ["SM-DQ-031"])
    assert got[("R1", "3", "s01.csv")] == ("QUARANTINED", ["SM-DQ-033"])
    assert got[("R1", "1", "s02.csv")][0] == "KEPT" and got[("R1", "1", "s01.csv")][0] == "DUPLICATE_DROPPED"
    assert hist.count() == 2 and cur.collect()[0].current_status == "COMPLETED"


# ---------------------------------------------------------------- intervals, gaps, reconciliation
GAP_ROWS = [reading(rid="R1", ts="2026-07-01T00:00:00"),
            reading(rid="R2", ts="2026-07-01T10:02:00"),               # +2 min jitter, still slot 1
            reading(rid="R3", ts="2026-07-01T20:00:00", kwh="99"),     # extreme -> quarantined -> slot 2 missing
            reading(rid="R4", ts="2026-07-02T06:00:00"),
            reading(rid="R4", ts="2026-07-02T06:00:00", file="b02.csv")]  # duplicate


def test_expected_intervals_detect_gap(spark, refs):
    ev = run_readings(spark, refs, GAP_ROWS)
    iv = st.build_expected_intervals(ev, st.silver_readings(ev), 600, 5)
    got = [(r.slot, r.expected_ts, r.reading_id, r.is_missing) for r in iv.orderBy("slot").collect()]
    assert got == [(0, TS("2026-07-01T00:00:00"), "R1", False), (1, TS("2026-07-01T10:00:00"), "R2", False),
                   (2, TS("2026-07-01T20:00:00"), None, True), (3, TS("2026-07-02T06:00:00"), "R4", False)]
    iv2 = st.build_expected_intervals(ev, st.silver_readings(ev), 600, 5, period_end="2026-07-02T16:00:00")
    assert iv2.filter("is_missing").count() == 2                         # trailing slot 4 also missing


def test_slot_not_yet_due_is_not_a_gap(spark, refs):
    # M2 next slot is 2026-07-02 06:03, after the data ends at 06:00 -> not expected yet
    rows = GAP_ROWS + [reading(rid="R5", meter="M2", ts="2026-07-01T20:03:00")]
    ev = run_readings(spark, refs, rows)
    iv = st.build_expected_intervals(ev, st.silver_readings(ev), 600, 5)
    assert [(r.slot, r.is_missing) for r in iv.filter("meter_id = 'M2'").collect()] == [(0, False)]
    # a reading up to the tolerance before a slot that lies just past the cut-off still fills it
    rows2 = GAP_ROWS + [reading(rid="R6", ts="2026-07-02T15:58:00")]   # slot 4 expected 16:00, data ends 15:58
    ev2 = run_readings(spark, refs, rows2)
    iv2 = st.build_expected_intervals(ev2, st.silver_readings(ev2), 600, 5)
    assert iv2.filter("slot = 4").collect()[0].reading_id == "R6"


def test_daily_energy_and_reconciliation(spark, refs):
    ev = run_readings(spark, refs, GAP_ROWS)
    s = st.silver_readings(ev)
    daily = st.meter_daily_energy(st.build_expected_intervals(ev, s, 600, 5), s)
    d = {r.reading_date: (r.expected_intervals, r.received_intervals, r.missing_intervals, r.energy_kwh) for r in daily.collect()}
    assert d == {dt.date(2026, 7, 1): (3, 2, 1, 3.0), dt.date(2026, 7, 2): (1, 1, 0, 1.5)}
    rec = {r.reading_date: r for r in st.reconcile_daily(ev, daily).collect()}
    r1 = rec[dt.date(2026, 7, 1)]
    assert (r1.bronze_rows, r1.kept_rows, r1.quarantined_rows, r1.quarantined_kwh) == (3, 2, 1, 99.0)
    assert rec[dt.date(2026, 7, 2)].duplicate_dropped_rows == 1
    assert all(r.status == "PASS" for r in rec.values())
    broken = daily.withColumn("energy_kwh", daily.energy_kwh - 1)        # the check must be able to fail
    assert all(r.status == "FAIL" for r in st.reconcile_daily(ev, broken).collect())
