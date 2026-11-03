# Decision Log

| ID | Date | Decision | Rationale | Owner |
| --- | --- | --- | --- | --- |
| D01 | 2026-11-02 | Source grain = one event row per `reading_id` per source arrival | Playbook Day 1; duplicates and revisions resolved in Silver | Tech Lead |
| D02 | 2026-11-02 | Auto Loader with `trigger(availableNow=True)` and per-source checkpoints | Restart-safe, exactly-once file tracking, schedulable as a job | Ingestion |
| D03 | 2026-11-02 | Event sources kept as STRING in Bronze (`inferColumnTypes=false`) | Preserve raw values for traceability; typing in Silver | Ingestion |
| D04 | 2026-11-02 | No dedup / cleansing in Bronze | Playbook rule: do not edit raw values or delete source duplicates | Ingestion |
| D05 | 2026-11-02 | Schema drift (`source_schema_version`) absorbed with schema evolution + `mergeSchema` | Batches 10-12 add a column; no manual intervention | Ingestion |
| D06 | 2026-11-02 | Checkpoints / schemas for event sources in managed volume `bronze.pipeline_meta` | Keep source volume free of pipeline state | Ingestion |
| D07 | 2026-11-02 | Lineage column convention `_input_file_name`, `_ingestion_timestamp` | Align with existing reference tables (team convention); columns renamed in place via Delta column mapping | Ingestion |
| D08 | 2026-11-02 | Reference entities from staged Azure SQL extracts, labelled TEST SIMULATION | Approved connectivity not available | Ingestion |
| D09 | 2026-11-02 | Extreme reading rule: `consumption_kwh` > 12 kWh (35 rows) | Normal max 11.997, smallest extreme 38.17; to be confirmed against BRS | Quality |
| D10 | 2026-11-03 | DQ rules defined once in `configs/silver_config.json`, published to `silver.dq_rule_catalogue` (rule ID, severity, expected action) | Single source of truth for code, tests and evidence | Quality |
| D11 | 2026-11-03 | CRITICAL rule failure -> row goes to `silver.dq_quarantine` with rule IDs and the untouched raw record; WARNING -> row kept with `dq_warnings`; extremes (SM-DQ-006) quarantined | Invalid mandatory values must not reach KPIs; nothing is silently dropped | Quality |
| D12 | 2026-11-03 | Dedup tie-breaker (readings): `ingest_ts` DESC, `_input_file_name` DESC, `_ingestion_timestamp` DESC, row SHA-256 ASC; only DQ-valid copies compete; audit in `silver.dq_duplicate_audit` | Latest source version wins; hash makes the result independent of input order | Quality |
| D13 | 2026-11-03 | Status CDC ordered by `source_sequence` (authoritative), then change time; version 1 = status on the reading; current = highest sequence. Late or out-of-order updates kept in history and flagged, never overwrite current | Prevents a late lower-sequence update from replacing a newer status | Quality |
| D14 | 2026-11-03 | Status updates / secondary events whose reading is quarantined are quarantined too (SM-DQ-030 / SM-DQ-021) | Silver keeps referential integrity; reprocessed when the reading is fixed | Quality |
| D15 | 2026-11-03 | Expected intervals: 600-minute cadence per meter from its first reading, +/- 5 min tolerance, slot expected only once due; gap = due slot without a valid reading | Source cadence is 10 h (36,000 s +/- 120 s, profiled) | Quality |
| D16 | 2026-11-03 | Silver tables rebuilt in full from Bronze (batch read, overwrite) | Deterministic and idempotent; avoids streaming reads on column-mapped Bronze tables; revisit incremental on Day 4 | Quality |
