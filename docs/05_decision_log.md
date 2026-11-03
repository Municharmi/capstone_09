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
