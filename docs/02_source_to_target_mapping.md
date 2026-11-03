# Source-to-Target Mapping (Day 1 - Bronze)

All paths are under `/Volumes/capstone9_smartmeter/bronze/raw_volume/`. Bronze copies every source column 1:1 without transformation (STRING for event sources), plus lineage columns. Silver / Gold targets are added on Day 2 / Day 3.

## Table level

| Source path | Origin | Target table | Load | Rows |
| --- | --- | --- | --- | --- |
| `adls/primary_events/meter_batch_01..12.csv` | ADLS Gen2 | `bronze.primary_events_raw` | Auto Loader, append | 11,540 |
| `adls/secondary_events/secondary_events.csv` | ADLS Gen2 | `bronze.secondary_events_raw` | Auto Loader, append | 310 |
| `adls/status_updates/status_updates.csv` | ADLS Gen2 (CDC) | `bronze.status_updates_raw` | Auto Loader, append | 520 |
| `azure_sql_extracts/meter_id.csv` | Azure SQL - TEST SIMULATION (staged extract) | `bronze.bronze_meter_id` | Auto Loader, append, typed | 150 |
| `azure_sql_extracts/substation_id.csv` | Azure SQL - TEST SIMULATION (staged extract) | `bronze.bronze_substation_id` | Auto Loader, append, typed | 110 |

## Column level

| Target table | Source column -> target column (1:1) | Bronze type | Notes |
| --- | --- | --- | --- |
| `primary_events_raw` | reading_id, event_timestamp, ingest_timestamp, substation_id, meter_id, tariff_type, meter_status, unit, consumption_kwh, voltage_v, feeder_id, source_system | STRING | `tariff_type` optional (12 nulls) |
| `primary_events_raw` | source_schema_version | STRING | Only in batches 10-12 (`v2`), NULL for 01-09 |
| `secondary_events_raw` | event_id, reading_id, event_timestamp, related_entity_id, event_type, event_value, source_system | STRING | `related_entity_id` = substation_id |
| `status_updates_raw` | reading_id, change_timestamp, previous_status, new_status, change_reason, source_sequence, source_system | STRING | Ordering key for CDC: `source_sequence` |
| `bronze_meter_id` | meter_id, display_name, region, active_flag, effective_date, risk_tier | inferred (effective_date DATE) | |
| `bronze_substation_id` | substation_id, display_name, region, active_flag, effective_date, risk_tier | inferred (effective_date DATE) | |
| all | `_metadata.file_path` -> `_input_file_name` | STRING | lineage |
| all | `current_timestamp()` -> `_ingestion_timestamp` | TIMESTAMP | lineage |
| all | unparsable values -> `_rescued_data` | STRING | 0 rows today |
