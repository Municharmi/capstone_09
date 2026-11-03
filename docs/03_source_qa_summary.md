# Source QA Summary (Day 1)

Evidence: notebook `day1/01_bronze_ingestion_autoloader` (profile and test cells). Bronze was also compared row for row with the raw CSV files (EXCEPT ALL in both directions, incl. file path): 0 differences for all three event sources.

## Primary events (`primary_events_raw`)

| Check | Expected | Actual |
| --- | --- | --- |
| Rows / files | 11,540 | 11,540 / 12 files (01-11: 1,000 each, 12: 540) |
| Exact duplicates (business columns) | 40 | 40 |
| Distinct `reading_id` | 11,500 | 11,500 |
| Optional nulls (`tariff_type`) | 12 | 12 (no other column has nulls) |
| Extreme values (`consumption_kwh` > 12 kWh) | 35 | 35 (normal range 0.101-11.997, extremes 38.17-888.83) |
| Type-cast failures (kWh, voltage, event/ingest timestamp) | 0 | 0 |
| Schema drift rows (`source_schema_version = v2`) | batches 10-12 only | 2,540, all in batches 10-12 |
| `meter_status` distribution | - | COMPLETED 2,842 / FAILED 2,884 / OPEN 2,851 / PENDING 2,963 |

## Other sources

| Source | Rows | Distinct key | Exact duplicates | Rows with nulls | Type failures |
| --- | --- | --- | --- | --- | --- |
| `secondary_events_raw` | 310 | 310 `event_id` | 0 | 0 | 0 |
| `status_updates_raw` | 520 | 520 `reading_id` | 0 | 0 | 0 |
| `bronze_meter_id` | 150 | 150 `meter_id` | 0 | 0 | 0 |
| `bronze_substation_id` | 110 | 110 `substation_id` | 0 | 0 | 0 |

`status_updates.new_status`: CANCELLED 179 / COMPLETED 159 / REVERSED 182 (one revision per reading, `source_sequence` 1..520).

## Key coverage (orphans)

| Relationship | Orphans |
| --- | --- |
| primary.meter_id -> bronze_meter_id | 0 |
| primary.substation_id -> bronze_substation_id | 0 |
| secondary.reading_id -> primary | 0 |
| secondary.related_entity_id -> bronze_substation_id | 0 |
| status_updates.reading_id -> primary | 0 |

## Day 1 tests

| Test | Result |
| --- | --- |
| TC01 rows 11,540 / 40 duplicates / 11,500 unique | PASS |
| TC02 12 nulls / 35 extremes | PASS |
| Replay of unchanged files adds no rows (early TC03/TC06 evidence at Bronze) | PASS |
| Every row has `_input_file_name` and `_ingestion_timestamp` | PASS |

## Known limitations

* Extreme threshold (> 12 kWh) is derived from the data distribution; confirm with the BRS.
* Azure SQL connectivity is simulated with staged CSV extracts.
* TC03 / TC06 must be re-proven on curated Silver / Gold data.
