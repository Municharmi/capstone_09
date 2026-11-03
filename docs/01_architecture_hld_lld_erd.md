# Capstone 09 Smart Meter - Architecture (Day 1)

## Scope and source grain

* Business goal: reliable daily energy KPIs per meter / substation with interval completeness and late-read reconciliation.
* Source grain (Bronze): **one event row per `reading_id` per source arrival**. Duplicates, nulls, extremes and later status revisions are kept in Bronze and resolved in Silver.
* BRS: not available in the workspace; this document captures the requirements taken from the Implementation Playbook. Link the BRS here when provided.

## HLD

```mermaid
flowchart LR
  subgraph Sources
    ADLS[ADLS Gen2<br/>meter events, aux events, status CDC]
    SQL[Azure SQL<br/>meter / substation reference<br/>TEST SIMULATION: staged CSV extracts]
  end
  subgraph Databricks [Databricks + Unity Catalog: capstone9_smartmeter]
    VOL[External volume<br/>bronze.raw_volume]
    AL[Auto Loader<br/>availableNow, checkpoints]
    BR[Bronze<br/>raw Delta, append-only]
    SV[Silver<br/>typed, DQ, dedup, CDC]
    GD[Gold<br/>daily KPIs]
  end
  JOB[Lakeflow Jobs DAG]
  WH[SQL Warehouse<br/>AI/BI dashboard, Genie]
  ADLS --> VOL
  SQL --> VOL
  VOL --> AL --> BR --> SV --> GD --> WH
  JOB -. orchestrates .-> AL
  JOB -. orchestrates .-> SV
  JOB -. orchestrates .-> GD
```

## LLD (Day 1 - Bronze)

| Component | Implementation |
| --- | --- |
| Catalog / schema | `capstone9_smartmeter.bronze` |
| Source volume (read-only) | `bronze.raw_volume` (EXTERNAL, `abfss://capstone09@smartmeter09.dfs.core.windows.net/smartmeterdata`) |
| Ingestion | Auto Loader `cloudFiles` CSV, `header=true`, `inferColumnTypes=false` (raw STRING), `trigger(availableNow=True)`, append |
| Schema / checkpoint locations | `bronze.pipeline_meta/_schemas/<source>`, `bronze.pipeline_meta/_checkpoints/<source>` |
| Lineage columns | `_input_file_name` (`_metadata.file_path`), `_ingestion_timestamp` (`current_timestamp()`), `_rescued_data` |
| Schema drift | New column `source_schema_version` (batches 10-12) added automatically; `mergeSchema=true` on write |
| Event tables | `primary_events_raw`, `secondary_events_raw`, `status_updates_raw` (notebook `day1/01_bronze_ingestion_autoloader`) |
| Reference tables | `bronze_meter_id`, `bronze_substation_id` (Ingestion owner, Auto Loader from `azure_sql_extracts/`) |

## ERD (logical)

```mermaid
erDiagram
  METER ||--o{ READING : "meter_id"
  SUBSTATION ||--o{ READING : "substation_id"
  READING ||--o{ SECONDARY_EVENT : "reading_id"
  SUBSTATION ||--o{ SECONDARY_EVENT : "related_entity_id"
  READING ||--o{ STATUS_UPDATE : "reading_id"
  METER { string meter_id PK string region string active_flag date effective_date string risk_tier }
  SUBSTATION { string substation_id PK string region string active_flag date effective_date string risk_tier }
  READING { string reading_id string meter_id FK string substation_id FK string feeder_id timestamp event_timestamp double consumption_kwh double voltage_v string tariff_type string meter_status }
  SECONDARY_EVENT { string event_id PK string reading_id FK string related_entity_id FK string event_type double event_value }
  STATUS_UPDATE { string reading_id FK int source_sequence timestamp change_timestamp string previous_status string new_status string change_reason }
```

`reading_id` is not unique in Bronze (40 exact duplicates); it becomes the business key of the Silver reading after deduplication.


