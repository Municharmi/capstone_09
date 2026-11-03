# RAID Tracker

| ID | Type | Description | Impact | Action / Mitigation | Owner | Status |
| --- | --- | --- | --- | --- | --- | --- |
| R01 | Risk | Azure SQL credentials are hardcoded in plain text in a draft notebook (`Drafts/Smart Meter Bronze Ingestion`) | Security exposure | Remove from notebook, store in a secret scope, rotate the password | Ingestion | Open |
| R02 | Risk | Reference-table Auto Loader checkpoints/schemas are stored inside the source volume `raw_volume` | Source area mixes data and pipeline state | Move to `bronze.pipeline_meta` on next reload | Ingestion | Open |
| R03 | Risk | Extreme-value rule (> 12 kWh) inferred from data, not from BRS | Wrong DQ classification | Confirm threshold with BRS / client | Quality | Open |
| A01 | Assumption | Azure SQL connectivity not approved; staged CSV extracts used as TEST SIMULATION | Reference data may differ from live system | Replace with approved connection when available | Ingestion | Open |
| A02 | Assumption | `related_entity_id` in secondary events refers to `substation_id` | Join correctness | Verified: 0 orphans; confirm in BRS | Tech Lead | Open |
| I01 | Issue | BRS document not available in the workspace | Traceability matrix incomplete | Obtain BRS and link it in `01_architecture_hld_lld_erd.md` | Tech Lead | Open |
| I02 | Issue | Reference tables have no table comment marking them as TEST SIMULATION | Labelling requirement (Day 1 step 7) | Owner to add table comments | Ingestion | Open |
| I03 | Issue | `status_updates` has one revision per reading | TC04 cannot be shown with source data alone | Covered on Day 2 by unit tests in `tests/silver/test_silver_transforms.py` (later revision, late update, out-of-order input); real data shows 0 late / out-of-order rows | Quality | Closed |
| D01 | Dependency | Silver (Day 2) depends on Bronze tables and lineage column names | Rework if renamed | Freeze Bronze contract (see mapping doc) | Quality | Open |
