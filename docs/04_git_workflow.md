# Git Workflow

## Repository layout

```
capstone_09/
  configs/              pipeline configuration (silver_config.json: DQ rules, interval, dedup)
  docs/                 architecture, mappings, QA summaries, Git workflow, decision log, RAID
  notebooks/bronze/     Bronze ingestion (01_bronze_ingestion_autoloader)
  notebooks/silver/     Silver, DQ, CDC (02_silver_quality_cdc)
  notebooks/gold/       Gold, KPIs (Day 3)
  src/silver/           reusable Silver transform code (silver_transforms.py)
  tests/silver/         pytest unit tests for src/silver
  Readme.md
```

## Branch naming

* `main` - reviewed, released code only (no direct commits).
* `feature/<owner>-<topic>` - new work, e.g. `feature/jeevan-bronze-autoloader`.
* `fix/<owner>-<topic>` - defect fixes.
* Personal working branches (e.g. `jeevan`) are allowed but must be merged to `main` through a PR.

## PR checklist

- [ ] Notebook runs top to bottom on serverless without errors
- [ ] All tests in the notebook PASS (paste the test output in the PR)
- [ ] No writes to `bronze.raw_volume`; Bronze raw values and duplicates untouched
- [ ] No credentials, tokens or passwords in code (use secret scopes)
- [ ] Table / column names follow the agreed convention (`_input_file_name`, `_ingestion_timestamp`)
- [ ] Docs updated (mapping, QA summary, decision log, RAID) when behaviour changes
- [ ] At least one peer review approval before merge
