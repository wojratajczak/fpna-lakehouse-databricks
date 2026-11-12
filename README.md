# FP&A Lakehouse on Databricks

An end-to-end data pipeline that turns raw ERP cost extracts into a finance-ready star
schema — built on Databricks with Delta Lake, Lakeflow and Unity Catalog.

The domain is deliberate. I spent three years in external audit and now work in Global
FP&A, so the problems this pipeline solves are the ones I meet at month close: late
corrections to a period already reported, cost centres that get reorganised mid-year,
and FX translation that has to use the rate of the period the cost belongs to — not
today's rate.

## Architecture

```
landing/                    bronze                silver                   gold
──────────                  ──────                ──────                   ────
costs_YYYY_MM.csv  ──┐
fx_rates.csv       ──┼─►  raw Delta tables  ─►  cleaned, deduplicated  ─►  fact_cost
chart_of_accounts  ──┤    (Auto Loader,          upserted (MERGE),         dim_account
cost_centers       ──┘     schema evolution)     SCD2 dimensions           dim_cost_center
                                                                           dim_period
                                                                           dim_currency
```

Orchestrated as a single Lakeflow job with data-quality expectations; governed through
Unity Catalog; deployed from this repo with Databricks Asset Bundles and GitHub Actions.

## Source data

`data_generator/generate_source_data.py` produces 24 monthly cost files (~200k rows),
an FX table built from the **National Bank of Poland** public API, and two reference
tables. The generator is standard library only — no dependencies.

```bash
python data_generator/generate_source_data.py --out ./data
```

The data is intentionally imperfect, because clean source data makes for a dishonest
portfolio project. Every defect below exists to be handled somewhere in the pipeline:

| Defect | Volume | Handled in |
|---|---|---|
| 24 separate monthly files | — | bronze — incremental load with Auto Loader |
| Late corrections to closed periods, same business key | ~1,500 rows | silver — `MERGE`, not append |
| Account codes with whitespace, leading zeros, mixed case | ~9% of rows | silver — standardisation |
| Exact duplicate rows | ~1,000 | silver — deduplication |
| Account codes outside the chart of accounts | ~650 | expectations — quarantined |
| Empty amounts | ~360 | expectations — quarantined |
| Cost centre reorganisation (two centres merged into one) | 1 event | gold — SCD type 2 |
| New column `project_code` appears mid-history | from file 13 | bronze — schema evolution |

`data/_manifest.json` records exactly what was generated and with which defects.

## Repository layout

```
.
├── data_generator/     synthetic source data (committed)
├── notebooks/          bronze / silver / gold transformations
├── pipelines/          Lakeflow job + Databricks Asset Bundle
├── docs/               architecture notes and design decisions
└── data/               generated output — NOT committed, see .gitignore
```

## Stack

Databricks (Free Edition) · Delta Lake · PySpark · Spark SQL · Lakeflow Jobs ·
Unity Catalog · Databricks Asset Bundles · GitHub Actions · Power BI

## Status

- [x] Source data generator
- [ ] Bronze — Auto Loader ingestion with schema evolution
- [ ] Silver — cleaning, deduplication, incremental MERGE
- [ ] Gold — star schema with SCD type 2 dimensions
- [ ] Orchestration — Lakeflow job with expectations
- [ ] Governance — Unity Catalog grants and lineage
- [ ] CI/CD — Asset Bundle and GitHub Actions

## Design decisions

Written up as they are made, in [`docs/decisions.md`](docs/decisions.md). The three that
drive most of the pipeline:

1. **Business key is `source_doc`**, not a row hash — a correction has to collide with
   the document it reverses, otherwise the ledger double-counts.
2. **Corrections are reversing entries under the same key**, which is why silver upserts
   rather than appends.
3. **A cost booked to a cost centre that no longer exists keeps its original key** and
   is resolved through the SCD2 dimension as at the posting date, so prior-period reports
   do not change retroactively.
