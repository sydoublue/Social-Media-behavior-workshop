# Processed tables

Generated parquet and JSON reports belong here and are gitignored except this README and `.gitkeep`.

`python -m src.ingest --platform mathoverflow` writes `mathoverflow/edges.parquet` and `mathoverflow/ingest_report.json`. A verified copy of that report is committed at `docs/mathoverflow-ingest-report.json`. Panel, label, and feature files are not produced yet.

Expected layout once ingest and the later steps exist (one directory per platform: `mathoverflow`, `askubuntu`, `superuser`):

- `edges.parquet` — cleaned typed interactions
- `panel.parquet` — zero-filled user-week counts
- `labels.parquet` — regression target and anomaly class
- `features.parquet` — past-only model matrix
- `ingest_report.json` — row counts against the SNAP published totals
- `label_config.json` — threshold values used for that run

Column definitions are in `docs/data-spec.md`.
