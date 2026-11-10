# Processed tables

Generated parquet and JSON reports belong here and are gitignored except this README and `.gitkeep`. Nothing has been generated yet.

Expected layout once ingest and the later steps exist (one directory per platform: `mathoverflow`, `askubuntu`, `superuser`):

- `edges.parquet` — cleaned typed interactions
- `panel.parquet` — zero-filled user-week counts
- `labels.parquet` — regression target and anomaly class
- `features.parquet` — past-only model matrix
- `ingest_report.json` — row counts against the SNAP published totals
- `label_config.json` — threshold values used for that run

Column definitions are in `docs/data-spec.md`.
