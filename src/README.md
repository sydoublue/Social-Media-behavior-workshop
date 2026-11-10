# `src` package

Importing this package does not load data or train a model.

| Module | Responsibility | State |
| --- | --- | --- |
| `ingest` | Read the three typed SNAP dumps for one platform into `edges.parquet`. | implemented (`python -m src.ingest`) |
| `panel` | Build the zero-filled user-week panel from past and current counts. | not implemented |
| `labels` | Attach next-week inbound change and the anomaly class. | not implemented |
| `features` | Build past-only activity features and graph features on `[t-W, t-1]`. | not implemented |
| `train` | Fit the pre-registered baselines on the time split and write metrics. | not implemented |

`ingest` follows `docs/data-spec.md` and `configs/default.yaml`. The MathOverflow run is the one checked in for M1. Later modules should be real implementations, not `NotImplementedError` stubs.
