# `src` package

This package is a placeholder. Importing it does not load data or train a model.

Planned modules, in the order the pipeline should grow:

| Module | Responsibility |
| --- | --- |
| `ingest` | Read the three typed SNAP dumps for one platform into `edges.parquet`. |
| `panel` | Build the zero-filled user-week panel from past and current counts. |
| `labels` | Attach next-week inbound change and the anomaly class. |
| `features` | Build past-only activity features and graph features on `[t-W, t-1]`. |
| `train` | Fit the pre-registered baselines on the time split and write metrics. |

The first module to add is `ingest`, starting with MathOverflow. Follow `docs/data-spec.md` and `configs/default.yaml`. Do not add a module that only raises `NotImplementedError`.
