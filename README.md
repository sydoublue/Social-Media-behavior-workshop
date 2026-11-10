# Academic Q&A user behavior

Upgrade of an ELEC 3544 project. The study asks whether the recent interaction history of a user, and that user's position in a graph built only from recent past edges, predict next-week behavior on Stack Exchange question-and-answer sites.

The domain is academic and professional Q&A. The anchor site is MathOverflow. The replication site is Ask Ubuntu. Super User is an optional later replication. Edge types stay separate: answers to questions (`a2q`), comments on questions (`c2q`), and comments on answers (`c2a`).

**Status: M1 MathOverflow ingest is implemented.** `src/ingest.py` reads the three typed SNAP dumps and writes `edges.parquet` plus an ingest report. The user-week panel, labels, features, and models are not implemented. This README does not report model metrics.

## Research question

Can past weeks of a user's interactions, together with leakage-free network position, predict:

1. **Regression.** The week-over-week change in inbound activity on the target week.
2. **Classification.** Whether that week is ordinary, an externally driven anomalous week, or a self-driven anomalous week.

The cross-site check runs the same protocol from MathOverflow to Ask Ubuntu. Bot detection is outside the mainline study.

Details, label formulas, and the confirmation rule for thresholds are in [docs/experiment-plan.md](docs/experiment-plan.md) and [docs/data-spec.md](docs/data-spec.md). Numeric defaults live in [configs/default.yaml](configs/default.yaml).

## Datasets

Source: Stanford SNAP temporal networks, built from the Stack Exchange data dump. A directed edge `(u, v, t)` means user `u` acted on user `v`'s post at Unix time `t`. Node ids are Stack Exchange `OwnerUserId` values and are comparable only within one site.

| Role | Platform | SNAP page | Published nodes | Published temporal edges |
| --- | --- | --- | --- | --- |
| Anchor | MathOverflow | [sx-mathoverflow](https://snap.stanford.edu/data/sx-mathoverflow.html) | 24,818 | 506,550 |
| Replication | Ask Ubuntu | [sx-askubuntu](https://snap.stanford.edu/data/sx-askubuntu.html) | 159,316 | 964,437 |
| Optional later | Super User | [sx-superuser](https://snap.stanford.edu/data/sx-superuser.html) | 194,085 | 1,443,339 |

Published temporal edges by type (SNAP):

| Platform | `a2q` | `c2q` | `c2a` |
| --- | ---: | ---: | ---: |
| MathOverflow | 107,581 | 203,639 | 195,330 |
| Ask Ubuntu | 280,102 | 327,513 | 356,822 |
| Super User | 430,033 | 479,067 | 534,239 |

On each site those three counts sum to the published union total. The union file itself does not record edge type, so ingest reads the three typed files. Download instructions are in [data/raw/README.md](data/raw/README.md).

Citation: Ashwin Paranjape, Austin R. Benson, and Jure Leskovec. “Motifs in Temporal Networks.” *Proceedings of the Tenth ACM International Conference on Web Search and Data Mining* (WSDM), 2017. <https://doi.org/10.1145/3018661.3018731>

## Repository layout

```text
configs/default.yaml      split ratios, windows, URLs, label defaults
data/raw/                 local SNAP dumps (gitignored except this README)
data/processed/           parquet and ingest reports (gitignored except this README)
docs/experiment-plan.md   study design
docs/data-spec.md         cleaning, panel, labels, leakage rules
notebooks/                empty; no analysis notebook is checked in
outputs/                  future plots and model files (gitignored)
src/                      ingest is implemented; later pipeline modules are not
requirements.txt
```

Pipeline: `ingest` (implemented) → `panel` → `labels` → `features` → `train`. See [src/README.md](src/README.md).

## Setup

Use Python 3.11 or newer. The pins in `requirements.txt` were taken from PyPI on 2026-10-18. Ingest has been run with numpy, pandas, pyarrow, and pyyaml. No model has been trained.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run MathOverflow ingest

From the repository root:

```bash
python -m src.ingest --platform mathoverflow
```

The command loads dataset URLs and published counts from [configs/default.yaml](configs/default.yaml). It downloads any missing typed gzip files into `data/raw/mathoverflow/` and writes:

- `data/processed/mathoverflow/edges.parquet`
- `data/processed/mathoverflow/ingest_report.json`

Those outputs stay untracked. Pass `--redownload` to fetch the dumps again, or `--skip-download` when the three typed files are already on disk. Curl equivalents remain in [data/raw/README.md](data/raw/README.md).

The MathOverflow run matches the published typed counts. A copy of that verified report is [docs/mathoverflow-ingest-report.json](docs/mathoverflow-ingest-report.json). Contract checks that do not need the SNAP dumps:

```bash
python tests/test_ingest.py
```

## Next step

Build the MathOverflow user-week panel (M2) from `edges.parquet`. Start that step only from an ingest report that matches. Ask Ubuntu stays out of scope until the panel exists. The column contract, week rule, and count checks are in [docs/data-spec.md](docs/data-spec.md).

## What this study deliberately leaves out

- Downloaded dumps and processed parquet stay gitignored. No notebooks or model artifacts are checked in.
- No claimed accuracy, F1, or error numbers.
- No bot detector and no other platforms (no mixed social-media graphs, private messages, or Voat).
- No copy of an earlier course notebook. The design in `docs/` is the one to implement.
