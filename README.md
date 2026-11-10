# Academic Q&A user behavior

Upgrade of an ELEC 3544 project. The study asks whether the recent interaction history of a user, and that user's position in a graph built only from recent past edges, predict next-week behavior on Stack Exchange question-and-answer sites.

The domain is academic and professional Q&A. The anchor site is MathOverflow. The replication site is Ask Ubuntu. Super User is an optional later replication. Edge types stay separate: answers to questions (`a2q`), comments on questions (`c2q`), and comments on answers (`c2a`).

**Status: scaffold.** The plan, the data contract, the directory layout, and the dependency list are in the repo. No SNAP dump has been added, no pipeline module has been written, and no model has been trained. This README does not report metrics.

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
data/processed/           future parquet tables (gitignored except this README)
docs/experiment-plan.md   study design
docs/data-spec.md         cleaning, panel, labels, leakage rules
notebooks/                empty; no analysis notebook is checked in
outputs/                  future plots and model files (gitignored)
src/                      package placeholder; no pipeline modules yet
requirements.txt
```

Planned pipeline, none of it implemented: `ingest` → `panel` → `labels` → `features` → `train`. See [src/README.md](src/README.md).

## Setup

Use Python 3.11 or newer. The pins in `requirements.txt` were taken from PyPI on 2026-10-18 and have not been exercised by a training run.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Then download the MathOverflow typed dumps into `data/raw/mathoverflow/` using the commands in [data/raw/README.md](data/raw/README.md). Leave those files untracked.

## Next step

Implement MathOverflow ingest and check the typed row counts against the SNAP totals in `configs/default.yaml`. The column contract, week rule, and count checks are in [docs/data-spec.md](docs/data-spec.md). Ask Ubuntu waits until that ingest report matches.

## What this scaffold deliberately leaves out

- No downloaded graphs, processed tables, notebooks, or model artifacts.
- No claimed accuracy, F1, or error numbers.
- No bot detector and no other platforms (no mixed social-media graphs, private messages, or Voat).
- No copy of an earlier course notebook. The design in `docs/` is the one to implement.
