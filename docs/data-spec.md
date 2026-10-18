# Data specification

Executable contract for cleaning, the user-week panel, and labels. The next coding step is MathOverflow ingest (milestone M1). No step below has been run.

Defaults that appear as numbers are copied from `configs/default.yaml`. If this document and that file disagree, treat the YAML as the config future code will load, and update both in the same change.

## 1. Source files

For each platform, read the three typed gzip files named in the config. Do not take the modeling table from the union file `sx-<site>.txt.gz`; that file cannot recover `a2q` / `c2q` / `c2a`.

Local paths:

```text
data/raw/mathoverflow/sx-mathoverflow-a2q.txt.gz
data/raw/mathoverflow/sx-mathoverflow-c2q.txt.gz
data/raw/mathoverflow/sx-mathoverflow-c2a.txt.gz
```

Ask Ubuntu and Super User follow the same pattern under `data/raw/askubuntu/` and `data/raw/superuser/`. Download commands are in `data/raw/README.md`. The dumps stay untracked.

Each file is whitespace-separated text, one interaction per line:

```text
SRC DST UNIXTS
```

`SRC` and `DST` are integer user ids. `UNIXTS` is seconds since the Unix epoch. The first line of `sx-mathoverflow-a2q.txt.gz` is data, not a header: `1 4 1254192988`, which is 2009-09-29 02:56:28 UTC. Other files may still contain comments. Skip a line when it is blank or the first non-whitespace character is `#`.

Published temporal-edge totals to recover (also in the YAML):

| Platform | `a2q` | `c2q` | `c2a` | Sum (published union) |
| --- | ---: | ---: | ---: | ---: |
| MathOverflow | 107,581 | 203,639 | 195,330 | 506,550 |
| Ask Ubuntu | 280,102 | 327,513 | 356,822 | 964,437 |
| Super User | 430,033 | 479,067 | 534,239 | 1,443,339 |

The Super User SNAP page titles its third statistics table `sx-mathoverflow-c2q`. The file to download is `sx-superuser-c2q.txt.gz`, and the counts in that table (94,548 nodes, 479,067 temporal edges) are the Super User `c2q` figures used here.

"Edges in static graph" on the SNAP pages collapse repeated pairs. Do not expect `nunique(src, dst)` to equal the temporal-edge total.

## 2. Edge schema

Write `data/processed/{platform}/edges.parquet` with these columns only, in this order:

| Column | Type | Definition |
| --- | --- | --- |
| `platform` | string | `mathoverflow`, `askubuntu`, or `superuser` |
| `edge_type` | string | `a2q`, `c2q`, or `c2a` |
| `src` | int64 | acting user |
| `dst` | int64 | owner of the post that was acted on |
| `ts` | int64 | Unix seconds, as published |
| `week_start` | date | Monday UTC of `ts`; see section 4 |
| `is_self_loop` | bool | `src == dst` |

One row is one published temporal edge. Keep exact duplicate `(edge_type, src, dst, ts)` rows if they occur, and record how many. Duplicate timestamps can be distinct interactions; the release does not include a post id that would let us tell them apart.

Keep original ids. A later matrix format may remap ids, and that map must be one function of the user, shared by `src` and `dst`. Section 8 explains why two encoders are wrong.

## 3. Cleaning procedure

Implement this order in `ingest`. The block is the specification, not a module that already runs.

1. For each edge type, open the typed gzip as text.
2. Drop blank lines and lines whose first non-whitespace character is `#`.
3. Split on whitespace. A data line has exactly three fields. Count lines that do not, and count fields that do not parse as integers. If either count is non-zero, write the counts into the report and stop before publishing the parquet. Do not coerce failures to zero.
4. Name the fields `src`, `dst`, `ts` as int64.
5. Set `platform` and `edge_type`.
6. Set `is_self_loop = (src == dst)`. Keep the row.
7. Concatenate the three types. Do not drop an edge because the same `(src, dst, ts)` appears under two types; those are different actions.
8. Compute `week_start` (section 4).
9. Sort by `ts`, `edge_type`, `src`, `dst` so the file is stable.
10. Write parquet and `ingest_report.json`.

Reference sketch:

```python
import pandas as pd

def week_start_utc(ts: pd.Series) -> pd.Series:
    """Monday date (UTC) of each Unix timestamp. Monday = dayofweek 0."""
    instant = pd.to_datetime(ts, unit="s", utc=True)
    midnight = instant.dt.floor("D")
    monday = midnight - pd.to_timedelta(midnight.dt.dayofweek, unit="D")
    return monday.dt.date

# per typed file, after skipping comments and blank lines:
# part = pd.read_csv(path, sep=r"\s+", header=None, names=["src", "dst", "ts"],
#                     dtype="int64", compression="gzip")
# part["platform"] = platform
# part["edge_type"] = edge_type
# part["is_self_loop"] = part["src"] == part["dst"]
# part["week_start"] = week_start_utc(part["ts"])
```

`pandas.read_csv(..., comment="#")` matches the comment rule when `#` is the first character. Lines with leading whitespace before `#` still need the explicit skip from step 2, or a preprocess pass.

### Ingest report

`data/processed/{platform}/ingest_report.json` must include:

- rows read per `edge_type`, after the comment filter and before any drop;
- rows dropped for bad field counts or non-integer fields (must be 0 to accept the file);
- self-loop rows kept, per edge type;
- exact-duplicate rows, per edge type (extra copies beyond the first);
- min and max `ts`, as integers and as UTC timestamps;
- min and max `week_start`;
- number of distinct users in `src`, in `dst`, and in the union of both, overall and per edge type;
- published comparison: rows per type versus `published_temporal_edges`; distinct endpoint ids per type versus `published_nodes` for that type; union of all endpoint ids versus `published_nodes.union`.

Accept M1 when, for MathOverflow, each typed row count equals the published temporal-edge count and the union of `src` and `dst` equals 24,818. Self-loops are included in that row count. If a count differs, keep the parquet only as a failed run under a different filename and fix the reader. Do not "correct" the file by sampling.

## 4. Calendar weeks

Timezone is UTC. The week key is the Monday of the UTC calendar date of `ts`.

```text
week_start = date(UTC midnight of ts) minus that date's weekday offset
```

with Monday as weekday 0. Store a date, not a zoned timestamp, so joins do not depend on a time component.

Checks after the column exists:

- every `week_start` is a Monday;
- `week_start` equals the Monday computed above for every row (recompute in the check; do not compare the column to itself);
- the platform calendar used later is `date_range` from the minimum `week_start` through the maximum, step 7 days, across **all** edge types. Per-type time spans on the SNAP pages differ (Ask Ubuntu `c2q` is published as 2,047 days against 2,613 for the union). One platform calendar prevents three misaligned panels.

Use `week_start` as the key. Do not key the panel by `(ISO year, ISO week)` alone: the Monday date is harder to mis-join.

Illustrative row only, from the first line of the MathOverflow `a2q` file: `src=1`, `dst=4`, `ts=1254192988` → `week_start=2009-09-28`. That illustration is not a claim about the file's maximum timestamp.

## 5. Edge types

Store `edge_type` on every edge and carry it into weekly count columns. Do not sum types inside ingest.

| Type | Inbound meaning for `dst` | Outbound meaning for `src` |
| --- | --- | --- |
| `a2q` | someone answered this user's question | this user answered someone else's question |
| `c2q` | someone commented on this user's question | this user commented on someone else's question |
| `c2a` | someone commented on this user's answer | this user commented on someone else's answer |

The regression target and the anomaly rule use the sums `in_total` and `out_total`. Features keep the per-type sums (experiment plan, section 5). A later sensitivity may redefine the target as inbound `a2q` only; that is a new label column, not a change to the stored edges.

Asking a question is not an edge type in this release. Do not synthesize an `ask` event by looking forward at the first inbound answer.

## 6. User-week panel and zero-fill

Write `data/processed/{platform}/panel.parquet`.

### 6.1 Who gets rows

A user is anyone who appears in `src` or in `dst` on that platform, self-loops included for membership and excluded from counts.

Let `first(u)` be the minimum `week_start` among edges where `u` is `src` or `dst`. Let `last_platform` be the maximum `week_start` on the platform. The user's index is every Monday from `first(u)` through `last_platform` inclusive.

This end rule (`panel.end: platform_max`) keeps silent weeks after the last observed interaction. Those weeks are how a drop to zero inbound shows up in `y_reg`.

### 6.2 Counts

Before aggregating, drop `is_self_loop` rows from the count input only. They remain in `edges.parquet`.

Aggregate:

- inbound: group the filtered edges by `(dst, week_start, edge_type)`;
- outbound: group by `(src, week_start, edge_type)`.

Pivot edge type to columns. Reindex to the full user-week index and fill missing counts with 0.

| Column | Type | Definition |
| --- | --- | --- |
| `platform` | string | |
| `user_id` | int64 | original id |
| `week_start` | date | target week candidate; also the week the counts describe |
| `in_a2q`, `in_c2q`, `in_c2a` | int32 | inbound events that week |
| `out_a2q`, `out_c2q`, `out_c2a` | int32 | outbound events that week |
| `in_total`, `out_total` | int32 | sums of the three typed counts |
| `active` | bool | `in_total + out_total > 0` |
| `history_weeks` | int32 | number of this user's panel rows with an earlier `week_start` |
| `eligible` | bool | `history_weeks >= min_history_weeks` (default 8) |
| `split` | string | `train`, `val`, or `test` from the calendar cut in section 6.3 |

`history_weeks` is 0 on the user's first panel week. Counts are non-negative. `in_total` equals the sum of the typed inbound columns on every row, including structural zeros.

Panel checks:

- for every user, `week_start` is a continuous Monday sequence from `first(u)` through `last_platform` with no gaps and no duplicates;
- the sum of `in_a2q` over the panel equals the number of non-self-loop `a2q` edges, and the same for the other five typed count columns;
- no count column contains nulls.

### 6.3 Split column

Compute boundaries on the platform Monday calendar (`n` weeks), not on the eligible subset:

```text
n_train = floor(0.70 * n)
n_val   = floor(0.15 * n)
n_test  = n - n_train - n_val
```

Assign `split` by which of those three contiguous blocks contains the row's `week_start`. Early weeks of the calendar are train even when many of their rows have `eligible = false`.

## 7. Labels

Write `data/processed/{platform}/labels.parquet`, one row per panel row. Keys: `platform`, `user_id`, `week_start`. Also write `label_config.json` with the threshold values used.

### 7.1 Inbound change

```text
y_inbound_change(u, t) = in_total(u, t) - in_total(u, t - 1)
```

Null when `t - 1` is outside the user's panel. Eligible rows have eight prior weeks, so their regression target is defined. The value is a signed int32. Do not clip it, and do not replace it with a percent change in the default column.

### 7.2 Z-scores

Baseline weeks are `[t - B, t - 1]` with `B = 8`, all inside the panel for eligible rows. Population of the baseline includes zeros.

```text
z_in  = (in_total(t)  - mean(in_total over baseline))  / (stdev(in_total over baseline, ddof=1)  + 1e-6)
z_out = (out_total(t) - mean(out_total over baseline)) / (stdev(out_total over baseline, ddof=1) + 1e-6)
```

Store `z_in`, `z_out` as float64. Store `zero_variance_baseline` true when either baseline standard deviation is 0. A constant-zero history gives a huge positive `z` on any week with a positive count, because the denominator is only `epsilon`. That behavior is accepted until the validation check in the experiment plan says otherwise.

These z-scores are labels and diagnostics. They use week `t`. They must not be copied into the feature matrix.

### 7.3 Class

Default method `zscore`, positive direction, `z_threshold = 2.0` (placeholder; confirm on validation as specified in the experiment plan):

```text
if z_in >= 2 and z_in > z_out:
    y_anomaly = "externally_driven"
elif z_out >= 2 and z_out > z_in:
    y_anomaly = "self_driven"
elif z_in >= 2 and z_out >= 2 and z_in == z_out:
    y_anomaly = "externally_driven"
    tie_break_applied = true
else:
    y_anomaly = "normal"
    tie_break_applied = false
```

Weeks with a large negative z stay `normal`. `min_event_count` defaults to 0. If a future confirmed config sets it above 0, a side can meet the z-score rule only when that side's week-`t` count is also at least `min_event_count`.

**Quantile alternative**, only when `primary_method` is `quantile`. Placeholder `q = 0.95`. A side flags when its week-`t` count is strictly greater than the empirical quantile `q` of its baseline window. Assign the class by the same comparison, using the flags and the excess over each side's quantile. Do not require both the z-score and the quantile flag.

Class values are exactly `normal`, `externally_driven`, and `self_driven`.

### 7.4 Where week `t` is allowed

| Object | May use counts from week `t`? |
| --- | --- |
| `edges.parquet` | stores them; it is not a feature matrix |
| `panel.parquet` counts on the row for week `t` | yes, the row describes week `t` |
| `y_inbound_change`, `z_in`, `z_out`, `y_anomaly` | yes, in the label file only |
| any `f_*` column | no |

## 8. Features and anti-leakage rules

The feature file is specified here so the label file is not built in a way that makes leakage easy. Milestone M4 implements it. Column list and model grids are in the experiment plan, sections 5 and 6.

Key the feature row by the **target** week `t`. Activity columns read `[t - 8, t - 1]`. Graph columns read edges with `week_start` in `[t - 8, t - 1]`. ISO week of Monday `t` is allowed. `user_id` is a key only.

Rules for anyone writing `features` or `train`:

1. **Past only.** For a row with target week `t`, no feature may depend on an edge with `week_start >= t`, or on a panel count from week `t` or later.
2. **Graph window.** Degree, strength, and reciprocity are recomputed from edges inside `[t - W, t - 1]`. A single graph built on every edge in the dump is not a feature source.
3. **Label isolation.** `features.parquet` columns are keys plus `f_*`. A build fails the column audit if it contains `z_in`, `z_out`, `y_inbound_change`, `y_anomaly`, `in_total`, `out_total`, or any unprefixed `in_*` / `out_*` count.
4. **Train-only fitting.** StandardScaler, class weights, and the xgboost early-stopping watch all fit on train or, for early stopping, on validation. Refitting them on all rows before the split is leakage.
5. **Threshold confirmation uses validation.** The grid in the experiment plan does not read test labels. Ask Ubuntu uses the threshold MathOverflow already confirmed.
6. **One id space.** `src` value `k` and `dst` value `k` are the same user. Encode or embed users with one map, or leave the raw id as a key and omit it from `X`.
7. **Site ids do not join.** Never bind MathOverflow user `k` to Ask Ubuntu user `k`.

Acceptance checks to automate with M3 and M4:

- **Delete-future check.** Drop all edges with `week_start >= t`, rebuild features for target week `t`, and require equality with the original feature row.
- **Perturb-present check.** Add a fake edge inside week `t` and require feature row `t` to stay the same. The label row for `t` may change.
- **Perturb-future check.** Add a fake edge inside week `t + 1` and require both the feature row and the label row at `t` to stay the same.
- **Window membership.** Every edge that contributes to the graph features at `t` has `week_start` in `[t - W, t - 1]`.

Looking backward from a test week into the train period is allowed. Those interactions had already happened.

## 9. Constructions that break the study

These are easy shortcuts that make a later metric incomparable with this plan.

1. **Random edge samples.** Drawing a fraction of edges independently deletes arbitrary weeks of a user's series and biases the sample toward high-degree users. If a smoke test needs less data, sample **users** and keep every edge of each sampled user. Smoke-test numbers are not study results, and the SNAP count check will not pass on a sample. Report metrics from the full typed graphs.
2. **Separate factorization of `src` and `dst`.** Fitting one `LabelEncoder`, vocabulary, or embedding matrix on the `src` column and another on `dst` assigns the same person two identities. Graph features and the user panel then fail to meet. Any remap is a single function of user id, applied to both ends of each edge.
3. **A full-history static graph.** Computing degree, PageRank, a community id, or a node embedding once on all timestamps, then joining it back to early weeks, feeds future answers and comments into past rows. Window the graph as in section 8. A descriptive picture of the whole network belongs in an appendix and is not an input to `train`.
4. **Random K-fold on rows, or a user holdout in place of the time cut.** The agreed evaluation is the 70/15/15 calendar split. A user holdout answers a different question (cold-start users) and is not a substitute.
5. **Baselines that include the target week or the future.** User-level means and standard deviations for `z_in` and `z_out` use `[t - B, t - 1]` only. A z-score taken over a user's entire panel uses the future and the week being labeled.
6. **Dropping `edge_type` at ingest, or reading only the union file.** Types cannot be reconstructed afterward. Aggregate in the panel and feature steps.
7. **Bot filters and bot labels.** High-volume accounts stay in the panel. The mainline study does not classify or remove bots.
8. **Cross-site user joins, post text, votes, or views.** They are not in these files. Do not merge an outside dump into the mainline table without a plan revision.

## 10. Milestone M1 checklist (MathOverflow ingest)

- [ ] Typed gz files downloaded under `data/raw/mathoverflow/` and left untracked (`git status` does not list them).
- [ ] `data/processed/mathoverflow/edges.parquet` has exactly the columns in section 2.
- [ ] Row counts: `a2q` 107,581, `c2q` 203,639, `c2a` 195,330.
- [ ] Distinct users in the union of `src` and `dst`: 24,818. Per type: `a2q` 21,688, `c2q` 16,836, `c2a` 13,840.
- [ ] Every `week_start` is a Monday and matches section 4.
- [ ] Self-loop count and duplicate count are in `ingest_report.json`, and self-loops were not deleted.
- [ ] Bad-parse count is 0.
- [ ] Ask Ubuntu is not required for this checklist.

Panel construction (M2) starts only after that report matches.
