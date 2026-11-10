# Experiment plan

Academic and professional Stack Exchange Q&A. Anchor: MathOverflow. Replication: Ask Ubuntu. Optional later replication: Super User.

This document is the study design. It is not a results report. Thresholds below match `configs/default.yaml` and are pre-registered defaults to confirm on the validation split. No split has been computed, because the dumps are not in the repo yet.

## 1. Problem

Users on a Q&A site receive answers and comments, and they write answers and comments of their own. The predictive question is whether the recent past is enough to anticipate the next calendar week.

**Research question.** Can past weeks of a user's interactions, and that user's position in a network built only from a recent past window, predict:

1. the week-over-week change in **inbound** activity on the target week (regression);
2. whether the target week is ordinary, an **externally driven** anomalous week, or a **self-driven** anomalous week (classification)?

**Cross-site check.** Specify the pipeline on MathOverflow. Freeze the confirmed config. Repeat it on Ask Ubuntu. Super User is a second replication after Ask Ubuntu, using the same frozen config.

The question is predictive. A good score means the past carries information about the next week. It does not identify a causal effect of network position.

**Mainline exclusions.** Bot detection is not a task, a filter, or a feature family. Post text, tags, votes, and views are absent from these SNAP graphs and are out of scope. Other social platforms are out of scope.

## 2. Data

### 2.1 Sources

| Role | Platform | Page |
| --- | --- | --- |
| Anchor | MathOverflow | <https://snap.stanford.edu/data/sx-mathoverflow.html> |
| Replication | Ask Ubuntu | <https://snap.stanford.edu/data/sx-askubuntu.html> |
| Optional later | Super User | <https://snap.stanford.edu/data/sx-superuser.html> |

Each site is released as three temporal graphs plus a union that discards type. Modeling uses the typed files only.

| Edge type | Meaning of `(src, dst, ts)` |
| --- | --- |
| `a2q` | `src` answered `dst`'s question at time `ts` |
| `c2q` | `src` commented on `dst`'s question at time `ts` |
| `c2a` | `src` commented on `dst`'s answer at time `ts` |

Published SNAP totals (temporal edges): MathOverflow 506,550; Ask Ubuntu 964,437; Super User 1,443,339. Per-type counts are in `configs/default.yaml` and the README. Those numbers are the publisher's, and the ingest report has to reproduce them.

Node ids are `OwnerUserId` values from the Stack Exchange dump. The same integer on two sites is not the same person. Panels are never concatenated on `user_id` alone.

### 2.2 What an edge does not record

Asking a question does not create an edge. The edge appears when someone answers or comments. A burst of inbound `a2q` and `c2q` can be the response to a question the user just asked, and the ask itself is invisible here. "Externally driven" and "self-driven" in this study are operational labels on inbound versus outbound **interactions**, described in section 4. They are not verified motives.

There is no message text, so the study cannot read whether a comment is a correction, a thanks, or a follow-up.

### 2.3 Citation

Ashwin Paranjape, Austin R. Benson, and Jure Leskovec. “Motifs in Temporal Networks.” WSDM 2017. <https://doi.org/10.1145/3018661.3018731>

## 3. Sample construction

The unit of prediction is a user-week `(platform, user_id, t)`, where `t` is the target week, identified by `week_start` (Monday 00:00 UTC of that week). The decision time is the start of week `t`. Everything used as a feature must be timestamped before `t`.

### 3.1 Calendar weeks and zero-fill

Weeks are Monday-start UTC calendar weeks, shared by every user on a platform. A week with no interactions for that user is a real observation of zero, not a missing row.

For each user who appears at least once as `src` or `dst`:

- the panel starts at that user's first `week_start`;
- the panel ends at the platform's last `week_start` across all three edge types;
- every Monday in between is present.

Silent weeks after a user leaves stay in the panel, because a drop of inbound activity to zero is part of the regression target. Building the panel only through each user's last active week would delete those drops.

Ask Ubuntu and Super User panels are large (on the order of 10^5 users times several hundred weeks). That is expected. MathOverflow is the place to prove the builder.

### 3.2 Eligibility

`history_weeks` is the number of panel weeks strictly before `t`. A row is eligible when `history_weeks >= min_history_weeks` (default 8). The first eight weeks of a user's panel remain in `panel.parquet` so the history exists, and they are excluded from fitting and scoring.

Ineligible rows still receive a split label from their calendar week, so the cut can be audited. Training, validation, and test filters keep eligible rows only.

### 3.3 Time split

The split is a single cut on the platform calendar, not a random row split and not a user holdout.

Let `n` be the number of Monday `week_start` values from the platform minimum through the platform maximum.

- `n_train = floor(0.70 * n)`
- `n_val = floor(0.15 * n)`
- `n_test = n - n_train - n_val` (the remainder, so the test share is about 15%)

The earliest `n_train` weeks are train, the next `n_val` are validation, and the rest are test. A row's split is the split of its **target** week `t`. The same user can appear in all three splits. That is the agreed design: it tests forward prediction for users the model has already seen, with a hard time cut.

Feature windows for a validation or test row may reach back into earlier splits. Those edges are already in the past at decision time.

Ask Ubuntu gets the same ratios on **its own** calendar. Its date bounds are not copied from MathOverflow.

Thresholds and hyperparameters are chosen on validation only. The test split is scored once, after that choice is written down.

## 4. Labels

Label columns live in `labels.parquet`, keyed by `(platform, user_id, week_start)`. They are computed from the panel. They are not model features. `z_in` and `z_out` use the target week's counts in the numerator and the past in the baseline; copying them into `features.parquet` would leak the label.

### 4.1 Inbound and outbound

For a non-self-loop edge:

- the event is **outbound** for `src` (the actor);
- the event is **inbound** for `dst` (the owner of the question or answer that was acted on).

Weekly counts, filled with zero when the user has no such events:

- `in_a2q`, `in_c2q`, `in_c2a`, and `in_total = in_a2q + in_c2q + in_c2a`
- `out_a2q`, `out_c2q`, `out_c2a`, and `out_total` defined the same way

Self-loops (`src == dst`) stay in the edge table for the SNAP audit and do not enter these counts.

The regression target uses **total** inbound. Per-type counts stay available for features and for a later sensitivity check. Aggregation happens after the typed edges are stored.

### 4.2 Regression target

Default, for an eligible row with target week `t`:

```text
y_reg(u, t) = in_total(u, t) - in_total(u, t - 1)
```

`in_total(u, t - 1)` is known at decision time. `in_total(u, t)` is the future the model has to anticipate. The difference is signed: inbound can rise or fall. It is not clipped.

A scale-free sensitivity, not the default and not the first reported number, is `log1p(in_total(u, t)) - log1p(in_total(u, t - 1))`. Run it only as a named sensitivity after the default target has a full baseline table.

### 4.3 Anomaly classes

The baseline window is the `B` weeks `[t - B, t - 1]`, with `B = label_baseline_weeks` (default 8). It does not include week `t`. Means and standard deviations are computed on that window, zeros included. Standard deviation is the sample standard deviation (`ddof = 1`).

```text
z_in(u, t)  = (in_total(u, t)  - mean_in)  / (std_in  + epsilon)
z_out(u, t) = (out_total(u, t) - mean_out) / (std_out + epsilon)
```

`epsilon` defaults to `1e-6`.

**Primary rule (z-score, positive direction only).** Default `z_star = 2.0`. This number is a placeholder to confirm on validation. It is not a tuned result.

```text
if z_in  >= z_star and z_in  > z_out:  y_anomaly = externally_driven
elif z_out >= z_star and z_out > z_in: y_anomaly = self_driven
elif z_in >= z_star and z_out >= z_star and z_in == z_out:
    y_anomaly = externally_driven   # tie_break; flag the row
else:
    y_anomaly = normal
```

Drops (`z` negative) stay `normal` under this default. A quiet week is a different event from a burst, and the agreed classes describe anomalous weeks driven by inbound attention versus the user's own answering and commenting.

When the past window is constant, `std` is 0 and the denominator collapses to `epsilon`, so any strictly higher week gets a huge z-score. Flag those rows with `zero_variance_baseline = true`. If validation shows they dominate the anomaly set, the pre-registered response is to revisit `min_event_count` (default 0, meaning off) or to move along the confirmation grid below. Do not invent a new rule on the test set.

**Alternative rule (quantile), used only if `primary_method` is switched to `quantile`.** Default `q = 0.95`, also a placeholder to confirm on validation. A side is anomalous when the target week's count is **strictly greater** than the empirical quantile `q` of that user's baseline window. Class assignment then follows the same comparison as above, using the quantile flags in place of `z >= z_star`, and the larger excess over its own quantile as the tie comparison. Do not AND the z-score rule with the quantile rule.

Quantile 0.95 of an all-zero window is 0, so any positive week flags. That is the same zero-history behavior as the z-score rule, and it is one reason the validation check exists.

### 4.4 Confirming the placeholder on validation

Fit nothing on test while doing this. On MathOverflow **validation** eligible rows, compute under the default `z_star = 2.0`:

- prevalence of non-`normal` labels;
- the smaller class's share among non-`normal` labels (`externally_driven` versus `self_driven`).

The default is **kept** when prevalence is inside `[0.005, 0.30]` and the smaller driver is at least 10% of anomalous validation rows. Definitions of those bounds are `labels.anomaly.degenerate_if` in the config.

If either condition fails, select on the pre-registered grid `{1.5, 2.0, 2.5}` (or the quantile grid `{0.90, 0.95, 0.99}` if the method was switched) by this order: the grid point whose validation prevalence is inside the band, then whose minority-driver share is inside the band, then closest to the original default. Write the chosen value into the run's `label_config.json` before any test score. If no grid point sits inside the band, stop and revise the plan. Do not search a finer grid against test.

Ask Ubuntu inherits the value confirmed on MathOverflow. It does not get its own search.

## 5. Features

Features are stored in `features.parquet` with the target week as the key. Column names use an `f_` prefix. The file contains no `y_*` column and no same-week `in_*` or `out_*` count.

For target week `t`, activity features use weeks `[t - L, t - 1]` with `L = activity_lookback_weeks` (default 8). Graph features use edges whose `week_start` lies in `[t - W, t - 1]` with `W = graph_lookback_weeks` (default 8). With the defaults, `L` and `W` match; the names stay separate so a later change to either window is explicit.

### 5.1 Activity

For each edge type in `{a2q, c2q, c2a}` and for `total`, over the activity window:

- sum of inbound counts, sum of outbound counts;
- the lag-1 value (the count on week `t - 1`).

For totals only:

- mean weekly inbound and mean weekly outbound over the window;
- number of active weeks in the window (`in_total + out_total > 0`);
- weeks since the user's previous inbound event and previous outbound event, counted in calendar weeks, capped at `L + 1` when the panel history inside the cap is silent.

Per-type sums are how edge type enters the model. The regression target remains total inbound change. Collapsing types before this stage is a different study.

### 5.2 Graph position

Build the directed graph from edges in `[t - W, t - 1]` only. Recompute it for every `t`. A user with no edge in the window gets zeros, which are informative.

Version-1 features, on the union of edge types (type is already represented in the activity block):

- in-degree and out-degree (unique neighbors);
- in-strength and out-strength (event counts in the window);
- reciprocity: the fraction of out-neighbors that also have an edge back to the user; 0 when out-degree is 0.

Strength duplicates the activity total sums when `W = L`. Keep both blocks anyway: activity is typed and lagged; the graph block is structural. The write-up should say they overlap under the default windows.

Later, and not required for the first model: per-type graphs, windowed PageRank, clustering. A centrality computed once on the full history is not a later feature. It leaks the future. The data spec, section 8, states that ban.

### 5.3 Allowed covariate

`f_iso_week`, the ISO week number of the target Monday, is known before any interaction in week `t` occurs. It may be included so a model can pick up calendar seasonality.

### 5.4 Identifiers

`user_id` is a key. It is not a covariate, an embedding, or a target-encoded column. Site-specific ids would not survive the Ask Ubuntu check, and they memorize people rather than recent behavior. `platform` is constant within a training run.

## 6. Baselines and models

All models read `f_*` columns only. Scalers and category weights are fit on train rows. Early stopping and the small grids below watch validation. Test is scored after the chosen config is fixed.

### 6.1 Regression

| Name | Prediction |
| --- | --- |
| `zero` | `0` (no week-over-week change) |
| `persistence` | `in_total(t - 1) - in_total(t - 2)` |
| `train_mean` | mean of `y_reg` on eligible train rows |
| `xgboost` | `XGBRegressor` on the feature matrix |

Many weeks are `0 → 0`, so `zero` will look strong on a raw average. Every regression report includes the same metrics on the subset of eligible rows where `in_total(t - 1) > 0` or `in_total(t) > 0`. Compare each model to `zero` on that subset before describing the model as an improvement.

Pre-registered `xgboost` regression grid: `max_depth` in `{3, 6}`, `learning_rate` in `{0.05, 0.1}`, `n_estimators` up to 500 with early stopping on validation MAE. Choose the lowest validation MAE.

### 6.2 Classification

Three classes: `normal`, `externally_driven`, `self_driven`. The model is trained on all eligible rows, including `normal`, because the decision at the start of week `t` includes "this week will be ordinary."

| Name | Prediction |
| --- | --- |
| `majority` | most common train class |
| `logreg` | `LogisticRegression(class_weight="balanced")` on features standardized with a train-only `StandardScaler` |
| `xgboost` | `XGBClassifier` with inverse-frequency sample weights from the train class counts |

Pre-registered `xgboost` classification grid: same depth and learning-rate pairs as regression, early stopping and selection by validation macro-F1.

SHAP on a validation sample is an interpretation step after a model exists. It is not a metric and it is not part of model selection.

### 6.3 Cross-site protocol

1. Finish MathOverflow, including the validation confirmation of `z_star` (or `q`).
2. Freeze `configs` values actually used, including the confirmed threshold, in `label_config.json`.
3. Refit the same model family on Ask Ubuntu's own time split. Retune nothing on Ask Ubuntu test, and do not rerun the threshold grid there.
4. Compare each Ask Ubuntu model with the Ask Ubuntu baselines, not with the MathOverflow scores. The sites differ in size and rate.

An optional follow-up trains on MathOverflow and scores Ask Ubuntu with the frozen weights. It is allowed only because features exclude raw user ids. It is not the replication milestone. Call it out separately if it is run.

Super User, if used, follows step 3 with the config frozen at the MathOverflow confirmation. It does not reopen the grid.

## 7. Evaluation

Report class prevalence and the share of trivial `0 → 0` regression rows next to every score, so a majority or zero baseline is visible.

**Regression**, on eligible test rows, overall and on the non-trivial subset:

- MAE (primary)
- RMSE
- R²

**Classification**, on eligible test rows:

- macro-F1 over the three classes (primary)
- balanced accuracy
- per-class precision and recall
- confusion matrix

**Secondary classification number:** among test rows whose **true** label is non-`normal`, binary F1 of `externally_driven` versus `self_driven`. This is the "split the anomalous weeks" question, conditional on the week actually being anomalous. It is secondary because the model does not observe the label when it predicts.

Accuracy alone is not a success metric. The `normal` class will be the majority under the default threshold.

No metric in this plan has been measured. A future results section should be a table produced by `train`, with the config hash or the `label_config.json` values beside it.

## 8. Milestones

| ID | Deliverable | State |
| --- | --- | --- |
| M0 | Repo scaffold: plan, data spec, config, layout | this change |
| M1 | MathOverflow `edges.parquet` and an ingest report that matches SNAP typed counts | not started |
| M2 | MathOverflow zero-filled `panel.parquet` | not started |
| M3 | `labels.parquet` plus the leakage checks in the data spec | not started |
| M4 | `features.parquet` with the column audit | not started |
| M5 | MathOverflow baselines, validation confirmation of the threshold, one test evaluation | not started |
| M6 | Frozen config repeated on Ask Ubuntu | not started |
| M6b | Optional weight transfer from MathOverflow to Ask Ubuntu | not scheduled |
| M7 | Optional Super User replication | not scheduled |
| M8 | Write-up from the produced tables | not started |

M1 is the next coding step. The acceptance checks for it are at the end of `docs/data-spec.md`.
