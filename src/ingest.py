"""Typed SNAP edge ingest.

Reads the three typed gzip files for one platform (``a2q``, ``c2q``, ``c2a``)
and writes ``edges.parquet`` plus ``ingest_report.json``. The cleaning order
is the one in ``docs/data-spec.md`` section 3. The user-week panel, labels,
and features are not built here.

Run from the repository root:

    python -m src.ingest --platform mathoverflow
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

EDGE_COLUMNS: tuple[str, ...] = (
    "platform",
    "edge_type",
    "src",
    "dst",
    "ts",
    "week_start",
    "is_self_loop",
)

EDGE_SCHEMA = pa.schema(
    [
        ("platform", pa.string()),
        ("edge_type", pa.string()),
        ("src", pa.int64()),
        ("dst", pa.int64()),
        ("ts", pa.int64()),
        ("week_start", pa.date32()),
        ("is_self_loop", pa.bool_()),
    ]
)

_INT64_MIN = -2**63
_INT64_MAX = 2**63 - 1
_EXAMPLE_TS = 1254192988
_EXAMPLE_WEEK = "2009-09-28"
_GZIP_MAGIC = b"\x1f\x8b"
_USER_AGENT = "elec3544-ingest/1.0 (academic Q&A user-behavior study)"


class IngestError(Exception):
    """A download, config, or reader failure that is not a counted bad parse."""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if not isinstance(loaded, dict):
        raise IngestError(f"config at {path} must be a YAML mapping")
    return loaded


def week_start_utc(ts: pd.Series) -> pd.Series:
    """Monday date (UTC) of each Unix timestamp. Monday = dayofweek 0."""
    instant = pd.to_datetime(ts, unit="s", utc=True)
    midnight = instant.dt.floor("D")
    monday = midnight - pd.to_timedelta(midnight.dt.dayofweek, unit="D")
    return monday.dt.date


def _classify_token(token: str) -> tuple[str, int | None]:
    """Return ``ok``, ``not_int``, or ``out_of_range`` for one field."""
    if token.startswith("+") or token == "":
        return "not_int", None
    try:
        value = int(token, 10)
    except ValueError:
        return "not_int", None
    # Reject leading zeros and other spellings int() would coerce.
    if str(value) != token:
        return "not_int", None
    if value < _INT64_MIN or value > _INT64_MAX:
        return "out_of_range", None
    return "ok", value


def parse_typed_gzip(path: Path) -> dict:
    """Parse one typed edge file.

    Blank lines and ``#`` comments are skipped. Every other line is a row
    read. A row is kept only when it has exactly three int64 fields.
    """
    rows: list[tuple[int, int, int]] = []
    stats = {
        "rows_read": 0,
        "bad_field_count_lines": 0,
        "non_integer_fields": 0,
        "int64_out_of_range_fields": 0,
        "rows_dropped": 0,
    }
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            for line in handle:
                content = line.strip()
                if not content or content[0] == "#":
                    continue
                stats["rows_read"] += 1
                fields = content.split()
                if len(fields) != 3:
                    stats["bad_field_count_lines"] += 1
                    stats["rows_dropped"] += 1
                    _count_bad_tokens(fields, stats)
                    continue
                parsed: list[int] = []
                bad_line = False
                for token in fields:
                    kind, value = _classify_token(token)
                    if kind == "ok" and value is not None:
                        parsed.append(value)
                    else:
                        bad_line = True
                        if kind == "not_int":
                            stats["non_integer_fields"] += 1
                        else:
                            stats["int64_out_of_range_fields"] += 1
                if bad_line:
                    stats["rows_dropped"] += 1
                    continue
                rows.append((parsed[0], parsed[1], parsed[2]))
    except gzip.BadGzipFile as exc:
        raise IngestError(f"{path} is not a valid gzip file: {exc}") from exc
    except UnicodeDecodeError as exc:
        raise IngestError(f"{path} is not UTF-8 text: {exc}") from exc
    except EOFError as exc:
        raise IngestError(f"{path} ended before the gzip stream finished: {exc}") from exc
    except OSError as exc:
        raise IngestError(f"could not read {path}: {exc}") from exc
    stats["rows"] = rows
    return stats


def _count_bad_tokens(fields: list[str], stats: dict) -> None:
    for token in fields:
        kind, _value = _classify_token(token)
        if kind == "not_int":
            stats["non_integer_fields"] += 1
        elif kind == "out_of_range":
            stats["int64_out_of_range_fields"] += 1


def _per_type(edge_types: Sequence[str], values: Mapping[str, int]) -> dict:
    per_type = {edge_type: int(values[edge_type]) for edge_type in edge_types}
    return {
        "per_edge_type": per_type,
        "total": int(sum(per_type.values())),
    }


def _endpoint_counts(src: pd.Series, dst: pd.Series) -> dict[str, int]:
    if src.empty and dst.empty:
        union = 0
    else:
        union = int(pd.unique(pd.concat([src, dst], ignore_index=True)).size)
    return {
        "src": int(pd.unique(src).size),
        "dst": int(pd.unique(dst).size),
        "union": union,
    }


def _duplicate_extras(part: pd.DataFrame) -> int:
    if part.empty:
        return 0
    return int(part.duplicated(["src", "dst", "ts"], keep="first").sum())


def _multi_type_src_dst_ts(edges: pd.DataFrame) -> dict[str, int]:
    """Rows whose ``(src, dst, ts)`` also appears under another edge type.

    These rows are kept. They are different actions, not duplicates.
    """
    if edges.empty:
        return {"keys": 0, "rows": 0}
    nunique = edges.groupby(["src", "dst", "ts"], sort=False)["edge_type"].nunique()
    multi = nunique[nunique > 1]
    if multi.empty:
        return {"keys": 0, "rows": 0}
    marked = edges.set_index(["src", "dst", "ts"]).index.isin(multi.index)
    return {"keys": int(len(multi)), "rows": int(marked.sum())}


def _format_utc(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _example_week_start() -> str:
    mapped = week_start_utc(pd.Series([_EXAMPLE_TS], dtype="int64")).iloc[0]
    return mapped.isoformat()


def _week_start_problems(edges: pd.DataFrame) -> list[str]:
    problems: list[str] = []
    if _example_week_start() != _EXAMPLE_WEEK:
        problems.append(
            f"ts={_EXAMPLE_TS} mapped to {_example_week_start()}, expected {_EXAMPLE_WEEK}"
        )
    if edges.empty:
        return problems
    recomputed = week_start_utc(edges["ts"]).reset_index(drop=True)
    stored = edges["week_start"].reset_index(drop=True)
    if not recomputed.equals(stored):
        problems.append("week_start does not match the section 4 Monday rule")
    week_dates = pd.to_datetime(stored)
    if not bool((week_dates.dt.dayofweek == 0).all()):
        problems.append("a week_start value is not a Monday")
    return problems


def build_edge_table(
    platform: str,
    kept_rows: Mapping[str, list[tuple[int, int, int]]],
    edge_types: Sequence[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for edge_type in edge_types:
        frame = pd.DataFrame(
            kept_rows[edge_type],
            columns=["src", "dst", "ts"],
            dtype="int64",
        )
        frame["platform"] = platform
        frame["edge_type"] = edge_type
        frame["is_self_loop"] = frame["src"].eq(frame["dst"])
        frames.append(frame)
    edges = pd.concat(frames, ignore_index=True)
    edges["week_start"] = week_start_utc(edges["ts"])
    edges = edges.sort_values(
        ["ts", "edge_type", "src", "dst"],
        kind="mergesort",
    ).reset_index(drop=True)
    return edges.loc[:, list(EDGE_COLUMNS)]


def write_edges_parquet(edges: pd.DataFrame, path: Path) -> None:
    missing = [column for column in EDGE_COLUMNS if column not in edges.columns]
    if missing:
        raise IngestError(f"edge table is missing columns: {missing}")
    ordered = edges.loc[:, list(EDGE_COLUMNS)]
    table = pa.table(
        {
            "platform": pa.array(ordered["platform"].tolist(), type=pa.string()),
            "edge_type": pa.array(ordered["edge_type"].tolist(), type=pa.string()),
            "src": pa.array(ordered["src"].to_numpy(dtype="int64", copy=False), type=pa.int64()),
            "dst": pa.array(ordered["dst"].to_numpy(dtype="int64", copy=False), type=pa.int64()),
            "ts": pa.array(ordered["ts"].to_numpy(dtype="int64", copy=False), type=pa.int64()),
            "week_start": pa.array(list(ordered["week_start"]), type=pa.date32()),
            "is_self_loop": pa.array(
                ordered["is_self_loop"].to_numpy(dtype="bool", copy=False),
                type=pa.bool_(),
            ),
        },
        schema=EDGE_SCHEMA,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
    written = pq.read_schema(path)
    if list(written.names) != list(EDGE_COLUMNS):
        raise IngestError(f"wrote unexpected columns to {path}: {written.names}")
    for field in EDGE_SCHEMA:
        if written.field(field.name).type != field.type:
            raise IngestError(
                f"{path} column {field.name} has type {written.field(field.name).type}, "
                f"expected {field.type}"
            )


def _display_path(path: Path, root: Path | None) -> str:
    if root is None:
        return path.as_posix()
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _remove(path: Path) -> None:
    if path.exists():
        path.unlink()


def run_ingest(
    *,
    platform: str,
    typed_paths: Mapping[str, Path],
    published_temporal_edges: Mapping[str, int],
    published_nodes: Mapping[str, int],
    edge_types: Sequence[str],
    report_path: Path,
    accepted_parquet_path: Path,
    source_urls: Mapping[str, str] | None = None,
    repo_root_path: Path | None = None,
    schema_version: int | None = None,
) -> dict:
    """Parse typed files, then write the report and, if accepted, the parquet."""
    missing = [edge_type for edge_type in edge_types if edge_type not in typed_paths]
    if missing:
        raise IngestError(f"missing typed files for {platform}: {missing}")

    parsed = {edge_type: parse_typed_gzip(typed_paths[edge_type]) for edge_type in edge_types}
    rows_read = {edge_type: parsed[edge_type]["rows_read"] for edge_type in edge_types}
    bad_lines = {edge_type: parsed[edge_type]["bad_field_count_lines"] for edge_type in edge_types}
    non_integers = {edge_type: parsed[edge_type]["non_integer_fields"] for edge_type in edge_types}
    out_of_range = {
        edge_type: parsed[edge_type]["int64_out_of_range_fields"] for edge_type in edge_types
    }
    dropped = {edge_type: parsed[edge_type]["rows_dropped"] for edge_type in edge_types}
    bad_parse_total = int(sum(dropped.values()))
    urls = source_urls or {}
    sources = {
        edge_type: {
            "url": urls.get(edge_type),
            "path": _display_path(typed_paths[edge_type], repo_root_path),
            "sha256": _sha256(typed_paths[edge_type]),
        }
        for edge_type in edge_types
    }

    failed_parquet_path = accepted_parquet_path.with_name(
        f"{accepted_parquet_path.stem}.failed{accepted_parquet_path.suffix}"
    )
    report: dict = {
        "platform": platform,
        "schema_version": schema_version,
        "accepted": False,
        "stop_reasons": [],
        "sources": sources,
        "rows_read": {
            "definition": (
                "lines remaining after blank lines and '#' comments are skipped, "
                "before any bad-parse drop"
            ),
            **_per_type(edge_types, rows_read),
        },
        "rows_dropped": {
            "definition": (
                "rows dropped because the line does not have exactly three fields "
                "or a field is not an int64; must be 0 to accept the file"
            ),
            **_per_type(edge_types, dropped),
        },
        "bad_field_count_lines": {
            "definition": "lines whose whitespace-separated field count is not 3",
            **_per_type(edge_types, bad_lines),
        },
        "non_integer_fields": {
            "definition": "fields that do not parse as base-10 integers",
            **_per_type(edge_types, non_integers),
        },
        "int64_out_of_range_fields": {
            "definition": "integer fields that do not fit in int64",
            **_per_type(edge_types, out_of_range),
        },
        "bad_parse_count": bad_parse_total,
        "parquet_path": None,
    }

    if bad_parse_total != 0 or any(out_of_range.values()) or any(non_integers.values()) or any(bad_lines.values()):
        report["stop_reasons"] = ["bad_parse"]
        report["self_loops_kept"] = None
        report["exact_duplicate_rows"] = None
        report["src_dst_ts_under_multiple_types"] = None
        report["ts"] = None
        report["week_start"] = None
        report["week_start_checks"] = {"example_ts_1254192988": _example_week_start()}
        report["distinct_users"] = None
        report["published_comparison"] = None
        _remove(accepted_parquet_path)
        _remove(failed_parquet_path)
        _write_json(report_path, report)
        return report

    edges = build_edge_table(
        platform,
        {edge_type: parsed[edge_type]["rows"] for edge_type in edge_types},
        edge_types,
    )
    week_problems = _week_start_problems(edges)
    per_type_users = {}
    self_loops = {}
    duplicates = {}
    for edge_type in edge_types:
        part = edges.loc[edges["edge_type"] == edge_type]
        per_type_users[edge_type] = _endpoint_counts(part["src"], part["dst"])
        self_loops[edge_type] = int(part["is_self_loop"].sum())
        duplicates[edge_type] = _duplicate_extras(part)
    overall_users = _endpoint_counts(edges["src"], edges["dst"])
    published_edges, edges_match = _compare_counts(
        edges,
        per_type_users,
        overall_users,
        published_temporal_edges,
        published_nodes,
        edge_types,
    )
    stop_reasons: list[str] = []
    if not edges_match:
        stop_reasons.append("published_count_mismatch")
    if week_problems:
        stop_reasons.append("week_start_rule")

    if edges.empty:
        ts_block = None
        week_block = None
    else:
        ts_min = int(edges["ts"].min())
        ts_max = int(edges["ts"].max())
        week_min = min(edges["week_start"]).isoformat()
        week_max = max(edges["week_start"]).isoformat()
        ts_block = {
            "min": ts_min,
            "max": ts_max,
            "min_utc": _format_utc(ts_min),
            "max_utc": _format_utc(ts_max),
        }
        week_block = {"min": week_min, "max": week_max}

    accepted = not stop_reasons
    parquet_path = accepted_parquet_path if accepted else failed_parquet_path
    if accepted:
        _remove(failed_parquet_path)
    else:
        _remove(accepted_parquet_path)
    write_edges_parquet(edges, parquet_path)

    report.update(
        {
            "accepted": accepted,
            "stop_reasons": stop_reasons,
            "parquet_path": _display_path(parquet_path, repo_root_path),
            "self_loops_kept": {
                "definition": "rows with src == dst; self-loops are kept",
                **_per_type(edge_types, self_loops),
            },
            "exact_duplicate_rows": {
                "definition": (
                    "extra copies beyond the first of (edge_type, src, dst, ts), "
                    "counted inside each edge type; copies are kept"
                ),
                **_per_type(edge_types, duplicates),
            },
            "src_dst_ts_under_multiple_types": {
                "definition": (
                    "kept rows whose (src, dst, ts) occurs under more than one edge type; "
                    "types are not collapsed"
                ),
                **_multi_type_src_dst_ts(edges),
            },
            "ts": ts_block,
            "week_start": week_block,
            "week_start_checks": {
                "example_ts_1254192988": _example_week_start(),
                "every_value_is_monday": "week_start_rule" not in stop_reasons,
                "matches_section_4_rule": "week_start_rule" not in stop_reasons,
                "problems": week_problems,
            },
            "distinct_users": {
                "definition": "distinct ids in src, in dst, and in the union of both",
                "overall": overall_users,
                "per_edge_type": per_type_users,
            },
            "published_comparison": published_edges,
        }
    )
    _write_json(report_path, report)
    return report


def _compare_counts(
    edges: pd.DataFrame,
    per_type_users: Mapping[str, Mapping[str, int]],
    overall_users: Mapping[str, int],
    published_temporal_edges: Mapping[str, int],
    published_nodes: Mapping[str, int],
    edge_types: Sequence[str],
) -> tuple[dict, bool]:
    temporal: dict = {}
    nodes: dict = {}
    matched = True
    for edge_type in edge_types:
        observed_rows = int((edges["edge_type"] == edge_type).sum())
        published_rows = int(published_temporal_edges[edge_type])
        row_match = observed_rows == published_rows
        temporal[edge_type] = {
            "observed": observed_rows,
            "published_temporal_edges": published_rows,
            "match": row_match,
        }
        observed_nodes = int(per_type_users[edge_type]["union"])
        published = int(published_nodes[edge_type])
        node_match = observed_nodes == published
        nodes[edge_type] = {
            "distinct_endpoints": observed_nodes,
            "published_nodes": published,
            "match": node_match,
        }
        matched = matched and row_match and node_match
    observed_union_rows = int(len(edges))
    published_union_rows = int(published_temporal_edges["union"])
    union_row_match = observed_union_rows == published_union_rows
    temporal["union"] = {
        "observed": observed_union_rows,
        "published_temporal_edges": published_union_rows,
        "match": union_row_match,
    }
    observed_union_nodes = int(overall_users["union"])
    published_union_nodes = int(published_nodes["union"])
    union_node_match = observed_union_nodes == published_union_nodes
    nodes["union"] = {
        "distinct_endpoints": observed_union_nodes,
        "published_nodes": published_union_nodes,
        "match": union_node_match,
    }
    matched = matched and union_row_match and union_node_match
    return {"temporal_edges": temporal, "nodes": nodes}, matched


def raw_destination(raw_root: Path, platform: str, edge_type: str, url: str) -> Path:
    """Local path for one typed URL. Rejects the untyped union dump."""
    name = urllib.parse.urlparse(url).path.rsplit("/", 1)[-1]
    expected_suffix = f"-{edge_type}.txt.gz"
    if not name.endswith(expected_suffix):
        raise IngestError(
            f"config URL for {platform} {edge_type} must end with {expected_suffix} "
            f"(the union file is not a modeling source); got {url}"
        )
    return raw_root / platform / name


def _gzip_magic(path: Path) -> bool:
    with path.open("rb") as handle:
        return handle.read(2) == _GZIP_MAGIC


def ensure_raw_file(url: str, dest: Path, *, redownload: bool) -> str:
    """Download ``url`` to ``dest`` when needed. Returns ``downloaded`` or ``existing``."""
    if dest.exists() and not redownload:
        if not _gzip_magic(dest):
            raise IngestError(f"{dest} exists but is not a gzip file; rerun with --redownload")
        return "existing"
    dest.parent.mkdir(parents=True, exist_ok=True)
    temporary = dest.with_name(dest.name + ".partial")
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=180) as response, temporary.open("wb") as out:
            shutil.copyfileobj(response, out)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        _remove(temporary)
        raise IngestError(f"failed to download {url}: {exc}") from exc
    if not _gzip_magic(temporary):
        _remove(temporary)
        raise IngestError(f"download from {url} is not a gzip file")
    temporary.replace(dest)
    return "downloaded"


def ingest_platform(
    *,
    config_path: Path,
    platform: str | None = None,
    redownload: bool = False,
    skip_download: bool = False,
) -> dict:
    config = load_config(config_path)
    root = repo_root()
    selected = platform or config["datasets"]["anchor"]
    try:
        platform_cfg = config["datasets"]["platforms"][selected]
        edge_types = list(config["edge_types"])
        files = platform_cfg["files"]
    except KeyError as exc:
        raise IngestError(f"config is missing {exc} for platform {selected}") from exc
    raw_root = root / config["paths"]["raw_dirname"]
    typed_paths: dict[str, Path] = {}
    source_urls: dict[str, str] = {}
    for edge_type in edge_types:
        if edge_type not in files:
            raise IngestError(f"config has no file URL for {selected} {edge_type}")
        url = str(files[edge_type])
        source_urls[edge_type] = url
        destination = raw_destination(raw_root, selected, edge_type, url)
        typed_paths[edge_type] = destination
        if skip_download:
            if not destination.exists():
                raise IngestError(f"missing raw file {destination}; run without --skip-download")
        else:
            action = ensure_raw_file(url, destination, redownload=redownload)
            print(f"{action}: {destination.relative_to(root)}", flush=True)
    accepted_path = root / config["outputs"]["edges"].format(platform=selected)
    report_path = root / config["outputs"]["ingest_report"].format(platform=selected)
    report = run_ingest(
        platform=selected,
        typed_paths=typed_paths,
        published_temporal_edges=platform_cfg["published_temporal_edges"],
        published_nodes=platform_cfg["published_nodes"],
        edge_types=edge_types,
        report_path=report_path,
        accepted_parquet_path=accepted_path,
        source_urls=source_urls,
        repo_root_path=root,
        schema_version=config.get("schema_version"),
    )
    _print_summary(report, report_path)
    return report


def _print_summary(report: dict, report_path: Path) -> None:
    print(f"platform: {report['platform']}", flush=True)
    print(f"accepted: {report['accepted']}", flush=True)
    print(f"bad_parse_count: {report['bad_parse_count']}", flush=True)
    rows = report["rows_read"]["per_edge_type"]
    print("rows_read: " + ", ".join(f"{key}={value}" for key, value in rows.items()), flush=True)
    if report.get("distinct_users"):
        users = report["distinct_users"]
        print(f"distinct_users_union: {users['overall']['union']}", flush=True)
        for edge_type, counts in users["per_edge_type"].items():
            print(f"distinct_users_{edge_type}: {counts['union']}", flush=True)
        loops = report["self_loops_kept"]
        duplicates = report["exact_duplicate_rows"]
        print(f"self_loops_kept: {loops['total']} {loops['per_edge_type']}", flush=True)
        print(
            f"exact_duplicate_rows: {duplicates['total']} {duplicates['per_edge_type']}",
            flush=True,
        )
        print(f"ts: {report['ts']}", flush=True)
        print(f"week_start: {report['week_start']}", flush=True)
    if report["stop_reasons"]:
        print(f"stop_reasons: {', '.join(report['stop_reasons'])}", flush=True)
    print(f"report: {report_path}", flush=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download and ingest typed SNAP temporal edges for one platform."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="YAML config. Default: configs/default.yaml in the repository root.",
    )
    parser.add_argument(
        "--platform",
        default=None,
        help="Platform key in the config. Default: datasets.anchor.",
    )
    parser.add_argument(
        "--redownload",
        action="store_true",
        help="Download the typed gzip files again even if they already exist.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Use raw files already on disk and do not contact the network.",
    )
    args = parser.parse_args(argv)
    if args.redownload and args.skip_download:
        print("choose only one of --redownload and --skip-download", file=sys.stderr)
        return 2
    config_path = args.config or (repo_root() / "configs" / "default.yaml")
    try:
        report = ingest_platform(
            config_path=config_path,
            platform=args.platform,
            redownload=args.redownload,
            skip_download=args.skip_download,
        )
    except IngestError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 0 if report["accepted"] else 1


if __name__ == "__main__":
    sys.exit(main())
