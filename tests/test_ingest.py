"""Contract checks for typed-edge ingest. No SNAP download required.

Run from the repository root:

    python tests/test_ingest.py
"""

from __future__ import annotations

import gzip
import sys
import tempfile
import traceback
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingest import (  # noqa: E402
    EDGE_COLUMNS,
    IngestError,
    load_config,
    parse_typed_gzip,
    raw_destination,
    run_ingest,
    week_start_utc,
)
import pandas as pd  # noqa: E402


def _write_gz(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="\n") as handle:
        handle.write(text)


def _run(directory: Path, files: dict[str, str], published_edges: dict, published_nodes: dict):
    typed = {}
    for edge_type, text in files.items():
        path = directory / f"{edge_type}.txt.gz"
        _write_gz(path, text)
        typed[edge_type] = path
    return run_ingest(
        platform="fixture",
        typed_paths=typed,
        published_temporal_edges=published_edges,
        published_nodes=published_nodes,
        edge_types=list(files),
        report_path=directory / "ingest_report.json",
        accepted_parquet_path=directory / "edges.parquet",
    )


def test_week_start_example() -> None:
    mapped = week_start_utc(pd.Series([1254192988], dtype="int64")).iloc[0]
    assert mapped == date(2009, 9, 28), mapped
    # The next Monday 00:00 UTC stays on that Monday.
    next_monday = datetime(2009, 10, 5, tzinfo=timezone.utc)
    mapped_monday = week_start_utc(
        pd.Series([int(next_monday.timestamp())], dtype="int64")
    ).iloc[0]
    assert mapped_monday == date(2009, 10, 5)
    sunday_before = next_monday - timedelta(seconds=1)
    mapped_sunday = week_start_utc(
        pd.Series([int(sunday_before.timestamp())], dtype="int64")
    ).iloc[0]
    assert mapped_sunday == date(2009, 9, 28)


def test_keeps_comments_self_loops_duplicates_and_cross_type() -> None:
    later = int(datetime(2009, 10, 5, 3, 0, tzinfo=timezone.utc).timestamp())
    a2q = "\n".join(
        [
            "# comment",
            "   # indented comment",
            "",
            "   ",
            "1 4 1254192988",
            "  2 2 1254192988",
            "2 2 1254192988",
            f"3 4 {later}",
            "",
        ]
    )
    c2q = "\n".join(
        [
            "1 4 1254192988",
            "5 6 1254279388",
        ]
    )
    c2a = "7 8 1254365788\n"
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        report = _run(
            directory,
            {"a2q": a2q, "c2q": c2q, "c2a": c2a},
            {"a2q": 4, "c2q": 2, "c2a": 1, "union": 7},
            {"a2q": 4, "c2q": 4, "c2a": 2, "union": 8},
        )
        assert report["accepted"] is True, report
        assert report["bad_parse_count"] == 0
        assert report["rows_read"]["per_edge_type"] == {"a2q": 4, "c2q": 2, "c2a": 1}
        assert report["rows_dropped"]["total"] == 0
        assert report["self_loops_kept"]["per_edge_type"]["a2q"] == 2
        assert report["self_loops_kept"]["total"] == 2
        assert report["exact_duplicate_rows"]["per_edge_type"]["a2q"] == 1
        assert report["exact_duplicate_rows"]["total"] == 1
        assert report["src_dst_ts_under_multiple_types"]["keys"] == 1
        assert report["src_dst_ts_under_multiple_types"]["rows"] == 2
        assert report["distinct_users"]["overall"]["union"] == 8
        assert report["week_start"]["min"] == "2009-09-28"
        assert report["week_start"]["max"] == "2009-10-05"
        table = pq.read_table(directory / "edges.parquet")
        assert table.column_names == list(EDGE_COLUMNS)
        frame = table.to_pandas()
        assert len(frame) == 7
        assert not (directory / "edges.failed.parquet").exists()
        # Same (src, dst, ts) under two types is two rows.
        pair = frame[(frame["src"] == 1) & (frame["dst"] == 4) & (frame["ts"] == 1254192988)]
        assert set(pair["edge_type"]) == {"a2q", "c2q"}
        loops = frame[frame["is_self_loop"]]
        assert len(loops) == 2
        assert set(loops["src"]) == {2}
        # Sort is ts, edge_type, src, dst. The duplicate self-loop stays.
        ordered = list(zip(frame["ts"], frame["edge_type"], frame["src"], frame["dst"]))
        assert ordered == sorted(ordered)
        assert frame["week_start"].tolist()[0] == date(2009, 9, 28)


def test_bad_parse_does_not_publish_or_coerce() -> None:
    text = "\n".join(
        [
            "# keep me out",
            "1 2",
            "1 2 3 4",
            "1 x 3",
            "4 5 6",
            "7 8 00",
        ]
    )
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        report = _run(
            directory,
            {"a2q": text, "c2q": "1 2 3\n", "c2a": "1 2 3\n"},
            {"a2q": 1, "c2q": 1, "c2a": 1, "union": 3},
            {"a2q": 1, "c2q": 2, "c2a": 2, "union": 2},
        )
        assert report["accepted"] is False
        assert report["stop_reasons"] == ["bad_parse"]
        assert report["rows_read"]["per_edge_type"]["a2q"] == 5
        assert report["bad_field_count_lines"]["per_edge_type"]["a2q"] == 2
        assert report["non_integer_fields"]["per_edge_type"]["a2q"] == 2
        assert report["rows_dropped"]["per_edge_type"]["a2q"] == 4
        assert report["bad_parse_count"] == 4
        assert report["parquet_path"] is None
        assert not (directory / "edges.parquet").exists()
        assert not (directory / "edges.failed.parquet").exists()
        # The good sibling files are not published either.
        assert report["rows_read"]["per_edge_type"]["c2q"] == 1


def test_count_mismatch_keeps_failed_filename_only() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        accepted = directory / "edges.parquet"
        accepted.write_text("stale", encoding="utf-8")
        report = _run(
            directory,
            {"a2q": "1 2 1254192988\n", "c2q": "3 4 1254192988\n", "c2a": "5 6 1254192988\n"},
            {"a2q": 9, "c2q": 1, "c2a": 1, "union": 3},
            {"a2q": 2, "c2q": 2, "c2a": 2, "union": 6},
        )
        assert report["accepted"] is False
        assert "published_count_mismatch" in report["stop_reasons"]
        assert not accepted.exists()
        failed = directory / "edges.failed.parquet"
        assert failed.exists()
        frame = pq.read_table(failed).to_pandas()
        assert len(frame) == 3
        assert report["published_comparison"]["temporal_edges"]["a2q"]["match"] is False
        assert report["published_comparison"]["nodes"]["union"]["match"] is True


def test_parser_skips_only_comments_and_blanks() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "sample.txt.gz"
        _write_gz(path, "# c\n\n  # c\n1 2 3\n")
        parsed = parse_typed_gzip(path)
        assert parsed["rows_read"] == 1
        assert parsed["rows"] == [(1, 2, 3)]
        assert parsed["rows_dropped"] == 0


def test_config_mathoverflow_urls_are_typed() -> None:
    config = load_config(ROOT / "configs" / "default.yaml")
    platform = config["datasets"]["platforms"]["mathoverflow"]
    edges = platform["published_temporal_edges"]
    nodes = platform["published_nodes"]
    assert edges == {"a2q": 107581, "c2q": 203639, "c2a": 195330, "union": 506550}
    assert nodes == {"union": 24818, "a2q": 21688, "c2q": 16836, "c2a": 13840}
    assert list(config["edge_types"]) == ["a2q", "c2q", "c2a"]
    for edge_type, url in platform["files"].items():
        name = urllib.parse.urlparse(url).path.rsplit("/", 1)[-1]
        assert name == f"sx-mathoverflow-{edge_type}.txt.gz"
        destination = raw_destination(ROOT / "data" / "raw", "mathoverflow", edge_type, url)
        assert destination.name == name
    try:
        raw_destination(
            ROOT / "data" / "raw",
            "mathoverflow",
            "a2q",
            "https://snap.stanford.edu/data/sx-mathoverflow.txt.gz",
        )
    except IngestError:
        pass
    else:
        raise AssertionError("union file URL was accepted")


def main() -> int:
    tests = [
        test_week_start_example,
        test_parser_skips_only_comments_and_blanks,
        test_keeps_comments_self_loops_duplicates_and_cross_type,
        test_bad_parse_does_not_publish_or_coerce,
        test_count_mismatch_keeps_failed_filename_only,
        test_config_mathoverflow_urls_are_typed,
    ]
    failed = 0
    for test in tests:
        try:
            test()
        except Exception:
            failed += 1
            print(f"FAIL {test.__name__}", flush=True)
            traceback.print_exc()
        else:
            print(f"ok {test.__name__}", flush=True)
    if failed:
        print(f"{failed} test(s) failed", flush=True)
        return 1
    print(f"{len(tests)} tests passed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
