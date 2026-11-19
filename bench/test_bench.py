"""Tests for the parts that decide what the benchmark reports.

The measurement code is the part that must not be wrong. A pipeline that is slow and a
pipeline that is measured wrong look identical in a report, so the percentile logic, the
parser and the loss accounting are tested here, where no broker is needed.

    pytest bench/
"""

import pytest

from bench.metrics import keeping_up, percentile, summarize
from pipeline.consumer import Run
from pipeline.schema import parse_line, unwrap, wrap


# --------------------------------------------------------------------- percentiles

def test_percentile_is_nearest_rank():
    values = list(range(1, 101))  # 1..100
    assert percentile(values, 0.50) == 50
    assert percentile(values, 0.95) == 95
    assert percentile(values, 0.99) == 99
    assert percentile(values, 1.0) == 100


def test_percentile_on_an_empty_sample_returns_zero_rather_than_raising():
    # A run that indexed nothing must still produce a report that says so.
    assert percentile([], 0.95) == 0.0


def test_percentile_rejects_an_out_of_range_p():
    with pytest.raises(ValueError):
        percentile([1, 2, 3], 0)
    with pytest.raises(ValueError):
        percentile([1, 2, 3], 1.5)


def test_percentile_of_one_value():
    assert percentile([7.0], 0.5) == 7.0
    assert percentile([7.0], 0.99) == 7.0


def test_summary_reports_the_tail_not_only_the_mean():
    # 99 fast events and one very slow one: the mean hides what p99 shows.
    latencies = [10.0] * 99 + [5000.0]
    s = summarize(latencies, indexed=100, seconds=1.0)
    assert s["latency_ms"]["mean"] < 60
    assert s["latency_ms"]["p99"] == 10.0
    assert s["latency_ms"]["max"] == 5000.0


def test_throughput_is_zero_rather_than_dividing_by_zero():
    assert summarize([], 0, 0.0)["throughput_eps"] == 0.0


# --------------------------------------------------------------------- keeping up

def test_keeping_up_allows_a_small_shortfall_but_not_a_large_one():
    assert keeping_up(960, 1000) is True      # within 5%
    assert keeping_up(800, 1000) is False
    assert keeping_up(5, 0) is True           # no target to miss


# --------------------------------------------------------------------- the envelope

def test_envelope_round_trips():
    payload = wrap('127.0.0.1 - - [01/Jul/1995:00:00:01 +0000] "GET / HTTP/1.0" 200 12',
                   produced_ns=123456789, seq=42)
    back = unwrap(payload)
    assert back["t"] == 123456789
    assert back["seq"] == 42
    assert back["line"].startswith("127.0.0.1")


# --------------------------------------------------------------------- the parser

def test_parse_a_well_formed_line():
    line = ('host1.example.net - - [01/Jul/1995:12:30:45 +0000] '
            '"GET /images/logo.gif HTTP/1.0" 200 4523')
    f = parse_line(line)
    assert f["host"] == "host1.example.net"
    assert f["method"] == "GET"
    assert f["path"] == "/images/logo.gif"
    assert f["status"] == 200
    assert f["size"] == 4523
    assert f["timestamp"].startswith("1995-07-01T12:30:45")


def test_a_dash_for_size_means_zero_bytes_not_a_crash():
    line = 'h - - [01/Jul/1995:00:00:01 +0000] "GET /x HTTP/1.0" 304 -'
    assert parse_line(line)["size"] == 0


def test_a_malformed_line_is_rejected_rather_than_raising():
    # Real logs contain these. Dying on one would stop the pipeline; counting them is
    # what lets the benchmark show that a fast parser is not silently dropping input.
    assert parse_line("this is not a log line") == {}
    assert parse_line("") == {}


def test_a_request_with_no_protocol_still_parses():
    line = 'h - - [01/Jul/1995:00:00:01 +0000] "GET /x" 200 10'
    f = parse_line(line)
    assert f["method"] == "GET" and f["path"] == "/x" and f["protocol"] == ""


# --------------------------------------------------------------------- loss counting

def test_gaps_count_missing_sequence_numbers():
    run = Run()
    run.seen_seq = {0, 1, 3, 4}   # 2 never arrived
    run.max_seq = 4
    assert run.gaps == 1


def test_a_duplicate_does_not_hide_a_loss():
    # Totals alone would net out: 4 received, 4 distinct expected, nothing wrong.
    # Counting the sequence numbers shows one lost and one duplicated.
    run = Run()
    run.seen_seq = {0, 1, 3}
    run.max_seq = 3
    run.duplicates = 1
    assert run.gaps == 1
    assert run.duplicates == 1


def test_no_events_means_no_gaps():
    assert Run().gaps == 0
