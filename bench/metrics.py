"""Turning a list of latencies into the numbers a reader can act on.

Percentiles, not averages. A mean latency hides the tail, and the tail is what a user
experiences: a pipeline averaging 40 ms with a p99 of 4 seconds is not a 40 ms pipeline.
"""

from statistics import mean


def percentile(values, p: float) -> float:
    """Nearest-rank percentile, which needs no interpolation and no assumptions.

    `p` is a fraction, so 0.95 is p95. Returns 0.0 for an empty sample rather than
    raising, because a run that indexed nothing should still produce a report saying so.
    """
    if not values:
        return 0.0
    if not 0 < p <= 1:
        raise ValueError("p must be in (0, 1]")
    ordered = sorted(values)
    rank = max(1, min(len(ordered), int(-(-p * len(ordered) // 1))))
    return ordered[rank - 1]


def summarize(latencies_ms, indexed: int, seconds: float) -> dict:
    return {
        "indexed": indexed,
        "seconds": round(seconds, 2),
        "throughput_eps": round(indexed / seconds, 1) if seconds else 0.0,
        "latency_ms": {
            "min": round(min(latencies_ms), 1) if latencies_ms else 0.0,
            "mean": round(mean(latencies_ms), 1) if latencies_ms else 0.0,
            "p50": round(percentile(latencies_ms, 0.50), 1),
            "p95": round(percentile(latencies_ms, 0.95), 1),
            "p99": round(percentile(latencies_ms, 0.99), 1),
            "max": round(max(latencies_ms), 1) if latencies_ms else 0.0,
        },
    }


def keeping_up(achieved_eps: float, target_eps: float, tolerance: float = 0.95) -> bool:
    """Whether the system sustained the offered rate.

    The whole throughput sweep rests on this: the interesting number is not the highest
    rate attempted but the highest rate actually delivered, and those stop being the same
    at the knee.
    """
    if target_eps <= 0:
        return True
    return achieved_eps >= target_eps * tolerance


def format_table(rows: list, columns: list) -> str:
    if not rows:
        return "(no results)"
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in columns}
    header = "  ".join(c.ljust(widths[c]) for c in columns)
    rule = "  ".join("-" * widths[c] for c in columns)
    body = ["  ".join(str(r.get(c, "")).ljust(widths[c]) for c in columns) for r in rows]
    return "\n".join([header, rule, *body])
