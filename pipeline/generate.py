"""Generate a Common Log Format file, so the benchmark runs without a download.

The real NASA July 1995 access log is the better input and the pipeline reads it
unchanged: drop it in and pass --file. This generator exists so that someone cloning the
repository can reproduce the measurements in one command, and because a benchmark whose
first step is "obtain a 200 MB file from a dead FTP server" does not get reproduced.

The distribution is deliberately skewed rather than uniform: a handful of hosts and paths
account for most requests, as in real traffic, so Elasticsearch sees realistic term
cardinality instead of a flat one.
"""

import argparse
import random
from datetime import datetime, timedelta, timezone

PATHS = [
    ("/", 0.18), ("/index.html", 0.12), ("/images/logo.gif", 0.10),
    ("/shuttle/missions/sts-71/mission-sts-71.html", 0.08),
    ("/shuttle/countdown/", 0.07), ("/images/nasa-logo.gif", 0.06),
    ("/software/winvn/winvn.html", 0.05), ("/history/apollo/apollo-13/", 0.05),
    ("/cgi-bin/imagemap/countdown", 0.04), ("/ksc.html", 0.04),
]
STATUS = [(200, 0.86), (304, 0.07), (404, 0.04), (302, 0.02), (500, 0.01)]
METHODS = [("GET", 0.94), ("HEAD", 0.04), ("POST", 0.02)]


def _pick(weighted):
    r, acc = random.random(), 0.0
    for value, weight in weighted:
        acc += weight
        if r <= acc:
            return value
    return weighted[-1][0]


def _hosts(n: int) -> list:
    """A few heavy hosts and a long tail, which is what real logs look like."""
    heavy = [f"host{i}.example.net" for i in range(n // 50 or 1)]
    return heavy * 40 + [f"{random.randint(1, 223)}.{random.randint(0, 255)}."
                         f"{random.randint(0, 255)}.{random.randint(1, 254)}"
                         for _ in range(n // 10 or 1)]


def generate(path: str, count: int, seed: int = 0) -> None:
    random.seed(seed)
    hosts = _hosts(count)
    start = datetime(1995, 7, 1, tzinfo=timezone.utc)

    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for i in range(count):
            when = start + timedelta(seconds=i // 12, milliseconds=(i % 12) * 83)
            status = _pick(STATUS)
            size = "-" if status in (304, 404) else random.randint(200, 90_000)
            fh.write(
                f'{random.choice(hosts)} - - '
                f'[{when.strftime("%d/%b/%Y:%H:%M:%S %z")}] '
                f'"{_pick(METHODS)} {_pick(PATHS)} HTTP/1.0" {status} {size}\n'
            )


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="data/access.log")
    ap.add_argument("--count", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args(argv)

    import os

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    generate(args.out, args.count, args.seed)
    print(f"  wrote {args.count} lines to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
