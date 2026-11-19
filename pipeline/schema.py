"""Parsing the Apache Common Log Format, and the envelope that makes latency measurable.

The envelope is the point of this module. A log line carries the time the *request*
happened, which in a replayed dataset is 1995. It says nothing about when the event
entered the pipeline, so end-to-end latency cannot be derived from the line itself.

Each message is therefore wrapped:

    {"t": <producer wall clock, nanoseconds>, "seq": <sequence number>, "line": "<raw log>"}

`t` is what makes latency measurable at the far end, and `seq` is what makes loss and
duplication countable. Both are producer-side, so a single clock is used throughout and
no clock skew enters the measurement.
"""

import json
import re
from datetime import datetime

# host ident authuser [date] "request" status bytes
CLF = re.compile(
    r'^(?P<host>\S+)\s+(?P<ident>\S+)\s+(?P<user>\S+)\s+'
    r'\[(?P<time>[^\]]+)\]\s+"(?P<request>[^"]*)"\s+'
    r'(?P<status>\d{3})\s+(?P<size>\S+)'
)

TIME_FORMAT = "%d/%b/%Y:%H:%M:%S %z"


def wrap(line: str, produced_ns: int, seq: int) -> bytes:
    return json.dumps({"t": produced_ns, "seq": seq, "line": line},
                      separators=(",", ":")).encode()


def unwrap(payload: bytes) -> dict:
    return json.loads(payload)


def parse_line(line: str) -> dict:
    """Return the fields of one CLF line, or {} if it does not match.

    Real access logs contain malformed lines. Returning {} rather than raising lets the
    pipeline count them as rejects instead of dying on one bad record, and the benchmark
    reports that count so a parser that silently drops half the input cannot look fast.
    """
    m = CLF.match(line.strip())
    if not m:
        return {}
    g = m.groupdict()

    try:
        timestamp = datetime.strptime(g["time"], TIME_FORMAT)
    except ValueError:
        timestamp = None

    parts = g["request"].split()
    method = parts[0] if parts else ""
    path = parts[1] if len(parts) > 1 else ""
    protocol = parts[2] if len(parts) > 2 else ""

    return {
        "host": g["host"],
        "timestamp": timestamp.isoformat() if timestamp else None,
        "method": method,
        "path": path,
        "protocol": protocol,
        "status": int(g["status"]),
        "size": 0 if g["size"] == "-" else int(g["size"]),
    }


ES_MAPPING = {
    "mappings": {
        "properties": {
            "host": {"type": "keyword"},
            "timestamp": {"type": "date"},
            "method": {"type": "keyword"},
            "path": {"type": "keyword"},
            "protocol": {"type": "keyword"},
            "status": {"type": "short"},
            "size": {"type": "long"},
            "seq": {"type": "long"},
            # latency_ms is computed at index time and stored, so the measurement
            # survives in the index and can be re-aggregated later.
            "latency_ms": {"type": "float"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        # The default is 1s. Making it explicit matters: a shorter interval costs
        # throughput, and leaving it implicit would hide that trade from the reader.
        "refresh_interval": "1s",
    },
}
