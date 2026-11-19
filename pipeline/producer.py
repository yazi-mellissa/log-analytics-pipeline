"""Kafka producer with a controlled send rate.

Rate control is what separates a benchmark from a stress test. Sending as fast as the
loop allows measures the producer, not the pipeline: the queue grows without bound and
every latency number becomes a queueing delay. Pinning the rate and raising it in steps
is what exposes the point where the system stops keeping up.
"""

import argparse
import time

from confluent_kafka import Producer

from .config import KAFKA_BOOTSTRAP, KAFKA_PARTITIONS, KAFKA_TOPIC
from .schema import wrap


def make_producer(acks: str = "1", idempotent: bool = False, linger_ms: int = 5) -> Producer:
    """Build a producer.

    `acks` and `idempotent` are exposed because they are the delivery-guarantee knobs
    the benchmark prices:
      acks=0  fire and forget, the broker may never have it
      acks=1  the leader has it, a leader failure can still lose it
      acks=all + idempotent  no loss and no duplicates, at a cost this harness measures
    """
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": acks,
        "linger.ms": linger_ms,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 1_000_000,
    }
    if idempotent:
        conf["enable.idempotence"] = True
        conf["acks"] = "all"
    return Producer(conf)


def ensure_topic(num_partitions: int = KAFKA_PARTITIONS) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    existing = admin.list_topics(timeout=10).topics
    if KAFKA_TOPIC in existing:
        return
    futures = admin.create_topics(
        [NewTopic(KAFKA_TOPIC, num_partitions=num_partitions, replication_factor=1)]
    )
    for f in futures.values():
        f.result()


def read_lines(path: str, limit: int | None = None):
    """Stream lines from the log file, cycling if more events are asked for than it holds."""
    out = 0
    while True:
        with open(path, encoding="latin-1", errors="replace") as fh:
            for line in fh:
                line = line.rstrip("\n")
                if not line:
                    continue
                yield line
                out += 1
                if limit is not None and out >= limit:
                    return


def produce(path: str, count: int, rate: int, producer: Producer) -> dict:
    """Send `count` events at `rate` per second. Returns what actually happened.

    The returned rate is measured, never assumed: if the loop cannot hold the target,
    the report says so instead of quietly reporting the target back.
    """
    delivered = {"ok": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is None:
            delivered["ok"] += 1
        else:
            delivered["failed"] += 1

    interval = 1.0 / rate if rate > 0 else 0.0
    start = time.perf_counter()
    next_send = start

    for seq, line in enumerate(read_lines(path, count)):
        if interval:
            now = time.perf_counter()
            if next_send > now:
                time.sleep(next_send - now)
            next_send += interval

        producer.produce(KAFKA_TOPIC, value=wrap(line, time.time_ns(), seq),
                         on_delivery=on_delivery)
        producer.poll(0)

    producer.flush(60)
    elapsed = time.perf_counter() - start

    return {
        "requested_rate": rate,
        "achieved_rate": round(delivered["ok"] / elapsed, 1) if elapsed else 0.0,
        "sent": count,
        "delivered": delivered["ok"],
        "failed": delivered["failed"],
        "seconds": round(elapsed, 3),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--file", required=True, help="access log file to replay")
    ap.add_argument("--count", type=int, default=50_000)
    ap.add_argument("--rate", type=int, default=5_000, help="events per second, 0 = unthrottled")
    ap.add_argument("--acks", default="1", choices=["0", "1", "all"])
    ap.add_argument("--idempotent", action="store_true")
    args = ap.parse_args(argv)

    ensure_topic()
    p = make_producer(acks=args.acks, idempotent=args.idempotent)
    report = produce(args.file, args.count, args.rate, p)
    for k, v in report.items():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
