"""The four measurements the original pipeline was missing.

    python -m bench.run_bench --file data/access.log --all
    python -m bench.run_bench --file data/access.log --throughput
    python -m bench.run_bench --file data/access.log --latency --rate 2000

Each test produces numbers, not an opinion, and each says what it does not cover.
"""

import argparse
import json
import threading
import time
import uuid
from pathlib import Path

from pipeline.config import ES_INDEX, RESULTS_DIR
from pipeline.consumer import consume, make_consumer, make_es
from pipeline.producer import ensure_topic, make_producer, produce

from .metrics import format_table, keeping_up, summarize


def _fresh_group() -> str:
    """A new consumer group per test, so one test never reads another's backlog."""
    return f"bench-{uuid.uuid4().hex[:8]}"


def _reset_index(es) -> None:
    if es.indices.exists(index=ES_INDEX):
        es.indices.delete(index=ES_INDEX)


def run_pair(path: str, count: int, rate: int, bulk_size: int,
             producer_kwargs: dict | None = None,
             per_doc_delay_ms: float = 0.0) -> tuple:
    """Produce and consume at the same time, and return (sent, run).

    They must overlap. Producing the whole file first and only then consuming measures
    how long records sat in Kafka waiting for the producer to finish, which for 20,000
    events at 1,000 per second is twenty seconds of queueing reported as latency. The
    first version of this harness did exactly that and claimed an 8-second p50 on a
    pipeline that actually answers in milliseconds.

    The consumer is subscribed and polling before the first record is produced, so no
    record is ever waiting for a consumer that has not started.
    """
    from pipeline.config import KAFKA_TOPIC

    es = make_es()
    _reset_index(es)
    es_ready = make_es()

    consumer = make_consumer(_fresh_group())
    consumer.subscribe([KAFKA_TOPIC])
    # Join the group before producing: the first poll is what triggers the assignment,
    # and an unassigned consumer silently misses everything sent meanwhile.
    for _ in range(20):
        consumer.poll(0.2)
        if consumer.assignment():
            break

    sent: dict = {}

    def _produce():
        producer = make_producer(**(producer_kwargs or {"acks": "1"}))
        sent.update(produce(path, count, rate, producer))

    thread = threading.Thread(target=_produce, daemon=True)
    thread.start()
    run = consume(count, consumer, es_ready, bulk_size=bulk_size,
                  per_doc_delay_ms=per_doc_delay_ms)
    thread.join(timeout=120)
    consumer.close()
    return sent, run


def test_latency(path: str, count: int, rate: int, bulk_size: int) -> dict:
    """End to end: producer timestamp to Elasticsearch acknowledgement.

    Run at a rate the pipeline can sustain. Measured while saturated, this number is a
    queueing delay and says nothing about the pipeline's own cost.
    """
    sent, run = run_pair(path, count, rate, bulk_size)

    result = summarize(run.latencies_ms, run.indexed, run.seconds)
    result.update({
        "offered_rate": rate,
        "producer_achieved_rate": sent["achieved_rate"],
        "bulk_size": bulk_size,
        "rejected_lines": run.rejected,
        "duplicates": run.duplicates,
        "lost": run.gaps,
    })
    return result


def test_throughput(path: str, count: int, rates: list, bulk_size: int) -> list:
    """Raise the offered rate until the pipeline stops keeping up.

    The answer is the last rate it sustained, not the highest one attempted.
    """
    rows = []
    for rate in rates:
        sent, run = run_pair(path, count, rate, bulk_size)

        s = summarize(run.latencies_ms, run.indexed, run.seconds)
        rows.append({
            "offered": rate,
            "produced": sent["achieved_rate"],
            "indexed/s": s["throughput_eps"],
            "p50 ms": s["latency_ms"]["p50"],
            "p95 ms": s["latency_ms"]["p95"],
            "p99 ms": s["latency_ms"]["p99"],
            "keeping up": "yes" if keeping_up(s["throughput_eps"], rate) else "NO",
        })
        print("   " + json.dumps(rows[-1]), flush=True)
    return rows


def test_backpressure(path: str, count: int, rate: int, delay_ms: float,
                      bulk_size: int) -> dict:
    """Hold the producer steady and slow the consumer, then watch the lag.

    This is the failure mode a tutorial never shows: the producer is fine, the broker is
    fine, and the end-to-end latency grows without bound because the far end cannot keep
    up. Kafka absorbs it, which is the point of having a broker, but the delay is real
    and the measurement has to show it.
    """
    sent, run = run_pair(path, count, rate, bulk_size, per_doc_delay_ms=delay_ms)

    result = summarize(run.latencies_ms, run.indexed, run.seconds)
    result.update({
        "offered_rate": rate,
        "consumer_delay_ms_per_doc": delay_ms,
        "producer_achieved_rate": sent["achieved_rate"],
        "lost": run.gaps,
        "duplicates": run.duplicates,
    })
    return result


def test_bulk_size(path: str, count: int, rate: int, sizes: list) -> list:
    """Sweep the Elasticsearch bulk size at a fixed offered rate.

    This is the knob that matters most and the one a tutorial leaves at its default.
    Intuition says smaller batches mean lower latency, because a document waits less for
    its batch to fill. Measurement says the opposite below a threshold: each bulk request
    carries a fixed cost, and once the consumer cannot drain the partition as fast as the
    producer fills it, latency is set by the growing backlog rather than by the batch.
    """
    rows = []
    for size in sizes:
        sent, run = run_pair(path, count, rate, size)
        s = summarize(run.latencies_ms, run.indexed, run.seconds)
        rows.append({
            "bulk size": size,
            "indexed/s": s["throughput_eps"],
            "p50 ms": s["latency_ms"]["p50"],
            "p95 ms": s["latency_ms"]["p95"],
            "max ms": s["latency_ms"]["max"],
            "keeping up": "yes" if keeping_up(s["throughput_eps"], rate) else "NO",
        })
        print("   " + json.dumps(rows[-1]), flush=True)
    return rows


def test_delivery(path: str, count: int, rate: int, bulk_size: int) -> list:
    """What the delivery guarantee costs.

    acks=1 lets the producer continue once the partition leader has the record; a leader
    failure can still lose it. acks=all with idempotence removes both loss and duplicate
    records, and this is what that safety costs in producer throughput here.
    """
    rows = []
    for label, kwargs in [("acks=1", {"acks": "1"}),
                          ("acks=all", {"acks": "all"}),
                          ("acks=all + idempotent", {"idempotent": True})]:
        sent, run = run_pair(path, count, rate, bulk_size, producer_kwargs=kwargs)

        s = summarize(run.latencies_ms, run.indexed, run.seconds)
        rows.append({
            "guarantee": label,
            "produced/s": sent["achieved_rate"],
            "indexed/s": s["throughput_eps"],
            "p95 ms": s["latency_ms"]["p95"],
            "lost": run.gaps,
            "duplicates": run.duplicates,
        })
        print("   " + json.dumps(rows[-1]), flush=True)
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--file", required=True)
    ap.add_argument("--count", type=int, default=20_000)
    ap.add_argument("--rate", type=int, default=2_000)
    ap.add_argument("--bulk-size", type=int, default=500)
    ap.add_argument("--rates", nargs="+", type=int,
                    default=[1_000, 2_000, 5_000, 10_000, 20_000])
    ap.add_argument("--delay-ms", type=float, default=0.5)
    ap.add_argument("--latency", action="store_true")
    ap.add_argument("--throughput", action="store_true")
    ap.add_argument("--backpressure", action="store_true")
    ap.add_argument("--delivery", action="store_true")
    ap.add_argument("--bulk-sweep", action="store_true")
    ap.add_argument("--bulk-sizes", nargs="+", type=int,
                    default=[50, 100, 250, 500, 1000, 2000])
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args(argv)

    if args.all:
        args.latency = args.throughput = args.backpressure = True
        args.delivery = args.bulk_sweep = True
    if not any([args.latency, args.throughput, args.backpressure, args.delivery,
                args.bulk_sweep]):
        ap.error("choose at least one test, or --all")

    ensure_topic()
    out = {"count": args.count, "bulk_size": args.bulk_size,
           "started": time.strftime("%Y-%m-%d %H:%M:%S")}

    if args.latency:
        print(f"\n== Latency, {args.count} events at {args.rate}/s ==")
        out["latency"] = test_latency(args.file, args.count, args.rate, args.bulk_size)
        print("   " + json.dumps(out["latency"]["latency_ms"]))

    if args.throughput:
        print(f"\n== Throughput sweep, {args.count} events per step ==")
        out["throughput"] = test_throughput(args.file, args.count, args.rates,
                                            args.bulk_size)
        print()
        print(format_table(out["throughput"],
                           ["offered", "produced", "indexed/s", "p50 ms", "p95 ms",
                            "p99 ms", "keeping up"]))

    if args.backpressure:
        print(f"\n== Backpressure, consumer slowed by {args.delay_ms} ms per document ==")
        out["backpressure"] = test_backpressure(args.file, args.count, args.rate,
                                                args.delay_ms, args.bulk_size)
        print("   " + json.dumps(out["backpressure"]["latency_ms"]))

    if args.bulk_sweep:
        print(f"\n== Bulk size sweep, {args.count} events at {args.rate}/s ==")
        out["bulk_size"] = test_bulk_size(args.file, args.count, args.rate,
                                          args.bulk_sizes)
        print()
        print(format_table(out["bulk_size"],
                           ["bulk size", "indexed/s", "p50 ms", "p95 ms", "max ms",
                            "keeping up"]))

    if args.delivery:
        print(f"\n== Delivery guarantees, {args.count} events at {args.rate}/s ==")
        out["delivery"] = test_delivery(args.file, args.count, args.rate, args.bulk_size)
        print()
        print(format_table(out["delivery"],
                           ["guarantee", "produced/s", "indexed/s", "p95 ms",
                            "lost", "duplicates"]))

    Path(RESULTS_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(RESULTS_DIR) / "bench.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nresults written to {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
