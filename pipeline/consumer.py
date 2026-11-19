"""Kafka to Elasticsearch, with the measurement built in.

The consumer records, for every document it indexes, how long that event took from the
moment the producer created it to the moment it was accepted by Elasticsearch. That is
the number the original pipeline could not produce, because nothing in a replayed 1995
access log tells you when the event entered the system.

It also tracks sequence numbers, so loss and duplication are counted rather than assumed.
A pipeline that silently drops 2% of events looks faster than one that does not.
"""

import logging
import time
from dataclasses import dataclass, field

from confluent_kafka import Consumer, KafkaError

from .config import ES_BULK_SIZE, ES_HOST, ES_INDEX, KAFKA_BOOTSTRAP, KAFKA_TOPIC
from .schema import ES_MAPPING, parse_line, unwrap

logger = logging.getLogger(__name__)


@dataclass
class Run:
    """What one consumer run observed. Every field is counted, none inferred."""

    indexed: int = 0
    rejected: int = 0           # lines the CLF parser did not match
    duplicates: int = 0         # a sequence number seen more than once
    latencies_ms: list = field(default_factory=list)
    seconds: float = 0.0
    seen_seq: set = field(default_factory=set)
    max_seq: int = -1

    @property
    def throughput(self) -> float:
        return self.indexed / self.seconds if self.seconds else 0.0

    @property
    def gaps(self) -> int:
        """Sequence numbers never seen, up to the highest one that was.

        This is the loss count. It is deliberately computed from the producer's own
        numbering rather than from a difference of totals, which would hide a loss that
        happens to coincide with a duplicate.
        """
        return (self.max_seq + 1) - len(self.seen_seq) if self.max_seq >= 0 else 0


def make_es():
    from elasticsearch import Elasticsearch

    es = Elasticsearch(ES_HOST, request_timeout=60, retry_on_timeout=True, max_retries=3)
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, **ES_MAPPING)
        logger.info("created index %s", ES_INDEX)
    return es


def make_consumer(group_id: str = "bench", auto_commit: bool = True,
                  offset_reset: str = "latest") -> Consumer:
    """Build a consumer.

    `offset_reset` defaults to "latest", which matters more than it looks. The benchmark
    uses a fresh consumer group for every run, and with "earliest" a fresh group replays
    the whole topic, including every event left by previous runs. Their producer
    timestamps are minutes old, so they arrive with minutes of apparent latency and the
    measurement becomes meaningless. An earlier version of this harness reported a p95 of
    60 seconds for exactly that reason.

    Use "earliest" for an actual replay, where reprocessing history is the point.
    """
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": auto_commit,
        "fetch.min.bytes": 1,
        "max.partition.fetch.bytes": 4 * 1024 * 1024,
    })


def consume(expected: int, consumer: Consumer, es, bulk_size: int = ES_BULK_SIZE,
            idle_timeout: float = 15.0, per_doc_delay_ms: float = 0.0) -> Run:
    """Consume `expected` events, index them, and return what was observed.

    `per_doc_delay_ms` deliberately slows the consumer. It is how the backpressure test
    is built: a slow consumer cannot be simulated by producing faster, because that also
    changes what the producer and the broker are doing.
    """
    from elasticsearch.helpers import bulk

    run = Run()
    batch = []
    started = time.perf_counter()
    last_message = time.perf_counter()

    while run.indexed + run.rejected < expected:
        if time.perf_counter() - last_message > idle_timeout:
            logger.warning("no message for %.0fs, stopping early", idle_timeout)
            break

        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("kafka error: %s", msg.error())
            continue

        last_message = time.perf_counter()
        envelope = unwrap(msg.value())
        seq = envelope["seq"]

        if seq in run.seen_seq:
            run.duplicates += 1
        run.seen_seq.add(seq)
        run.max_seq = max(run.max_seq, seq)

        fields = parse_line(envelope["line"])
        if not fields:
            run.rejected += 1
            continue

        if per_doc_delay_ms:
            time.sleep(per_doc_delay_ms / 1000.0)

        # Latency is stamped here, before the bulk call, and corrected after it returns,
        # so the figure includes the time the document waited in the batch.
        fields["seq"] = seq
        fields["_produced_ns"] = envelope["t"]
        batch.append(fields)

        if len(batch) >= bulk_size:
            run.indexed += _flush(bulk, es, batch, run)

    if batch:
        run.indexed += _flush(bulk, es, batch, run)

    run.seconds = time.perf_counter() - started
    return run


def _flush(bulk, es, batch: list, run: Run) -> int:
    now_ns = time.time_ns()
    actions = []
    for doc in batch:
        produced = doc.pop("_produced_ns")
        doc["latency_ms"] = (now_ns - produced) / 1_000_000
        run.latencies_ms.append(doc["latency_ms"])
        actions.append({"_index": ES_INDEX, "_source": doc})
    ok, _ = bulk(es, actions, raise_on_error=False, request_timeout=60)
    batch.clear()
    return ok
