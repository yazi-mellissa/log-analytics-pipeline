# Real-time log analytics: how fast is this pipeline, and where does it break?

Access logs flow through Kafka into Elasticsearch. That architecture is in a thousand
tutorials, and almost none of them answer the only questions that matter when you run it:
how many events per second can it actually sustain, how long does one event take from end
to end, what happens when the consumer falls behind, and what does a delivery guarantee
cost.

This repository is that pipeline with the measurements attached. Every number below came
out of `python -m bench.run_bench` on the machine described at the bottom, and the harness
is in the repository so the numbers can be disputed.

## Results

20,000 events per run, Kafka and Elasticsearch in Docker on one laptop, Elasticsearch
given a 1 GB heap.

### The ceiling is between 2,000 and 5,000 events per second

| Offered | Produced/s | Indexed/s | p50 ms | p95 ms | p99 ms | Keeping up |
|---|---|---|---|---|---|---|
| 1,000 | 999.8 | 997.2 | 261.8 | 480.3 | 507.8 | yes |
| 2,000 | 1,995.4 | 1,978.2 | **159.0** | 274.6 | 296.3 | yes |
| 5,000 | 4,980.2 | 3,862.7 | 1,081.6 | 1,218.7 | 1,273.2 | **no** |
| 10,000 | 9,903.7 | 4,951.4 | 1,186.6 | 1,957.0 | 1,981.6 | no |
| 20,000 | 16,461.7 | 5,001.2 | 1,300.7 | 2,587.8 | 2,774.2 | no |

The indexing rate flattens at about **5,000 events per second** and does not move again no
matter how hard the producer pushes. Past that point the extra load turns into latency
rather than throughput, which is what a queue is for and also why a throughput number
quoted without a latency number means nothing.

Note the producer column at 20,000: it only reached 16,462. Above the consumer's ceiling
the producer's own buffer fills, so even the offered rate stops being what was asked for.

### Latency is lower at 2,000 events per second than at 1,000

This looks wrong and is not. At 1,000 events per second a batch of 500 takes half a second
to fill, and every document in it waits for the slowest member. Doubling the rate halves
that wait, which is worth more than the extra work costs: p50 falls from 262 ms to 159 ms.

Below the ceiling, latency is dominated by how long a document waits for company, not by
how long the system takes to handle it.

### The batch size has an optimum, and going below it is catastrophic

20,000 events offered at 2,000 per second, varying only the Elasticsearch bulk size.

| Bulk size | Indexed/s | p50 ms | p95 ms | Keeping up |
|---|---|---|---|---|
| 50 | 673.3 | 9,431.7 | 18,528.2 | no |
| 100 | 1,168.1 | 4,074.4 | 6,817.5 | no |
| 250 | 1,946.4 | 205.1 | 334.7 | yes |
| **500** | **1,984.9** | **166.9** | **289.7** | **yes** |
| 1,000 | 1,978.8 | 281.4 | 510.2 | yes |
| 2,000 | 778.7 | 589.9 | 15,701.2 | no |

Intuition says smaller batches give lower latency, because a document waits less for its
batch to fill. The measurement says the opposite, and not by a little: **p50 at a batch of
50 is 56 times worse than at 500**. Each bulk request carries a fixed cost, and below
about 250 the consumer cannot drain the partition as fast as the producer fills it. From
there latency is set by a backlog that only grows, and the batch-fill argument stops
applying entirely.

Above the optimum the curve turns up again more gently, as documents wait for a larger
batch to fill. The run at 2,000 hit the harness's idle timeout and stopped early, so its
throughput is a lower bound and its p95 includes the stall; what it establishes is that
the pipeline stopped keeping up, not by how much.

### A slow consumer costs seconds, and loses nothing

| | p50 | p95 | max | lost | duplicates |
|---|---|---|---|---|---|
| Consumer at full speed, 2,000/s | 1,885 ms | 2,873 ms | 3,032 ms | 0 | 0 |
| Consumer slowed by 0.5 ms per document | **7,239 ms** | **11,847 ms** | 12,399 ms | 0 | 0 |

Half a millisecond of extra work per document, against a producer that does not slow down,
multiplies end-to-end latency by about four. Nothing is dropped: Kafka holds the backlog,
which is the whole reason for putting a broker in the middle. But the data is minutes old
by the time it is searchable, and a dashboard built on it is quietly wrong rather than
visibly broken.

### Idempotence is nearly free in throughput and expensive in tail latency

| Guarantee | Produced/s | Indexed/s | p95 ms | Lost | Duplicates |
|---|---|---|---|---|---|
| `acks=1` | 1,993.0 | 1,977.6 | 238.5 | 0 | 0 |
| `acks=all` | 1,988.4 | 1,980.8 | 271.8 | 0 | 0 |
| `acks=all` + idempotent | 1,999.3 | 1,989.5 | **845.9** | 0 | 0 |

Waiting for every replica instead of the leader costs almost nothing here, because there
is one broker and no replica to wait for; on a real cluster this row would look different
and the harness is what would show it. Turning on idempotence costs **3.5 times the p95
latency** while leaving throughput untouched.

No run lost or duplicated an event, including at `acks=1`. That is not evidence that
`acks=1` is safe: nothing failed over during these runs, and `acks=1` loses data precisely
when a leader dies. It is evidence that the harness counts correctly, which is what has to
be true before the other numbers mean anything.

## How loss is counted

Every message carries an envelope the producer writes:

```json
{"t": 1763571042123456789, "seq": 41287, "line": "host - - [01/Jul/1995:..] \"GET / HTTP/1.0\" 200 12"}
```

`t` is the producer's clock in nanoseconds, so end-to-end latency is a subtraction at
index time with one clock involved and no skew. `seq` is a sequence number, so loss is the
count of numbers never seen below the highest one that was, and duplication is the count
of numbers seen twice. Totals alone would hide a loss that coincides with a duplicate.

This is what a replayed log file cannot give you on its own: the timestamp inside an
access log line is from 1995 and says nothing about when the event entered the pipeline.

## Running it

```bash
docker compose up -d kafka elasticsearch       # add --profile ui for Kibana
pip install -r requirements.txt
python -m pipeline.generate --out data/access.log --count 200000
python -m bench.run_bench --file data/access.log --all
```

The generator exists so the benchmark reproduces in one command. The real NASA July 1995
access log is the better input and the pipeline reads it unchanged: drop it in and pass
`--file`.

Individual tests:

```
--latency        end to end at a sustainable rate
--throughput     raise the offered rate until it stops keeping up
--bulk-sweep     vary the Elasticsearch batch size at a fixed rate
--backpressure   hold the producer steady and slow the consumer
--delivery       acks=1 against acks=all against acks=all with idempotence
```

`pytest bench/` covers the percentile logic, the log parser and the loss accounting, which
are the parts that decide what every number above says.

## What is in here

| Path | Content |
|---|---|
| `pipeline/schema.py` | Common Log Format parsing, and the envelope that makes latency measurable |
| `pipeline/producer.py` | Kafka producer with a controlled send rate and selectable delivery guarantees |
| `pipeline/consumer.py` | Kafka to Elasticsearch, recording per-document latency, rejects, gaps and duplicates |
| `pipeline/generate.py` | A skewed CLF generator, so the benchmark needs no download |
| `bench/` | The five tests, the percentile code, and their tests |
| `docker-compose.yml` | Kafka in KRaft mode, Elasticsearch, and Kibana behind a profile |

## Two mistakes worth recording

Both produced plausible numbers, which is what makes them worth writing down.

**Producing and consuming in sequence.** The first harness sent every event and then
started reading, so the reported latency was how long records waited for the producer to
finish. It claimed a p50 of 8.4 seconds for a pipeline that answers in milliseconds. The
two must overlap, and the consumer must be assigned to its partitions before the first
record is sent.

**A fresh consumer group with `auto.offset.reset=earliest`.** Each test uses a new group so
it does not inherit another's offsets, but with `earliest` a new group replays the whole
topic, including every event left by previous runs. Their timestamps were minutes old, so
they arrived with minutes of apparent latency: p95 of 60 seconds, from a pipeline that was
working correctly.

## Credits and limits

Built with **Khaled Benmachiche**. An earlier version of the pipeline lives in
[his repository](https://github.com/khaledbenmachiche/real-time-log-analytics-pipeline);
this one adds the measurement layer, the envelope, the five tests and the loss accounting.

The numbers are from one laptop with everything in Docker on one host, which bounds what
they mean. Kafka, Elasticsearch, the producer and the consumer all contend for the same
cores, so the 5,000 per second ceiling is a property of this arrangement, not of the
software. What transfers is the shape: where the knee is relative to the batch size, that
latency falls as the rate rises below the knee, and what each delivery guarantee costs.
