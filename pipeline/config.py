"""Configuration, read from the environment so a run can be reproduced from a shell line."""

import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "web-access-logs")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX", "web-logs")

# How many documents go to Elasticsearch in one bulk request. This is the single
# most important tuning knob in the pipeline and the benchmark sweeps it.
ES_BULK_SIZE = int(os.getenv("ES_BULK_SIZE", "500"))

RESULTS_DIR = os.getenv("RESULTS_DIR", "results")
