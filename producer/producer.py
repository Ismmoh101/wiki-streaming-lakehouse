"""Kafka producer: Wikimedia EventStreams (Server-Sent Events) -> Kafka.

Reads the live firehose of edits across every Wikimedia wiki and publishes
each edit to a Kafka topic, keyed by wiki so one wiki's events stay ordered
within a partition.

Run:  python -m producer.producer
"""
from __future__ import annotations

import json
import logging
import os
import signal
import time
from typing import Iterable, Iterator

import requests

log = logging.getLogger("producer")

STREAM_URL = os.getenv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.recentchange")
# Wikimedia asks every client to identify itself.
USER_AGENT = os.getenv("USER_AGENT", "wiki-streaming-lakehouse/1.0 (student portfolio project)")
KEEP_TYPES = {"edit", "new"}


def parse_sse(lines: Iterable[str]) -> Iterator[tuple[str | None, str]]:
    """Parse a Server-Sent Events stream into (event_id, data) pairs.

    Events are separated by a blank line; a multi-line `data:` field is
    joined with newlines, per the SSE spec.
    """
    event_id, data = None, []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if line == "":
            if data:
                yield event_id, "\n".join(data)
            event_id, data = None, []
        elif line.startswith(":"):
            continue  # comment / keep-alive
        else:
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "id":
                event_id = value
            elif field == "data":
                data.append(value)
    if data:
        yield event_id, "\n".join(data)


def to_record(data: str) -> tuple[bytes, bytes] | None:
    """Return (key, value) for an event worth keeping, else None."""
    try:
        event = json.loads(data)
    except json.JSONDecodeError:
        return None
    meta = event.get("meta") or {}
    if event.get("type") not in KEEP_TYPES or meta.get("domain") == "canary" or not meta.get("id"):
        return None
    return event.get("wiki", "unknown").encode(), data.encode()


class Stats:
    def __init__(self):
        self.seen = self.sent = self.skipped = self.failed = 0
        self.started = time.time()

    def line(self) -> str:
        rate = self.sent / max(time.time() - self.started, 1e-9)
        return (f"seen={self.seen} sent={self.sent} skipped={self.skipped} "
                f"failed={self.failed} rate={rate:.1f}/s")


def build_producer():
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,   # no duplicates from producer retries
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,              # small batching window for throughput
        "client.id": "wiki-producer",
    })


def publish(events: Iterable[tuple[str | None, str]], producer, topic: str, stats: Stats,
            log_every: int = 1000) -> str | None:
    """Send events to Kafka. Returns the last SSE id seen (for resuming)."""
    def on_delivery(err, _msg):
        if err is not None:
            stats.failed += 1
            log.warning("Delivery failed: %s", err)
        else:
            stats.sent += 1

    last_id = None
    for event_id, data in events:
        stats.seen += 1
        last_id = event_id or last_id
        record = to_record(data)
        if record is None:
            stats.skipped += 1
            continue
        key, value = record
        while True:
            try:
                producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)  # local queue full: wait for deliveries
        producer.poll(0)
        if stats.seen % log_every == 0:
            log.info(stats.line())
    return last_id


def run(max_retries: int | None = None) -> None:
    producer = build_producer()
    stats = Stats()
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    last_id, attempt = None, 0
    while not stopping and (max_retries is None or attempt <= max_retries):
        headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
        if last_id:
            headers["Last-Event-ID"] = last_id  # resume where the stream dropped
        try:
            with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as resp:
                resp.raise_for_status()
                attempt = 0
                log.info("Connected to %s", STREAM_URL)
                lines = (l for l in resp.iter_lines(decode_unicode=True) if not stopping)
                last_id = publish(parse_sse(lines), producer, TOPIC, stats) or last_id
        except requests.RequestException as exc:
            attempt += 1
            wait = min(2 ** attempt, 60)
            log.warning("Stream error (%s); reconnecting in %ss", exc, wait)
            time.sleep(wait)

    log.info("Shutting down; flushing. %s", stats.line())
    producer.flush(30)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
