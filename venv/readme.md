# Real-Time E-Commerce Clickstream Pipeline

A production-style streaming system — Kafka, schema registry, stream
processing, hot + cold serving paths — built to mimic how this actually
works at big tech (Netflix/Uber/Meta-style Kappa architecture), but running
100% locally in Docker. Zero cloud cost.

## Why this architecture

Most portfolio projects use a single "bronze → silver → gold" pipeline.
That's the right shape for **offline analytics**, but it's not how
production recommendation/personalization systems actually work — those
need a **hot path** (serve a recommendation in <200ms) and a **cold path**
(train models, build dashboards) off the *same* event stream. This project
builds both, because being able to explain that split — and why one stream
needs two consumers with very different latency requirements — is a
stronger system-design answer than a single pipeline.

```
Producer (chaos-injecting clickstream simulator)
        │  Avro-serialized, schema-registry-enforced
        ▼
   Redpanda (Kafka API)  ──topic: clickstream.raw.v1
        │
        ▼
   Spark Structured Streaming (stateful, checkpointed)
        │
   ┌────┴─────────────────────┐
   ▼                          ▼
 HOT PATH                  COLD PATH
 Redis (next phase)        Iceberg (Bronze → Silver → Gold)
 <200ms feature lookup     on MinIO (S3-compatible), via
 for live recommendations  Iceberg REST catalog
```

## What's built so far (Phase 1)

- **Infra**: `docker-compose.yml` — Redpanda broker, Schema Registry,
  Redpanda Console (UI), MinIO, Iceberg REST catalog, Spark, Prometheus +
  Grafana stubs.
- **Schema**: `schemas/click_event_v1.avsc` — the event contract, enforced
  at the Kafka boundary by the schema registry (not just handled later in
  Iceberg).
- **Producer**: `producer/producer.py` — this is the important one. It
  doesn't just simulate clean traffic. It deliberately injects, at
  independently-tunable rates:
  - **Duplicates** (simulated producer retry after a slow ack)
  - **Late / out-of-order events** (simulated offline mobile client
    flushing a buffer minutes later)
  - **Malformed / poison-pill events** (raw non-Avro bytes — a broken
    client SDK)
  - **Schema-valid-but-business-invalid events** (e.g. negative price —
    the schema registry can't catch this, only downstream validation can)
  - **Traffic bursts** (simulated flash sale, N× normal rate for a window)

  Every injected chaos event is logged to stdout so you can watch exactly
  what's about to hit the pipeline and why.

## Running it

```bash
# 1. Start the infrastructure
docker compose up -d

# 2. Wait ~30s for Redpanda to report healthy, then check
docker compose ps

# 3. Create the topic
docker exec redpanda rpk topic create clickstream.raw.v1 --partitions 6

# 4. Install producer deps (on your host, not in Docker — keeps iteration fast)
cd producer && python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 5. Run the producer
python producer.py --rate 5 --duplicate-rate 0.05 --late-rate 0.08

# 6. Watch it in Redpanda Console
open http://localhost:8080
```

Useful URLs once running:
- Redpanda Console (topics, consumer groups, schema registry): `localhost:8080`
- MinIO console (buckets/objects): `localhost:9001` (admin / password123)
- Spark UI: `localhost:4040`
- Grafana: `localhost:3000` (admin / admin)

## Roadmap — what we build next, in order

1. **Silver-layer Spark job**: consume from `clickstream.raw.v1`,
   deduplicate on `event_id`, apply event-time watermarking, route
   malformed/invalid events to a dead-letter Kafka topic
   (`clickstream.deadletter.v1`) instead of crashing, write clean events to
   an Iceberg table via `MERGE INTO` for idempotency.
2. **Late-event handling policy**: decide and implement what happens to
   events arriving after their session's watermark has passed — a
   dedicated "late events" Iceberg table plus a metric, not a silent drop.
3. **Schema evolution test**: register `click_event_v2.avsc` (adds
   `referrer_campaign_id`) against the registry with `BACKWARD`
   compatibility, prove an old consumer doesn't break, prove Iceberg's
   `mergeSchema` picks it up.
4. **Hot path**: Redis sink for a live "last N events per session" feature
   store — the piece that would actually power real-time recommendations.
5. **Gold layer**: windowed aggregates — trending products, cart
   abandonment rate — from the Iceberg Silver table.
6. **Observability**: Grafana dashboard wired to Redpanda + Spark metrics —
   consumer lag, DLQ rate, late-event rate. This is what turns "I handled
   duplicates" into a screenshot you can show in an interview.
7. **Interview answer bank**: a short written STAR-format answer for each
   edge case, once it's built and you've watched it happen.

Next up: the Silver-layer Spark Structured Streaming job (step 1 above) —
that's where the dedup, watermarking, and DLQ logic actually lives.