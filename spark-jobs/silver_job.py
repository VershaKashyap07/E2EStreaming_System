"""
Silver layer: reads raw clickstream events from Kafka and produces a clean,
deduplicated, idempotently-written Iceberg table.

WHAT THIS JOB ACTUALLY HANDLES (map this back to the chaos the producer injects):

  1. MALFORMED events (raw non-Avro bytes)
       -> decode is attempted, caught, routed to clickstream.deadletter.v1
          (a Kafka topic, not dropped/crashed on)
  2. DUPLICATE events (same event_id sent twice)
       -> dropDuplicatesWithinWatermark on event_id handles duplicates that
          arrive within the watermark window in-stream; the Iceberg MERGE
          on write is the second, stronger layer — it makes the write
          idempotent even across job restarts/checkpoint replays, which
          in-stream dedup alone does not guarantee.
  3. BUSINESS-INVALID events (schema-valid, but e.g. negative price)
       -> filtered out of the main table into a separate dlq table, so
          they're inspectable rather than silently corrupting aggregates.
  4. LATE / OUT-OF-ORDER events
       -> event-time watermark of 10 minutes. Events older than that
          relative to the max event_time seen so far are dropped by
          dropDuplicatesWithinWatermark's watermark enforcement. (Building
          an explicit "late events" side-table with its own metric is the
          next iteration — noted in the README roadmap, not silently
          skipped.)

NOT YET HANDLED HERE (intentionally, to keep this step reviewable):
  - Traffic bursts: no explicit backpressure tuning yet (trigger interval,
    maxOffsetsPerTrigger) — next pass.
  - Schema evolution: this job assumes schema v1 — the v2 test is a
    separate, later step.
"""

import struct

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)

import fastavro
import io

SCHEMA_PATH = "/home/iceberg/schemas/click_event_v1.avsc"
KAFKA_BOOTSTRAP = "redpanda:9092"          # internal address, not localhost
SOURCE_TOPIC = "clickstream.raw.v1"
DEADLETTER_TOPIC = "clickstream.deadletter.v1"
CATALOG = "demo"
NAMESPACE = "ecommerce"
SILVER_TABLE = f"{CATALOG}.{NAMESPACE}.silver_clickstream"
INVALID_TABLE = f"{CATALOG}.{NAMESPACE}.dlq_business_invalid"
CHECKPOINT_ROOT = "/home/iceberg/spark-jobs/checkpoints"

with open(SCHEMA_PATH) as f:
    AVRO_SCHEMA = fastavro.schema.parse_schema(fastavro.schema.load_schema(SCHEMA_PATH))


# ---------------------------------------------------------------------------
# Decode: Confluent wire format is [magic byte 0x0][4-byte schema id][avro payload]
# We strip the 5-byte header and decode the payload against our known schema.
# Any failure here (wrong magic byte, truncated bytes, not-avro-at-all) means
# this is a poison-pill message — caught, not crashed on.
# ---------------------------------------------------------------------------
def try_decode(raw_bytes):
    if raw_bytes is None or len(raw_bytes) < 6 or raw_bytes[0] != 0x0:
        return None  # not Confluent-Avro-framed at all -> malformed
    try:
        payload = io.BytesIO(raw_bytes[5:])
        record = fastavro.schemaless_reader(payload, AVRO_SCHEMA)
        return record
    except Exception:
        return None  # malformed avro payload -> malformed


decode_schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("event_time", TimestampType()),
    StructField("ingest_time", TimestampType()),
    StructField("device_type", StringType()),
])


@udf(returnType=decode_schema)
def decode_udf(raw_bytes):
    record = try_decode(bytes(raw_bytes) if raw_bytes is not None else None)
    if record is None:
        return None
    return (
        record["event_id"], record["event_type"], record["user_id"],
        record["session_id"], record.get("product_id"), record.get("category"),
        record.get("price"), record.get("quantity"),
        record["event_time"], record["ingest_time"], record["device_type"],
    )


def main():
    spark = (
        SparkSession.builder
        .appName("silver-clickstream")
        # --- Iceberg catalog: overriding the image's baked-in defaults,
        # which point at container names ("rest", "minio") we didn't use.
        .config(f"spark.sql.catalog.{CATALOG}.uri", "http://iceberg-rest:8181")
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", "http://localstack:4566")
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            event_id STRING, event_type STRING, user_id STRING,
            session_id STRING, product_id STRING, category STRING,
            price DOUBLE, quantity INT,
            event_time TIMESTAMP, ingest_time TIMESTAMP, device_type STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {INVALID_TABLE} (
            event_id STRING, event_type STRING, user_id STRING,
            session_id STRING, product_id STRING, category STRING,
            price DOUBLE, quantity INT,
            event_time TIMESTAMP, ingest_time TIMESTAMP, device_type STRING,
            reason STRING
        ) USING iceberg
    """)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    decoded = raw.withColumn("parsed", decode_udf(col("value")))

    # --- Malformed branch: decode returned null -> send the ORIGINAL raw
    # bytes to the dead-letter topic, untouched, so nothing is lost.
    malformed = decoded.filter(col("parsed").isNull()).select(
        col("key"), col("value")
    )
    malformed_query = (
        malformed.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", DEADLETTER_TOPIC)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/deadletter")
        .outputMode("append")
        .start()
    )

    # --- Successfully decoded events
    events = decoded.filter(col("parsed").isNotNull()).select("parsed.*")
    events = events.withWatermark("event_time", "10 minutes")

    # --- Split: business-invalid (negative price) vs valid
    invalid = events.filter(col("price") < 0)
    valid = (
        events.filter(col("price") >= 0)
        .dropDuplicatesWithinWatermark(["event_id"])
    )

    def write_valid_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.createOrReplaceTempView("batch_events")
        batch_df.sparkSession.sql(f"""
            MERGE INTO {SILVER_TABLE} t
            USING batch_events s
            ON t.event_id = s.event_id
            WHEN NOT MATCHED THEN INSERT *
        """)
        print(f"  [silver] batch {batch_id}: merged {batch_df.count()} rows")

    def write_invalid_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        batch_df.withColumn("reason", col("price").cast("string")) \
            .selectExpr(
                "event_id", "event_type", "user_id", "session_id",
                "product_id", "category", "price", "quantity",
                "event_time", "ingest_time", "device_type",
                "'negative_price' as reason"
            ).createOrReplaceTempView("batch_invalid")
        batch_df.sparkSession.sql(f"""
            INSERT INTO {INVALID_TABLE}
            SELECT * FROM batch_invalid
        """)
        print(f"  [dlq] batch {batch_id}: {batch_df.count()} business-invalid rows")

    valid_query = (
        valid.writeStream
        .foreachBatch(write_valid_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver")
        .outputMode("update")
        .start()
    )

    invalid_query = (
        invalid.writeStream
        .foreachBatch(write_invalid_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/invalid")
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
