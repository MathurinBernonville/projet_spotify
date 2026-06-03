"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "/tmp/checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":     "spotify",
    "password": "spotify",
    "driver":   "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  False),
    StructField("user_id",      StringType(),  False),
    StructField("track_id",     StringType(),  False),
    StructField("source_peer",  StringType(),  True),
    StructField("timestamp",    StringType(),  False),
    StructField("duration_ms",  IntegerType(), True),
    StructField("device_type",  StringType(),  True),
    StructField("geo_country",  StringType(),  True),
    StructField("completed",    BooleanType(), True),
    StructField("event_source", StringType(),  True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka listening_events en streaming.
    Parse le JSON et caste le timestamp en event_time.
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )

    # Caster la value bytes en string puis parser le JSON
    parsed_df = (
        raw_df
        .select(F.col("value").cast("string").alias("json_value"))
        .select(F.from_json(F.col("json_value"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .drop("timestamp")
    )

    return parsed_df


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top tracks par tumbling window de 5 minutes — sink console pour test.
    """
    top_tracks_df = (
        events_df
        .filter(F.col("completed") == True)
        .groupBy(
            F.window(F.col("event_time"), "5 minutes"),
            F.col("track_id")
        )
        .agg(
            F.count("*").alias("stream_count"),
            F.approx_count_distinct("user_id").alias("unique_listeners"),
        )
        .orderBy(F.col("stream_count").desc())
    )

    query = (
        top_tracks_df.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .option("checkpointLocation", CHECKPOINT_PATH + "/console")
        .trigger(processingTime="10 seconds")
        .start()
    )

    return query


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    events_df = read_kafka_stream(spark)
    query_top_tracks = compute_top_tracks_tumbling(events_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()