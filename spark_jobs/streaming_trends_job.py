"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.
"""

import os
import json
import psycopg2
import redis as redis_lib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "172.19.0.8:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://172.19.0.4:5432/spotify")
POSTGRES_PROPS   = {
    "user":     "spotify",
    "password": "spotify",
    "driver":   "org.postgresql.Driver",
}
REDIS_URL = "redis://172.19.0.3:6379/1"

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
        .config("spark.hadoop.fs.s3a.endpoint",          "http://172.19.0.5:9000")
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka listening_events en streaming.
    Parse le JSON, caste le timestamp en event_time et applique le watermark.
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

    parsed_df = (
        raw_df
        .select(F.col("value").cast("string").alias("json_value"))
        .select(F.from_json(F.col("json_value"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .drop("timestamp")
        .withWatermark("event_time", "10 minutes")
    )

    return parsed_df


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top tracks par tumbling window de 5 minutes.
    Écriture dans PostgreSQL via foreachBatch.
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
    )

    def write_to_postgres(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        try:
            conn = psycopg2.connect(
                host="172.19.0.4", port=5432,
                dbname="spotify", user="spotify", password="spotify"
            )
            cursor = conn.cursor()
            for row in rows:
                cursor.execute("""
                    INSERT INTO realtime_top_tracks
                        (window_start, window_end, track_id, stream_count, unique_listeners)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (window_start, track_id) DO UPDATE SET
                        stream_count     = EXCLUDED.stream_count,
                        unique_listeners = EXCLUDED.unique_listeners,
                        updated_at       = NOW()
                """, (
                    row["window"]["start"],
                    row["window"]["end"],
                    row["track_id"],
                    row["stream_count"],
                    row["unique_listeners"],
                ))
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Batch {batch_id} : {len(rows)} tracks insérés dans realtime_top_tracks")
        except Exception as e:
            print(f"Erreur PostgreSQL batch {batch_id} : {e}")

    query = (
        top_tracks_df.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH + "/top_tracks")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return query


def compute_genre_listeners_sliding(events_df, spark):
    """
    Listeners uniques par genre en sliding window (15 min / 5 min).
    Jointure stream-static avec catalogue PostgreSQL.
    Écriture dans Redis.
    """
    catalog_df = spark.read.jdbc(
        POSTGRES_URL,
        "tracks",
        properties=POSTGRES_PROPS
    ).select(
        F.col("id").cast("string").alias("track_id"),
        F.col("genre")
    )

    enriched_df = events_df.join(
        F.broadcast(catalog_df),
        on="track_id",
        how="left"
    )

    genre_df = (
        enriched_df
        .filter(F.col("genre").isNotNull())
        .groupBy(
            F.window(F.col("event_time"), "15 minutes", "5 minutes"),
            F.col("genre")
        )
        .agg(
            F.approx_count_distinct("user_id").alias("unique_listeners"),
            F.count("*").alias("stream_count"),
        )
    )

    def write_to_redis(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        try:
            r = redis_lib.from_url(REDIS_URL, decode_responses=True)
            genre_data = {}
            for row in rows:
                genre = row["genre"]
                if genre not in genre_data or row["unique_listeners"] > genre_data[genre]["unique_listeners"]:
                    genre_data[genre] = {
                        "genre": genre,
                        "unique_listeners": row["unique_listeners"],
                        "stream_count": row["stream_count"],
                        "window_start": str(row["window"]["start"]),
                        "window_end": str(row["window"]["end"]),
                    }
            r.set("genre_listeners:live", json.dumps(genre_data), ex=3600)
            print(f"Batch {batch_id} : {len(genre_data)} genres écrits dans Redis")
        except Exception as e:
            print(f"Erreur Redis batch {batch_id} : {e}")

    query = (
        genre_df.writeStream
        .outputMode("update")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_PATH + "/genres")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return query


def route_late_events(events_df):
    """
    Route les late events vers le topic Kafka late_listening_events.
    Un event est tardif si son event_time est antérieur de plus de 10 minutes.
    """
    late_df = (
        events_df
        .filter(
            F.col("event_time") < (F.current_timestamp() - F.expr("INTERVAL 10 MINUTES"))
        )
    )

    def send_to_kafka(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
        try:
            from confluent_kafka import Producer
            producer = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "enable.idempotence": True,
            })
            for row in rows:
                payload = json.dumps(row.asDict(), default=str)
                producer.produce("late_listening_events", value=payload.encode("utf-8"))
            producer.flush()
            print(f"Batch {batch_id} : {len(rows)} late events routés vers late_listening_events")
        except Exception as e:
            print(f"Erreur Kafka late events batch {batch_id} : {e}")

    query = (
        late_df.writeStream
        .outputMode("append")
        .foreachBatch(send_to_kafka)
        .option("checkpointLocation", CHECKPOINT_PATH + "/late_events")
        .trigger(processingTime="30 seconds")
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
    query_genres     = compute_genre_listeners_sliding(events_df, spark)
    query_late       = route_late_events(events_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()