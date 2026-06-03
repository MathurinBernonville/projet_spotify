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


KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "/tmp/checkpoints/streaming_trends"
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "172.19.0.11:9092")
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://172.19.0.4:5432/spotify")
REDIS_URL        = "redis://172.19.0.3:6379/1"
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
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """Lit le topic Kafka listening_events en streaming."""
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
                host="postgres", port=5432,
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
        .outputMode("complete")
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
    # Chargement statique du catalogue
    catalog_df = spark.read.jdbc(
        POSTGRES_URL,
        "tracks",
        properties=POSTGRES_PROPS
    ).select(
        F.col("id").cast("string").alias("track_id"),
        F.col("genre")
    )

    # Jointure stream-static
    enriched_df = events_df.join(
        F.broadcast(catalog_df),
        on="track_id",
        how="left"
    )

    # Sliding window 15 min / slide 5 min
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
        .outputMode("complete")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_PATH + "/genres")
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
    query_genres = compute_genre_listeners_sliding(events_df, spark)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()