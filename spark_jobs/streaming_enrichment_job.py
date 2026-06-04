from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json, to_json, struct
from pyspark.sql.types import StructType, StringType, TimestampType

# === 1. SCHEMAS ===
LISTENING_EVENT_SCHEMA = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("item_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("item_type", StringType())

P2P_EVENT_SCHEMA = StructType() \
    .add("event_id", StringType()) \
    .add("peer_id", StringType()) \
    .add("action", StringType()) \
    .add("timestamp", StringType())

def create_spark_session():
    return SparkSession.builder \
        .appName("Spotify-Streaming-Enrichment") \
        .getOrCreate()

def read_static_catalog(spark):
    """Charge le catalogue PostgreSQL (statique)"""
    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/spotify") \
        .option("dbtable", "tracks") \
        .option("user", "spotify") \
        .option("password", "spotify") \
        .load() \
        .select(col("id").alias("item_id"), "title", "artist_id", "genre")

def read_kafka_stream(spark, topic, schema):
    """Lecture générique d'un topic Kafka"""
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9094,kafka-3:9096") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("isolation.level", "read_committed") \
        .load()
    
    return df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", col("timestamp").cast(TimestampType())) \
        .drop("timestamp")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    catalog_df = read_static_catalog(spark)
    
    listening_stream = read_kafka_stream(spark, "listening_events", LISTENING_EVENT_SCHEMA)
    p2p_stream = read_kafka_stream(spark, "p2p_network_events", P2P_EVENT_SCHEMA)

    listening_with_watermark = listening_stream \
        .withWatermark("event_time", "10 minutes") \
        .dropDuplicates(["event_id"])
        
    p2p_with_watermark = p2p_stream \
        .withWatermark("event_time", "2 minutes")

    enriched_listening = listening_with_watermark.join(
        catalog_df, 
        "item_id", 
        "left"
    )

    
    final_enriched_stream = enriched_listening.alias("listen").join(
        p2p_with_watermark.alias("p2p"),
        expr("""
            listen.event_id = p2p.event_id AND
            p2p.event_time >= listen.event_time AND
            p2p.event_time <= listen.event_time + interval 2 minutes
        """),
        "left"
    ).select(
        "listen.event_id", "listen.user_id", "listen.item_id", "listen.event_time",
        "listen.title", "listen.genre", "p2p.peer_id", "p2p.action"
    )

    kafka_sink = final_enriched_stream \
        .selectExpr("CAST(event_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9094,kafka-3:9096") \
        .option("topic", "enriched_events") \
        .option("checkpointLocation", "s3a://spotify-checkpoints/enriched_events_kafka/") \
        .outputMode("append")

    minio_sink = final_enriched_stream \
        .writeStream \
        .format("parquet") \
        .option("path", "s3a://spotify-parquet/enriched_events/") \
        .option("checkpointLocation", "s3a://spotify-checkpoints/enriched_events_minio/") \
        .partitionBy("genre") \
        .outputMode("append")

    kafka_query = kafka_sink.start()
    minio_query = minio_sink.start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()