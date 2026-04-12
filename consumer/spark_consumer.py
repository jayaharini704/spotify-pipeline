import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, from_unixtime, when,
    lag, unix_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    FloatType, LongType, DoubleType
)
from pyspark.sql.window import Window

# PostgreSQL connection - uses container name when running in Docker
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "spotify_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "spotify_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASS", "spotify_pass")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver": "org.postgresql.Driver"
}

JDBC_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"

# Kafka address - uses container name inside Docker
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

spark = SparkSession.builder \
    .appName("SpotifyConsumer") \
    .config("spark.jars", JDBC_JAR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("track_id", StringType()),
    StructField("track_name", StringType()),
    StructField("artist", StringType()),
    StructField("album", StringType()),
    StructField("duration_ms", LongType()),
    StructField("timestamp", DoubleType()),
    StructField("source", StringType()),
    StructField("valence", FloatType()),
    StructField("energy", FloatType()),
    StructField("danceability", FloatType()),
    StructField("tempo", FloatType())
])

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "spotify-plays") \
    .option("startingOffsets", "earliest") \
    .option("kafka.metadata.max.age.ms", "60000") \
    .option("failOnDataLoss", "false") \
    .load()

parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

parsed = parsed.withColumn(
    "played_at",
    from_unixtime(col("timestamp")).cast("timestamp")
)

parsed = parsed.withColumn(
    "mood_label",
    when(col("valence") > 0.7, "Happy")
    .when(col("valence") > 0.5, "Positive")
    .when(col("valence") > 0.3, "Neutral")
    .when(col("valence") > 0.1, "Melancholic")
    .otherwise("Sad")
)

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    print(f"Processing batch {batch_id} with {batch_df.count()} records")

    window_spec = Window.orderBy("played_at")
    batch_df = batch_df.withColumn(
        "prev_played_at",
        lag("played_at", 1).over(window_spec)
    )
    batch_df = batch_df.withColumn(
        "gap_minutes",
        (unix_timestamp("played_at") - unix_timestamp("prev_played_at")) / 60
    )
    batch_df = batch_df.withColumn(
        "new_session",
        when(col("gap_minutes") > 30, 1)
        .when(col("prev_played_at").isNull(), 1)
        .otherwise(0)
    )
    batch_df = batch_df.withColumn(
        "session_id",
        when(col("new_session") == 1,
             col("played_at").cast("string"))
        .otherwise(lit(None))
    )

    final_df = batch_df.select(
        "track_id", "track_name", "artist", "album",
        "duration_ms", "played_at", "source",
        "valence", "energy", "danceability", "tempo",
        "session_id", "mood_label"
    )

    final_df.write \
        .jdbc(
            url=POSTGRES_URL,
            table="tracks",
            mode="append",
            properties=POSTGRES_PROPERTIES
        )

    print(f"Batch {batch_id} written to PostgreSQL successfully")

query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Spark consumer started. Listening to Kafka topic: spotify-plays")
query.awaitTermination()