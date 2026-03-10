from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import sys

# =====================================
# Création de la session Spark
# =====================================
spark = SparkSession.builder \
    .appName("KafkaToPostgresAgg") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =====================================
# Schema des données Kafka
# =====================================
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("flow_rate", DoubleType(), True),
    StructField("water_quality", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("pipeline_id", StringType(), True)
])

# =====================================
# Lecture Kafka
# =====================================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "water_sensor_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# =====================================
# Parsing JSON
# =====================================
df_parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# =====================================
# Nettoyage données
# =====================================
df_clean = df_parsed.filter(
    (col("pressure").isNotNull()) & (col("pressure") > 0) &
    (col("flow_rate").isNotNull()) & (col("flow_rate") > 0)
)

# =====================================
# Agrégation streaming
# =====================================
df_agg = df_clean.groupBy(
    window(col("timestamp"), "5 minutes"),
    col("pipeline_id"),
    col("location")
).agg(
    avg("pressure").alias("avg_pressure"),
    avg("flow_rate").alias("avg_flow_rate"),
    avg("water_quality").alias("avg_water_quality"),
    avg("temperature").alias("avg_temperature"),
    count("sensor_id").alias("num_measurements")
).select(
    col("window.start").alias("start_time"),
    col("window.end").alias("end_time"),
    "pipeline_id",
    "location",
    "avg_pressure",
    "avg_flow_rate",
    "avg_water_quality",
    "avg_temperature",
    "num_measurements"
)

# =====================================
# Fonction écriture PostgreSQL
# =====================================
def write_agg_to_postgres(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} vide")
        return

    try:

        batch_df.coalesce(1).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/project") \
            .option("dbtable", "water_sensor_agg") \
            .option("user", "project") \
            .option("password", "project") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"Batch {batch_id} inséré avec succès")

    except Exception as e:
        print(f"Erreur batch {batch_id}: {e}", file=sys.stderr)

# =====================================
# Streaming Query
# =====================================
query = df_agg.writeStream \
    .foreachBatch(write_agg_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/water_sensor_agg_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming démarré...")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming arrêté")
    query.stop()