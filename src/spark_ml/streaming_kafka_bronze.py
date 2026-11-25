import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType


def get_spark_streaming_session():
    minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_pass = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    return (
        SparkSession.builder.appName("ISSE02-Kafka-To-Bronze-Streaming")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_pass)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = get_spark_streaming_session()
    spark.sparkContext.setLogLevel("WARN")

    # Schéma exact envoyé par ingestion/producer/producer.py
    sensor_fields = [StructField(f"sensor_{i}", DoubleType(), True) for i in range(1, 22)]
    payload_schema = StructType(
        [
            StructField("engine_id", IntegerType(), True),
            StructField("cycle", IntegerType(), True),
            StructField("setting_1", DoubleType(), True),
            StructField("setting_2", DoubleType(), True),
            StructField("setting_3", DoubleType(), True),
        ]
        + sensor_fields
    )

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # 1. Lecture du flux temps réel depuis Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "capteurs_moteurs")
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. Parsing JSON sans modifier les noms d'origine + ajout de ingestion_timestamp
    bronze_stream = (
        raw_stream.select(
            F.from_json(F.col("value").cast("string"), payload_schema).alias("data")
        )
        .select("data.*")
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    # 3. Écriture en JSON brut dans MinIO Bronze
    query = (
        bronze_stream.writeStream.format("json")
        .outputMode("append")
        .option("path", "s3a://bronze/capteurs_moteurs/")
        .option("checkpointLocation", "s3a://bronze/_checkpoints/capteurs_moteurs/")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(">>> Streaming Kafka -> MinIO Bronze démarré sur le topic 'capteurs_moteurs'...")
    query.awaitTermination()


if __name__ == "__main__":
    main()