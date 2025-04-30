from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, to_json, struct, min, avg, split
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def start_streaming(spark, topic, schema):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)", "timestamp")
        .select(from_json(col("value"), schema).alias("data"), col("timestamp"))
        .select("data.*", "timestamp")
    )

def write_to_kafka(df, topic, checkpoint, output_mode="append"):
    return (
        df
        .select(to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("topic", topic)
        .option("checkpointLocation", checkpoint)
        .outputMode(output_mode)  # <- Usa aqui
        .start()
    )

def main():
    spark = SparkSession.builder.appName("F1StreamingAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    # Schemas
    historical_schema = StructType([
        StructField("driverId", StringType()),
        StructField("position", StringType()),
        StructField("time", StringType()),
        StructField("year", IntegerType()),
        StructField("lap", IntegerType()),
        StructField("track", StringType())
    ])
    actual_schema = historical_schema

    # Streams
    historical_data = (
        start_streaming(spark=spark, topic="historical_f1_topic", schema=historical_schema)
        .withColumn("position", col("position").cast("int"))
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        .withColumn("minutes", F.regexp_extract("time", r"(\d+):", 1).cast("int"))
        .withColumn("seconds", F.regexp_extract("time", r":(\d+\.\d+)", 1).cast("double"))
        .withColumn("time_converted", (F.col("minutes") * 60 + F.col("seconds")).cast("double"))
    )

    statistics_per_lap = (
        historical_data
        .withWatermark("timestamp", "1 minute")
        .groupby("lap","driverId","year","position")
        .agg(min("time_converted").alias("Time_Lap"))
    )
    statistics_per_lap.printSchema()

    try:
        query1 = write_to_kafka(statistics_per_lap, "f1_driver_stats_topic", "/tmp/kafka-checkpoint/stats", output_mode="update")
        query1.awaitTermination()

    except Exception as e:
        print(f"Erro durante o streaming: {e}")
        import traceback
        traceback.print_exc()
    
if __name__ == '__main__':
    main()
