from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, StructField, TimestampType, DoubleType, IntegerType

from config import configuration

def main():
    spark = SparkSession.builder.appName("SmartTravelStreaming")\
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.6,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.664")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()

    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel("WARN")

    # vehicle schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True)
    ])

    # gps schema
    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True)
    ])

    # traffic schema
    traffic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("snapshot", StringType(), True)
    ])

    # weather schema
    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("precipitation", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("air_quality", DoubleType(), True)
    ])

    # emergency schema
    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("incident_id", StringType(), True),
        StructField("incident_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-kraft:29092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withWatermark("timestamp", "2 minutes")
                )

    def stream_writer(input: DataFrame, checkpointFolder, output):
        return(input.writeStream
               .format("parquet")
               .option("checkpointLocation", checkpointFolder)
               .option("path", output)
               .outputMode("append")
               .start())

    vehicle_dataframe = read_kafka_topic('vehicle_data', vehicle_schema).alias('vehicle')
    gps_dataframe = read_kafka_topic('gps_data', gps_schema).alias('gps')
    traffic_dataframe = read_kafka_topic('traffic_data', traffic_schema).alias('traffic')
    weather_dataframe = read_kafka_topic('weather_data', weather_schema).alias('weather')
    emergency_dataframe = read_kafka_topic('emergency_data', emergency_schema).alias('emergency')

    # Checkpoint folder paths
    vehicle_checkpoint_folder = "s3a://eh-spark-streaming-data/checkpoints/vehicle_data"
    gps_checkpoint_folder = "s3a://eh-spark-streaming-data/checkpoints/gps_data"
    traffic_checkpoint_folder = "s3a://eh-spark-streaming-data/checkpoints/traffic_data"
    weather_checkpoint_folder = "s3a://eh-spark-streaming-data/checkpoints/weather_data"
    emergency_checkpoint_folder = "s3a://eh-spark-streaming-data/checkpoints/emergency_data"

    # Output paths
    vehicle_output = "s3a://eh-spark-streaming-data/output/vehicle_data"
    gps_output = "s3a://eh-spark-streaming-data/output/gps_data"
    traffic_output = "s3a://eh-spark-streaming-data/output/traffic_data"
    weather_output = "s3a://eh-spark-streaming-data/output/weather_data"
    emergency_output = "s3a://eh-spark-streaming-data/output/emergency_data"

    # join dataframes with id and timestamp
    query1 = stream_writer(vehicle_dataframe, vehicle_checkpoint_folder, vehicle_output)
    query2 = stream_writer(gps_dataframe, gps_checkpoint_folder, gps_output)
    query3 = stream_writer(traffic_dataframe, traffic_checkpoint_folder, traffic_output)
    query4 = stream_writer(weather_dataframe, weather_checkpoint_folder, weather_output)
    query5 = stream_writer(emergency_dataframe, emergency_checkpoint_folder, emergency_output)

    query5.awaitTermination()

if __name__ == "__main__":
    main()