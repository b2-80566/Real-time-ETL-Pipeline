from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, regexp_replace

spark = SparkSession.builder \
    .appName("KafkaStreamProcessing") \
    .getOrCreate()
schema = StructType() \
    .add("Stock Price", StringType()) \
    .add("OPEN PRICE", StringType()) \
    .add("HIGH ", StringType()) \
    .add("LOW", StringType()) \
    .add("RANK", StringType()) \
    .add("PE_RATIO", StringType()) \
    .add("EPS", StringType()) \
    .add("MCAP", StringType()) \
    .add("SECTOR MCAP RANK", StringType()) \
    .add("PB RATIO", StringType()) \
    .add("Div Yield percentage", StringType()) \
    .add("FACE VALUE", StringType()) \
    .add("BETA", StringType()) \
    .add("VWAP", StringType()) \
    .add("52 Week High/Low", StringType()) \
    .add("Overall Recommendation", StringType()) \
    .add("Strong Buy", StringType()) \
    .add("Buy", StringType()) \
    .add("Hold", StringType()) \
    .add("Sell", StringType()) \
    .add("Strong_sell", StringType()) \
    .add("Date", StringType()) \
    .add("Time", StringType())

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "selenium_tests") \
    .load()

# Convert value column from binary to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data and apply schema
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Perform data type conversions
df = df.withColumn("Date", col("Date").cast(DateType())) \
    .withColumn("Time", col("Time").cast(TimestampType())) \
    .withColumn("MCAP", regexp_replace(col("MCAP"), ",", "").cast(DoubleType())) \
    .withColumn("Stock Price", regexp_replace(col("Stock Price"), ",", "").cast(DoubleType())) \
    .withColumn("OPEN PRICE", regexp_replace(col("OPEN PRICE"), ",", "").cast(DoubleType())) \
    .withColumn("HIGH ", regexp_replace(col("HIGH "), ",", "").cast(DoubleType())) \
    .withColumn("LOW", regexp_replace(col("LOW"), ",", "").cast(DoubleType())) \
    .withColumn("PB RATIO", regexp_replace(col("PB RATIO"), ",", "").cast(DoubleType())) \
    .withColumn("52 Week High/Low", regexp_replace(col("52 Week High/Low"), ",", "").cast(DoubleType())) \
    .withColumn("Strong_sell", regexp_replace(col("Strong_sell"), ",", "").cast(DoubleType())) \
    .withColumn("VWAP", regexp_replace(col("VWAP"), ",", "").cast(DoubleType())) \
    .withColumn("Div Yield percentage", regexp_replace(col("Div Yield percentage"), ",", "").cast(DoubleType())) \
    .withColumn("FACE VALUE", regexp_replace(col("FACE VALUE"), ",", "").cast(DoubleType())) \
    .withColumn("BETA", regexp_replace(col("BETA"), ",", "").cast(DoubleType())) \
    .drop("52 Week High/Low").drop("Strong_sell")

query = df.writeStream.format("parquet") \
    .option("path", "hdfs://localhost:9000/user/etlproject") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/checkpoint") \
    .trigger(processingTime="60 seconds") \
    .start()

query.awaitTermination()

# command to submit file on command line
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 SparkConsumer.py
