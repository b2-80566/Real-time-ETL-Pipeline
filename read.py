from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master('local') \
    .appName('read') \
    .getOrCreate()


table = spark.read\
            .option('header', True)\
            .option('inferSchema', True)\
            .parquet("hdfs://localhost:9000/user/etlproject")

table.createOrReplaceTempView("table")
result = spark.sql("SELECT count(*) FROM table")
result.show()