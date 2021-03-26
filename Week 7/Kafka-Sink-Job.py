from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    logger = Log4j(spark)

    schema = StructType([
        StructField("Date", StringType()),
        StructField("Province", StringType()),
        StructField("Confirmed", IntegerType()),
        StructField("Recovered", IntegerType()),
        StructField("Deaths", IntegerType()),
        StructField("NumberToday", IntegerType())
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "topic-a") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("value"))

    notification_df = value_df.select("value.Date", "value.Province", "value.Confirmed",
                                      "value.Recovered", "value.Deaths", "value.NumberToday")

    kafka_target_df = notification_df.selectExpr("Date as key",
                                                 """to_json(named_struct(
                                                 'Province', Province,
                                                 'Confirmed', Confirmed,
                                                 'Recovered', Recovered,
                                                 'Deaths', Deaths,
                                                 'NumberToday', NumberToday)) as value
                                                 """)

    notification_writer_query = kafka_target_df \
        .writeStream \
        .queryName("Notification Writer") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "topic-b") \
        .outputMode("append") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

    logger.info("Listening and writing to Kafka")
    notification_writer_query.awaitTermination()