from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def embed_and_ingest() -> None:
    spark = SparkSession.builder.appName("embed and ingest").getOrCreate()
    ingest(spark)


def ingest(spark: SparkSession) -> None:
    df = spark.read.csv("data/source/real_time_advertiser_auction.csv", header=True, inferSchema=True,
                        timestampFormat="yyyy-MM-dd HH:mm:ss")

    df = df.withColumnRenamed("date", "timestamp").withColumn("date", to_date(col("timestamp"))).drop("timestamp")

    df.write.parquet("data/ingested/real_time_advertiser_auction/", partitionBy="date", mode="overwrite")
