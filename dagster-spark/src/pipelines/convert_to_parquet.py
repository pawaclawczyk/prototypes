from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("Convert to Parquet").getOrCreate()

df = spark.read.csv("data/source/real_time_advertiser_auction.csv", header=True, inferSchema=True,
                    timestampFormat="yyyy-MM-dd HH:mm:ss")

df = df.withColumnRenamed("date", "timestamp").withColumn("date", to_date(col("timestamp"))).drop("timestamp")

df.write.parquet("data/ingested/real_time_advertiser_auction/", partitionBy="date")
