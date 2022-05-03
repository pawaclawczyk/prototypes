from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def embed_and_ingest(src: str, dst: str) -> None:
    spark = SparkSession.builder.appName("embed and ingest").getOrCreate()
    ingest(spark, src, dst)


def ingest(spark: SparkSession, src: str, dst: str) -> None:
    df = spark.read.csv(src, header=True, inferSchema=True,
                        timestampFormat="yyyy-MM-dd HH:mm:ss")

    df = df.withColumnRenamed("date", "timestamp").withColumn("date", to_date(col("timestamp"))).drop("timestamp")

    df.write.parquet(dst, partitionBy="date", mode="overwrite")
