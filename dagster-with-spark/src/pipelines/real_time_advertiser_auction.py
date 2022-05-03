from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date


def embed_and_ingest(src: str, dst: str) -> None:
    spark = SparkSession.builder.appName("embed and ingest").getOrCreate()
    ingest_and_save(spark, src, dst)


def ingest_and_save(spark: SparkSession, src: str, dst: str) -> None:
    df = ingest(spark, src)
    df.write.parquet(dst, partitionBy="date", mode="overwrite")


def ingest(spark: SparkSession, src: str) -> DataFrame:
    df = spark.read.csv(src, header=True, inferSchema=True,
                        timestampFormat="yyyy-MM-dd HH:mm:ss")

    return df.withColumnRenamed("date", "timestamp").withColumn("date", to_date(col("timestamp"))).drop("timestamp")
