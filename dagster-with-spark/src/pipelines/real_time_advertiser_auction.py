from pyspark.sql import SparkSession, DataFrame, functions as fn
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
    return fix_date(df)


def fix_date(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("date", "timestamp").withColumn("date", to_date(col("timestamp"))).drop("timestamp")


def clean(df: DataFrame) -> DataFrame:
    return df.where(fn.col("total_revenue") >= 0)


def add_unpaid_impressions(df: DataFrame) -> DataFrame:
    return df.withColumn("unpaid_total_impressions",
                         fn.when(fn.col("total_revenue") > 0.0001, 0).otherwise(fn.col("total_impressions")))


def publisher_metrics(df: DataFrame) -> DataFrame:
    return df.groupBy("site_id", "date").agg(
        fn.sum("total_impressions").alias("total_impressions"),
        fn.sum("unpaid_total_impressions").alias("unpaid_total_impressions"),
        fn.sum("total_revenue").alias("total_revenue"),
    ).withColumn(
        "fill_rate", (fn.col("total_impressions") - fn.col("unpaid_total_impressions")) / fn.col("total_impressions")
    ).withColumn(
        "CPM", fn.col("total_revenue") * 1000 / (fn.col("total_impressions") - fn.col("unpaid_total_impressions"))
    ).sort("site_id", "date")


def advertiser_metrics(df: DataFrame) -> DataFrame:
    return df.groupBy("advertiser_id", "date").agg(
        fn.sum("total_impressions").alias("total_impressions"),
        fn.sum("unpaid_total_impressions").alias("unpaid_total_impressions"),
        fn.sum("total_revenue").alias("total_cost"),
    ).withColumn(
        "fill_rate", (fn.col("total_impressions") - fn.col("unpaid_total_impressions")) / fn.col("total_impressions")
    ).withColumn(
        "CPM", fn.col("total_cost") * 1000 / (fn.col("total_impressions") - fn.col("unpaid_total_impressions"))
    ).sort("advertiser_id", "date")


def clean_and_compute_metrics(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = clean(df)
    df = add_unpaid_impressions(df)
    return publisher_metrics(df), advertiser_metrics(df)
