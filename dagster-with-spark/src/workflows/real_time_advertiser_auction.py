from dagster import JobDefinition, OpExecutionContext, repository, job, op, Permissive, IOManager, InputContext, \
    OutputContext, io_manager, InitResourceContext, Out, root_input_manager, In
from dagster_pyspark import pyspark_resource
from pyspark.sql import DataFrame, SparkSession

from pipelines.real_time_advertiser_auction import ingest, embed_and_ingest, ingest_and_save, fix_date


@root_input_manager(input_config_schema={"path": str}, required_resource_keys={"spark"})
def spark_csv_input_manager(context: InputContext) -> DataFrame:
    spark: SparkSession = context.resources.spark.spark_session
    return spark.read.csv(path=context.config["path"], header=True, inferSchema=True,
                          timestampFormat="yyyy-MM-dd HH:mm:ss")


class SparkParquetIOManager(IOManager):
    def load_input(self, context: InputContext):
        spark: SparkSession = context.resources.spark.spark_session
        return spark.read.parquet(context.upstream_output.config["path"])

    def handle_output(self, context: OutputContext, df: DataFrame):
        df.write.parquet(path=context.config["path"], mode="overwrite", partitionBy=context.metadata["partition_by"])


@io_manager(output_config_schema={"path": str}, required_resource_keys={"spark"})
def spark_parquet_io_manager(init_context: InitResourceContext) -> IOManager:
    return SparkParquetIOManager()


def configure_spark_session(values: dict):
    hadoop_aws = "org.apache.hadoop:hadoop-aws:3.3.1"
    conf = {
        "spark.app.name": values["app"],
        "spark.master": values["master"],
    }
    conf.update(values["extra"])
    if "spark.jars.packages" in conf and conf["spark.jars.packages"]:
        conf["spark.jars.packages"] += ";" + hadoop_aws
    else:
        conf["spark.jars.packages"] = hadoop_aws
    return {"spark_conf": conf}


spark_standalone = pyspark_resource.configured(configure_spark_session, config_schema={
    "app": str,
    "master": str,
    "extra": Permissive(),
})


@op(config_schema={"src": str, "dst": str})
def ingest_with_embedded_spark(context: OpExecutionContext) -> None:
    embed_and_ingest(
        context.op_config["src"],
        context.op_config["dst"],
    )


@op(required_resource_keys={"spark"}, config_schema={"src": str, "dst": str})
def ingest_with_spark_resource(context: OpExecutionContext) -> None:
    ingest_and_save(
        context.resources.spark.spark_session,
        context.op_config["src"],
        context.op_config["dst"],
    )


@op(required_resource_keys={"spark"}, config_schema={"src": str}, out=Out(metadata={"partition_by": "date"}))
def ingest_with_io_manager(context: OpExecutionContext) -> DataFrame:
    return ingest(
        context.resources.spark.spark_session,
        context.op_config["src"],
    )


@op(ins={"df": In(root_manager_key="input_manager")}, out=Out(metadata={"partition_by": "date"}),
    required_resource_keys={"spark"})
def ingest_with_input_manager(df: DataFrame) -> DataFrame:
    return fix_date(df)


@job
def job_with_embedded_spark() -> None:
    ingest_with_embedded_spark()


@job(resource_defs={"spark": spark_standalone})
def job_with_spark_resource() -> None:
    ingest_with_spark_resource()


@job(resource_defs={"spark": spark_standalone, "io_manager": spark_parquet_io_manager})
def job_with_io_manager() -> None:
    ingest_with_io_manager()


@job(resource_defs={"spark": spark_standalone, "io_manager": spark_parquet_io_manager,
                    "input_manager": spark_csv_input_manager})
def job_with_input_manager() -> None:
    ingest_with_input_manager()


@repository
def real_time_advertiser_auction_repository() -> list[JobDefinition]:
    return [
        job_with_embedded_spark,
        job_with_spark_resource,
        job_with_io_manager,
        job_with_input_manager
    ]
