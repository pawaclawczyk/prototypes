from dagster import JobDefinition, OpExecutionContext, repository, job, op, Permissive
from dagster_pyspark import pyspark_resource

from pipelines.real_time_advertiser_auction import ingest, embed_and_ingest


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
    ingest(
        context.resources.spark.spark_session,
        context.op_config["src"],
        context.op_config["dst"],
    )


@job(resource_defs={"spark": spark_standalone})
def job_with_spark_resource() -> None:
    ingest_with_spark_resource()


@job
def job_with_embedded_spark() -> None:
    ingest_with_embedded_spark()


@repository
def real_time_advertiser_auction_repository() -> list[JobDefinition]:
    return [
        job_with_embedded_spark,
        job_with_spark_resource
    ]
