from dagster import JobDefinition, OpExecutionContext, repository, job, op
from dagster_pyspark import pyspark_resource

from pipelines.real_time_advertiser_auction import ingest, embed_and_ingest


def configure_spark_session(values: dict):
    return {
        "spark_conf": {
            "spark": {
                "master": values["master"],
                "app": {
                    "name": values["app"]
                }
            }
        }
    }


spark_standalone = pyspark_resource.configured(configure_spark_session, config_schema={"master": str, "app": str})


@op
def ingest_with_embedded_spark() -> None:
    embed_and_ingest()


@op(required_resource_keys={"spark"})
def ingest_with_spark_resource(context: OpExecutionContext) -> None:
    ingest(context.resources.spark.spark_session)


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
