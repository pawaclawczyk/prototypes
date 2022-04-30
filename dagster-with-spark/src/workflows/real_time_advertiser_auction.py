from dagster import JobDefinition, OpExecutionContext, repository, job, op
from dagster_pyspark import pyspark_resource

from pipelines.real_time_advertiser_auction import ingest, embedded_and_ingest

_pyspark_resource = pyspark_resource.configured({
    "spark_conf": {
        "spark.app.name": "PySpark Resource"
    }
})


@op
def ingest_with_embedded_spark() -> None:
    embedded_and_ingest()


@op(required_resource_keys={"pyspark"})
def ingest_with_pyspark_resource(context: OpExecutionContext) -> None:
    ingest(context.resources.pyspark.spark_session)


@job(resource_defs={"pyspark": pyspark_resource})
def job_with_pyspark_resource() -> None:
    ingest_with_pyspark_resource()


@job
def job_with_embedded_spark() -> None:
    ingest_with_embedded_spark()


@repository
def real_time_advertiser_auction_repository() -> list[JobDefinition]:
    return [
        job_with_embedded_spark,
        job_with_pyspark_resource
    ]
