# Dagster with Apache Spark

## Data sets

- [Real time Advertiser's Auction](https://www.kaggle.com/datasets/saurav9786/real-time-advertisers-auction)

## Dagster

[Homepage](https://dagster.io/)

### Concepts

- [Op](https://docs.dagster.io/concepts/ops-jobs-graphs/ops)
- [Job](https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs)
- [Resource](https://docs.dagster.io/concepts/resources)
- [IO manager](https://docs.dagster.io/concepts/io-management/io-managers)
- [Root input manager](https://docs.dagster.io/concepts/io-management/unconnected-inputs#providing-an-input-manager-for-a-root-input-)

## Apache Spark

[Homepage](https://spark.apache.org/)

The `pyspark` package lets you run applications in `local` mode.

I've tested the jobs also with standalone instance on a local and a remote host.

## Dagster jobs

### `job_with_embedded_spark`

The simplest setup, similar to what would be implemented when using orchestrators
like [Apache Airflow](https://airflow.apache.org/).
The infrastructure operations, i.e. managing the Spark session, loading and writing data sets,
is implemented within the [job](https://docs.dagster.io/concepts/ops-jobs-graphs/jobs-graphs).

In details:

- Spark session is instantiated within the op,
- data set is loaded within the op,
- and data set is written within the op.

```yaml
# run configuration
ops:
  ingest_with_embedded_spark:
    config:
      src: "data/source/real_time_advertiser_auction.csv"
      dst: "data/ingested/ingest_with_embedded_spark/"
```

It was tested only with `local` Spark instance.

### `job_with_spark_resource`

The Spark session is created once and provided to ops as a [resource](https://docs.dagster.io/concepts/resources).

The data set loading and writing the result is still implemented within an op.

```yaml
# run configuration
ops:
  ingest_with_spark_resource:
    config:
      src: "data/source/real_time_advertiser_auction.csv"
      dst: "data/ingested/ingest_with_spark_resource/"
resources:
  spark:
    config:
      app: "ingest_with_spark_resource"
      master: "local"
      # master: "spark://127.0.0.1:7077"
```

### `job_with_io_manager`

The [IO manager](https://docs.dagster.io/concepts/io-management/io-managers) can handle the data loading and writing
between ops (also after the last op in a job).

In order to test how an IO manager configuration can be customized by an op:

- the path is provided with run configuration,
- the partition key is provided through the op definition.

The data set loading is still implemented within an op.

```yaml
# run configuration
ops:
  ingest_with_io_manager:
    config:
      src: "data/source/real_time_advertiser_auction.csv"
    outputs:
      result:
        path: "data/ingested/ingest_with_io_manager/"
resources:
  spark:
    config:
      app: "ingest_with_io_manager"
      master: "local"
      # master: "spark://127.0.0.1:7077"
```

### `job_with_input_manager`

The [root input manager](https://docs.dagster.io/concepts/io-management/unconnected-inputs#providing-an-input-manager-for-a-root-input-)
is similar to IO manager, but its can only serve as an op's input.

Now, the op implementation provides only data transformations.

```yaml
# run configuration
ops:
  ingest_with_input_manager:
    inputs:
      df:
        path: "data/source/real_time_advertiser_auction.csv"
    outputs:
      result:
        path: "data/ingested/ingest_with_input_manager/"
resources:
  spark:
    config:
      app: "ingest_with_input_manager"
      master: "local"
      # master: "spark://127.0.0.1:7077"
```
