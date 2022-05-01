# Dagster & Spark

## Data sets

- [Real time Advertiser's Auction](https://www.kaggle.com/datasets/saurav9786/real-time-advertisers-auction)

## Spark Standalone

1. [Download Spark](https://spark.apache.org/downloads.html)
2. Extract archive and export environment variable `SPARK_HOME` as a path to the extracted directory.
3. Start master `$SPARK_HOME/sbin/start-master.sh`.
4. Start worker `$SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077`.
5. Open [web user interface](http://127.0.0.1:8080).

### Spark History Server

1. Add following configuration to `$SPARK_HOME/conf/spark-defaults.conf`:
    ```
    spark.eventLog.enabled             true
    spark.eventLog.dir                 file:///Users/pwc/var/logs/spark/events
    spark.history.fs.logDirectory      file:///Users/pwc/var/logs/spark/events
    ```
2. Start history server `$SPARK_HOME/sbin/start-history-server.sh`.
3. Open [web user interface](http://127.0.0.1:18080).

## Workflows configurations

### `job_with_spark_resource`

The configuration allows to:

- set custom application name,
- use Spark instance started by `PySpark` (bundled together with the library),
- use standalone Spark instance.

```yaml
resources:
  spark:
    config:
      app: my-app-name
      master: local
```

```yaml
resources:
  spark:
    config:
      app: my-app-name
      master: spark://127.0.0.1:7077
```
