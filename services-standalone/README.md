# Services Standalone

Guides and scripts to quickly setup various services for development.

All commands were tested on Ubuntu.

## Apache Spark

Download distribution from [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html).

### Makefile

By default master will be set listen on all available IP addresses (`0.0.0.0`).
Overwrite it with `SPARK_LISTEN` environment variable.

By default one worker will be started with default resource allocations.

```shell
make spark-start
```

The master UI is available at http://localhost:8080.
The worker UI is available at http://localhost:8081.

```shell
make spark-stop
```

Edit `$SPARK_HOME/conf/spark-defaults.conf` and enable event log.

```
spark.eventLog.enabled             true
spark.eventLog.dir                 file:///var/log/spark-events/
spark.history.fs.logDirectory      file:///var/log/spark-events/
```

The paths must be the same.

```shell
make spark-history-server-start
```

The history server UI is available at http://localhost:18080.

```shell
make spark-history-server-stop
```

## MinIO

Download MinIO binary from https://min.io/download#/linux.

### Makefile

Not yet implemented:
- Setting custom user and password for administration account. Default credentails (minioadmin:minioadmin) are used.

Parameters:
- `MINIO_HOME` - installation directory
- `MINIO_BIN` - path to `minio` binary
- `MINIO_DATA_DIR` - path to data directory
- `MINIO_PID` - path to pid file

Actions:
- `make minio-init` - creates directory where pid file will be stored
- `make minio-start` - starts server and creates pid file
- `make minio-stop` - stops server using pid file

Services:
- [API @ http://host:9000](http://localhost:9000)
- [UI @ http://host:9090](http://localhost:9090)

## K3S

- Installs as a system service; root access is required.
- Depends on `docker` installation.
- Copy k3s config and replace server address to access the service remotely.
- Tested only service exposed as `NodePort`.

Actions:
- `make k3s-install` - installs k3s server and agent
- `make k3s-config` - links k3s config as `~/.kube/config`
- `make k3s-status` - shows docker and k3s services statuses
- `make k3s-uninstall` - uninstall k3s server and agent

Services:
- [API @ https://host:6443](https://localhost:6443)

