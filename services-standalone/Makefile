.PHONY: spark-start spark-stop spark-history-server-start spark-history-server-stop minio-init minio-start minio-stop k3s-install k3s-config k3s-status k3s-uninstall

DATA_DIR   ?= $(HOME)/data
VAR_DIR    ?= $(HOME)/var

SPARK_HOME   ?= $(HOME)/opt/spark
# set to 0.0.0.0 to accept connections from any host
SPARK_LISTEN ?= 0.0.0.0

MINIO_HOME     ?= $(HOME)/opt/minio
MINIO_BIN      ?= $(MINIO_HOME)/bin/minio
MINIO_DATA_DIR ?= $(DATA_DIR)/minio/
MINIO_PID      ?= $(VAR_DIR)/run/minio.pid

K3S_HOME   ?= $(HOME)/opt/k3s

spark-start: ## start Spark master and worker
	$(SPARK_HOME)/sbin/start-master.sh -h $(SPARK_LISTEN)
	$(SPARK_HOME)/sbin/start-worker.sh spark://127.0.0.1:7077

spark-stop: ## stop Spark master and worker
	$(SPARK_HOME)/sbin/stop-worker.sh
	$(SPARK_HOME)/sbin/stop-master.sh

spark-history-server-start: ## start Spark history server
	$(SPARK_HOME)/sbin/start-history-server.sh

spark-history-server-stop: ## stop Spark history server
	$(SPARK_HOME)/sbin/stop-history-server.sh

minio-init: ## initialize directories required for Minio
	mkdir -pv `dirname $(MINIO_PID)`

minio-start: ## start Minio server
	$(MINIO_BIN) --quiet server $(MINIO_DATA_DIR) --console-address :9090 & echo "$$!" > $(MINIO_PID)

minio-stop: $(MINIO_PID) ## stop Minio server
	pkill -F $(MINIO_PID)
	rm $(MINIO_PID)

k3s-install: ## download and install k3s
	curl -sfL https://get.k3s.io | sh -s - --docker --write-kubeconfig-mode 0644

k3s-config: ## link k3s config as kubectl config
	ln -s /etc/rancher/k3s/k3s.yaml ~/.kube/config

k3s-status: ## show services statuses
	systemctl status docker
	systemctl status k3s

k3s-uninstall: ## uninstall k3s
	/usr/local/bin/k3s-uninstall.sh || true

