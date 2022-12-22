#!/bin/bash
source ~/lakehouse/bin/activate

export MLFLOW_KERBEROS_TICKET_CACHE=/tmp/krb5cc_22222222
export MLFLOW_KERBEROS_USER=kowlsss
export MLFLOW_HDFS_DRIVER=libhdfs3

mlflow server \
	--backend-store-uri postgresql://mlflow:password@localhost:5432/mlflowdb \
	--default-artifact-root hdfs://kowalskyyy:9000/mlflow-bucket \
        --host kowalskyyy
