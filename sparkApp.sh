#!/bin/bash

source ~/lakehouse/bin/activate
# export PYSPARK_PYTHON=./lakehouse/bin/python3

$SPARK_HOME/bin/spark-submit \
	--master spark://kowalskyyy:7077 \
	--packages io.delta:delta-core_2.12:2.0.0,org.postgresql:postgresql:42.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2 \
	--archives lakehouse.tar.gz#lakehouse \
	--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
	--conf "spark.executor.memory=4g" \
	--conf "spark.executorEnv.PYSPARK=./lakehouse/bin/python3" \
	--conf "spark.executorEnv.LD_LIBRARY_PATH=./lakehouse/lib/python3.8/site-packages" \
	--conf "spark.executor.extraLibraryPath=./lakehouse/lib/python3.8/site-packages" \
	--conf "spark.driver.extraLibraryPath=./lakehouse/lib/python3.8/site-packages" \
	$1
