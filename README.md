# **POC-Lakehouse**
Proof Of Concept implementation of data lakehouse. Using Spark as analytical engine, Delta lake as storage layer, MLFlow as Machine Learning model tracking. Titanic dataset as sample data for ingestion and create a model to predict a passenger survived

## **Prerequisite**
- Apache Hadoop
- Apache Spark
- Apache Kafka
- Apache Hive
- Postgresql
- MLflow
- Pandas
- Sklearn


## Create User and Database Postgresql
```shell
create database mlflowdb;
alter user mlflow with encrypted password 'password';
grant all privileges on database mlflowdb to mlflow;
```

# **How To Run**

## Start MLflow Server
```shell
$ ./start-mlflow-server.sh
```
## Set Environment
```shell
$ vi .env
MODEL_NAME=...       # used for Mlflow
ARTIFACT_PATH=...    # used for Mlflow
EXPERTIMENT_NAME=... # used for Mlflow
TOPIC=...            # used for Kafka
```

## **Batch**
1. Make sure Hadoop, Kafka, Hive, Spark server run/Up
2. Create python environment

    ```python3
    $ python3 -m venv lakehouse
    ```

3. Install python library

    ```python3
    $ pip3 install -r requirements.txt
    ```

4. Create package management

    ```python3
    $ source lakehouse/bin/activate
    
    $(lakehouse) pip3 install venv-pack
    
    $(lakehouse) venv-pack -o lakehouse.tar.gz
    ```
5. Ingest Titanic dataset to delta table
    ```shell
    $ ./sparkApp.sh src/ingest.py
    ```
6. Data preprocessing/Feature engineering and store it to delta table
    ```shell
    $ ./sparkApp.sh src/feature_engineering.py
    ```
7. Export/extract data processing from delta table to local storage (csv file) 
    ```shell
    $ ./sparkApp.sh src/extract-feature-engineering.py
    ```
8. Get a model Machine Learning
    ```shell
    $ python3 src/modelling.py
    ```
9. Predict the data
    ```shell
    $ ./sparkApp.sh src/inference.py --model-version <model versioning>
    ```

## **Streaming**
1. Generate data dummy and send to kafka topic
    ```shell
    python3 src/producer.py
    ```
2. Open new terminal and then running spark streaming application
    ```shell
    ./sparkApp.sh src/delta-streaming.py --model-version <model versioning>
    ```