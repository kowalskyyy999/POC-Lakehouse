import os
from pyspark.sql import SparkSession

baseDir = os.getcwd().split('POC-Lakehouse')[0] + 'POC-Lakehouse'

def main():
    spark = SparkSession \
            .builder \
            .appName("POC - EndToEnd Lakehouse") \
            .enableHiveSupport() \
            .getOrCreate()


    titanicDataset = spark \
            .read \
            .csv(f"file://{baseDir}/titanicDataset/titanic.csv", header=True)

    ## Create Table in the metastore with path

    spark.sql("""CREATE DATABASE IF NOT EXISTS titanic""")

    spark.sql("""CREATE TABLE IF NOT EXISTS titanic.bronze (   
                PassengerId STRING,
                Pclass STRING,
                Name STRING,
                Sex STRING,
                Age STRING,
                SibSp STRING,
                Parch STRING,
                Ticket STRING,
                Fare STRING,
                Cabin STRING,
                Embarked STRING
            )
            USING DELTA
        """)

    ## Ingest to Bronze Delta Lake
    titanicDataset \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable("titanic.bronze")

if __name__ == "__main__":
    main()
