from pyspark.sql import SparkSession

import pandas as pd
import mlflow
import argparse
import os

from dotenv import load_dotenv

parser = argparse.ArgumentParser()
parser.add_argument("--model-version", type=int, action='store', help='Version model training')
args = parser.parse_args()

baseDir = os.getcwd().split('POC-Lakehouse')[0] + 'POC-Lakehouse'
load_dotenv(f'{baseDir}/.env')

def schema_to_query(schemas):
    query = ""
    for cols in str(schemas).split('StructType(List(')[-1].split('StructField')[1:]:
        col = cols.split('(')[-1].split(',')[0]
        types = cols.split('(')[-1].split(',')[1]
        if types == 'StringType':
            query += col + " " + "STRING,"
        elif types == 'DoubleType':
            query += col + " " + "DOUBLE,"
        elif types == 'LongType':
            query += col + " " + "LONG,"
        else:
            print('Column undifined types')
            break
    return query

def main():

    spark = SparkSession \
            .builder \
            .appName("POC - EndToEnd LakeHouse") \
            .enableHiveSupport() \
            .getOrCreate()
    
    titanic_dataset_silver = spark.sql("select * from titanic.silver")
    titanic_dataset_bronze = spark.sql("select * from titanic.bronze")

    titanic_dataset_silver_df = titanic_dataset_silver.toPandas()
    titanic_dataset_silver_df.dropna(inplace=True)
    feature = titanic_dataset_silver_df.drop(['PassengerId', 'Name'], axis=1)
    
    mlflow.set_tracking_uri("postgresql://mlflow:password@localhost:5432/mlflowdb")
    mlflow.set_registry_uri("postgresql://mlflow:password@localhost:5432/mlflowdb")

    model_name = str(os.getenv('MODEL_NAME'))

    if args.model_version is None:
        version = 1
    else:
        version = args.model_version

    model = mlflow.pyfunc.load_model(
            model_uri=f"models:/{model_name}/{version}")

    predicted = pd.DataFrame(model.predict(feature), columns=['Survived'])
    predicted = pd.concat([titanic_dataset_silver_df[['PassengerId', 'Name']], predicted], axis=1)

    predicted_spark = spark.createDataFrame(predicted)
    predicted_spark.createOrReplaceGlobalTempView("predicted")

    titanic_dataset_gold = spark.sql("""
            select bronze.*, pred.Survived
            from titanic.bronze as bronze
            inner join global_temp.predicted as pred
            on bronze.PassengerId=pred.PassengerId AND bronze.Name=pred.Name
            """)

    query = schema_to_query(titanic_dataset_gold.schema)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS titanic.gold ({query[:-1]}) USING DELTA""")

    titanic_dataset_gold \
            .write \
            .format('delta') \
            .mode('overwrite') \
            .option('overwriteSchema', 'true') \
            .saveAsTable('titanic.gold')

if __name__ == "__main__":
    main()
