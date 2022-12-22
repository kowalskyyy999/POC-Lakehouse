from pyspark.sql import SparkSession
from pyspark.sql.functions import json_tuple

import os
import mlflow
import argparse
import pandas as pd
from pickle import load

from dotenv import load_dotenv

parser = argparse.ArgumentParser()
parser.add_argument("--model-version", type=int, action='store', help='Version model training')
args = parser.parse_args()

baseDir = os.getcwd().split('POC-Lakehouse')[0] + 'POC-Lakehouse'
load_dotenv(f'{baseDir}/.env')

def prep_inference(spark, df, preprocessing, model):
        df.createOrReplaceGlobalTempView("data_stream")

        df = df.toPandas()
        feature_categorical = df[['Pclass', 'Sex', 'SibSp', 'Parch', 'Cabin', 'Embarked']]
        feature_numerical = df[['Age', 'Fare']]

        oho_enc = preprocessing['One_hot_encoder']
        oho_enc.set_params(**{'handle_unknown':'ignore'})
        minmax_enc = preprocessing['MinMax_scaler']

        data_features_categorical = oho_enc.transform(feature_categorical).toarray()
        oho_columns = oho_enc.get_feature_names_out(['Pclass', 'Sex', 'SibSp', 'Parch', 'Cabin', 'Embarked'])
        feature_categorical_df = pd.DataFrame(data_features_categorical, columns=oho_columns)

        # Rename Columns
        for cols in feature_categorical_df.columns:
                if len(cols.split(" ")) > 1:
                        feature_categorical_df=feature_categorical_df.rename(columns={cols:"_".join(cols.split(" "))})

        data_features_numerical = minmax_enc.transform(feature_numerical)
        minmax_columns = minmax_enc.get_feature_names_out(['Age', 'Fare'])
        feature_numerical_df = pd.DataFrame(data_features_numerical, columns=minmax_columns)

        features = pd.concat([feature_categorical_df, feature_numerical_df], axis=1)
        predicted = pd.DataFrame(model.predict(features), columns=['Survived'])
        predicted = pd.concat([df[['PassengerId', 'Name']], predicted], axis=1)
        predicted_spark = spark.createDataFrame(predicted)
        predicted_spark.createOrReplaceGlobalTempView("predicted")

        features = pd.concat([df[['PassengerId', 'Name']], features], axis=1)
        features = spark.createDataFrame(features)
        
        features \
                .write \
                .format('delta') \
                .mode('append') \
                .option('overwriteSchema', 'true') \
                .saveAsTable('titanic.silver')

        predicted = spark.sql("""
        select stream.*, pred.Survived
        from global_temp.data_stream as stream
        inner join global_temp.predicted as pred
        on stream.PassengerId=pred.PassengerId AND stream.Name=pred.Name""")

        predicted \
                .write \
                .format('delta') \
                .mode('append') \
                .option('overwriteSchema', 'true') \
                .saveAsTable('titanic.gold')

def predictive_streaming(model_version=1):
        mlflow.set_tracking_uri("postgresql://mlflow:password@localhost:5432/mlflowdb")
        mlflow.set_registry_uri("postgresql://mlflow:password@localhost:5432/mlflowdb")

        model_name = str(os.getenv('MODEL_NAME'))
        model = mlflow.pyfunc.load_model(
                model_uri=f"models:/{model_name}/{model_version}")

        return model
    
def main():

        spark = SparkSession \
            .builder \
            .appName("POC - EndToEnd Lakehouse") \
            .enableHiveSupport() \
            .getOrCreate()


        if args.model_version is None:
                version = 1
        else:
                version = args.model_version

        ## Load a Preprocessing Pipeline
        preprocessing = load(open(f"{baseDir}/preprocessing.pkl", "rb"))

        ## Load Model
        model = predictive_streaming(model_version=version)

        df = spark \
                .readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe', str(os.getenv('TOPIC'))) \
                .option('startingOffsets', 'earliest') \
                .option('failOnDataLoss', 'false') \
                .load() \
                .selectExpr('CAST(value AS STRING)')
    
        df_clean = df \
            .withColumn('PassengerId', json_tuple('value', 'PassengerId')) \
            .withColumn('Pclass', json_tuple('value', 'Pclass')) \
            .withColumn('Name', json_tuple('value', 'Name')) \
            .withColumn('Sex', json_tuple('value', 'Sex')) \
            .withColumn('Age', json_tuple('value', 'Age')) \
            .withColumn('SibSp', json_tuple('value', 'SibSp')) \
            .withColumn('Parch', json_tuple('value', 'Parch')) \
            .withColumn('Ticket', json_tuple('value', 'Ticket')) \
            .withColumn('Fare', json_tuple('value', 'Fare')) \
            .withColumn('Cabin', json_tuple('value', 'Cabin')) \
            .withColumn('Embarked', json_tuple('value', 'Embarked')) \
            .select('PassengerId', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin', 'Embarked')
    

        df_clean.writeStream \
            .format('delta') \
            .outputMode('append') \
            .option('checkpointLocation', '/spark-streaming/checkpoint') \
            .toTable("titanic.bronze")

        df_clean.writeStream \
            .foreachBatch(lambda df, ids: prep_inference(spark, df, preprocessing, model)) \
            .start()

        df_clean.writeStream \
                .format('console') \
                .outputMode('append') \
                .start()

        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
