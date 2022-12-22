from pyspark.sql import SparkSession

from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder

import os
import pandas as pd
from pickle import dump

baseDir = os.getcwd().split('POC-Lakehouse')[0] + 'POC-Lakehouse'

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

    titanic_bronze = spark.sql("select * from titanic.bronze")
    # Conver spark dataframe to pandas dataframe
    titanic_bronze_df = titanic_bronze.toPandas()
    
    ## feature selection
    raw_features = titanic_bronze_df[['Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin', 'Embarked']]
    ## feature selection categorical 
    raw_features_categorical = raw_features[['Pclass', 'Sex', 'SibSp', 'Parch', 'Cabin', 'Embarked']].dropna()
    ## feature selection numerical
    raw_features_numerical = raw_features[['Age', 'Fare']].dropna()

    # One Hot Encoding
    oho_enc = OneHotEncoder()

    oho_enc.fit(raw_features_categorical)
    oho_columns = oho_enc.get_feature_names_out(['Pclass', 'Sex', 'SibSp', 'Parch', 'Cabin', 'Embarked'])
    data_features_categorical = oho_enc.transform(raw_features_categorical).toarray()
    # feature encoder into pandas dataframe
    features_categorical_df = pd.DataFrame(data_features_categorical, columns=oho_columns)
    
    # Rename Columns
    rename_columns = {}
    for cols in features_categorical_df.columns:
        if len(cols.split(" ")) > 1:
            rename_columns[cols] = "_".join(cols.split(" "))
            features_categorical_df=features_categorical_df.rename(columns={cols:"_".join(cols.split(" "))})
    
    # MinMax Scaling
    minmax_enc = MinMaxScaler()
    minmax_enc.fit(raw_features_numerical)
    minmax_columns = minmax_enc.get_feature_names_out(['Age', 'Fare'])
    data_features_numerical = minmax_enc.transform(raw_features_numerical)
    # features scaling into pandas dataframe
    features_numerical_df = pd.DataFrame(data_features_numerical, columns=minmax_columns)

    ## Saving Preprocessing Model
    preprocessing = {'One_hot_encoder':oho_enc,'MinMax_scaler':minmax_enc}
    dump(preprocessing, open(f'{baseDir}/preprocessing.pkl', 'wb'))

    ## Concat to a dataframe
    features = pd.concat([titanic_bronze_df[['PassengerId', 'Name']], features_categorical_df, features_numerical_df], axis=1)

    ## Convert pandas dataframe to spark dataframe
    features = spark.createDataFrame(features)

    # Get Spark SQL query to Create a table in metastore
    query = schema_to_query(features.schema)

    ## Create a table s
    spark.sql(f"""CREATE TABLE IF NOT EXISTS titanic.silver ({query[:-1]}) USING DELTA""")

    features \
            .write \
            .format('delta') \
            .mode('overwrite') \
            .option('overwriteSchema', 'true') \
            .saveAsTable('titanic.silver') 

if __name__ == "__main__":
    main()
