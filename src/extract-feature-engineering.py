import os
from pyspark.sql import SparkSession

baseDir = os.getcwd().split('POC-Lakehouse')[0] + 'POC-Lakehouse'

def main():
    spark = SparkSession \
            .builder \
            .appName("POC - EndToEnd LakeHouse") \
            .enableHiveSupport() \
            .getOrCreate()

    feature_engineering_dataset = spark.sql("select * from titanic.silver")
    ## Read Train CSV and Store in global_temp table
    spark \
            .read \
            .csv(f'file://{baseDir}/titanicDataset/train.csv', header=True) \
            .createOrReplaceGlobalTempView('train')

    raw_titanic_train = spark.sql("""
        select silver.*, train.Survived
        from titanic.silver as silver
        inner join global_temp.train as train
        on silver.PassengerId=train.PassengerId
        """)
    
    raw_titanic_train_df = raw_titanic_train \
            .coalesce(1) \
            .toPandas()

    raw_titanic_train_df.to_csv(f'{baseDir}/titanicDataset/titanic-train-clean.csv')

if __name__ == "__main__":
    main()
