from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import mlflow

baseDir = os.getcwd().split('POC-Lakehouse')[0] + 'POC-Lakehouse'
load_dotenv(f'{baseDir}/.env')

def main():
    train_df = pd.read_csv(f"{baseDir}/titanicDataset/titanic-train-clean.csv")
    train_df = train_df.drop(['Unnamed: 0'], axis=1)
    
    train_df = train_df.dropna()

    target = train_df[['Survived']]
    feature = train_df.drop(['PassengerId', 'Name', 'Survived'], axis=1)

    X_train, X_test, y_train, y_test = train_test_split(feature, target, test_size=0.2, shuffle=True, random_state=42)
    
    experiment_name = str(os.getenv('EXPERIMENT_NAME'))

    mlflow.set_tracking_uri("postgresql://mlflow:password@localhost:5432/mlflowdb")
    mlflow.set_registry_uri("postgresql://mlflow:password@localhost:5432/mlflowdb")

    try:
        exp_id = mlflow.set_experiment(experiment_name).experiment_id
    except:
        exp_id = mlflow.create_experiment(experiment_name)

    model_name = str(os.getenv('MODEL_NAME'))
    artifact_path = str(os.getenv('ARTIFACT_PATH'))

    with mlflow.start_run(experiment_id=exp_id):
        model = LogisticRegression()
        model.fit(X_train, y_train)

        accuracy = accuracy_score(y_test, model.predict(X_test))

        mlflow.log_metric("accuracy", accuracy)
        mlflow.sklearn.log_model(sk_model=model, artifact_path=artifact_path)

        run_id = mlflow.active_run().info.run_id

    model_uri = f"runs:/{run_id}/{artifact_path}"
    result = mlflow.register_model(model_uri, model_name)
    mlflow.end_run()


if __name__ == "__main__":
    main()
