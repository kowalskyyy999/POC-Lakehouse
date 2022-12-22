import os
import time
import json

import pandas as pd
import numpy as np

from kafka import KafkaProducer
from kafka import KafkaAdminClient

from dotenv import load_dotenv

baseDir = os.getcwd().split('POC-Lakehouse')[0] + 'POC-Lakehouse'
load_dotenv(f'{baseDir}/.env')

# function to generate data
def generate_data_dummy(appened_data=True):
    datas = {}
    
    try:
        df = pd.read_csv(f"{baseDir}/titanicDataset/titanic-append.csv")
    except:
        df = pd.read_csv(f"{baseDir}/titanicDataset/titanic.csv")
        
    names = df['Name'].unique()
    latestPassengerId = max(df['PassengerId'].unique())
    randPClass = np.random.randint(1, len(df['Pclass'].unique()+1))
    randNum = np.random.randint(0, len(names))
    
    datas['PassengerId'] = latestPassengerId+1    
    datas['Pclass'] = randPClass
    datas['Name'] = names[randNum]
    datas['Sex'] = "".join(df[df['Name'] == names[randNum]]['Sex'].values)
    datas['Age'] = np.random.randint(1, max(df['Age']))
    datas['SibSp'] = int(np.random.choice(df['SibSp'].unique(), 1))
    datas['Parch'] = int(np.random.choice(df['Parch'].unique(), 1))
    datas['Ticket'] = "".join(df[df['Name'] == names[randNum]]['Ticket'].values)
    datas['Fare'] = np.random.uniform(0, max(df['Fare']))
    datas['Cabin'] = "".join(np.random.choice(df['Cabin'].dropna().unique(), 1))
    datas['Embarked'] = "".join(np.random.choice(df['Embarked'].unique(), 1))
    
    if appened_data:
        datas_df = pd.DataFrame(datas, index=[df.index.stop + 1])
        new_df = pd.concat([df, datas_df], axis=0)
        new_df.to_csv(f"{baseDir}/titanicDataset/titanic-append.csv")        
    
    return datas

# converter data type while send data to kafka broker/topic
def converter(x):
    if isinstance(x, np.int64):
        return int(x)
    raise TypeError

# object of create a new topic
class NewTopic:
    def __init__(self, topic, num_partitions=1, replication_factor=1, replica_assignments=None, topic_configs=None):
        self.name = topic
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.replica_assignments = replica_assignments or {}
        self.topic_configs = topic_configs or {}

    def __iter__(self):
        yield self

def main():

    ## Create a New Topic
    adminClient = KafkaAdminClient(bootstrap_servers="localhost:9092")
    topic = NewTopic(str(os.getenv('TOPIC')))
    try:
        adminClient.create_topic(topic)
        if topic.name in adminClient.list_topics():
            print("Topic successfully create")
    except:
        print("Topic Already Exists")

    ## Initializing Kafka Producer
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    if producer.bootstrap_connected():
        print("Producer connected the cluster")

    while True:
        ## wait 3 minute
        time.sleep(3*60)
        ## Generate Data Dummy
        data = generate_data_dummy()
        ## Send data to kafka topic
        producer.send(topic=topic.name, value=json.dumps(data, default=converter).encode('utf-8'))
        print("Data has been sent to Kafka Broker")

if __name__ == "__main__":
    main()
