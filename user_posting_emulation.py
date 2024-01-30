import requests
from time import sleep
import random
from multiprocessing import Process
import datetime as dt
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.orm import close_all_sessions

random.seed(100)

class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def serialize_datetime(obj):
    """Serialize datetime objects to ISO format."""
    if isinstance(obj, dt.datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def send_to_kafka(records, topic_name):
    '''Sends data to AWS S3 bucket'''
    invoke_url = "https://7ql7feik5h.execute-api.us-east-1.amazonaws.com/dev/topics/" + topic_name
    
    # Serialize datetime objects using the custom serializer
    payload = json.dumps({"records": [{"value": records}]}, default=serialize_datetime)
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.post(invoke_url, headers=headers, data=payload)

    if response.status_code == 200:
        print(f"Data sent to Kafka topic {topic_name}")
    else:
        print(f"Failed to send data to Kafka topic {topic_name}. Status code: {response.status_code}")
        
def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            '''
            print("pin result")
            print(pin_result)
            print("geo result")
            print(geo_result)
            print("user result")
            print(user_result)
            '''

            # Send data to Kafka
            send_to_kafka(pin_result, "0a9be3223a47.pin")
            send_to_kafka(geo_result, "0a9be3223a47.geo")
            send_to_kafka(user_result, "0a9be3223a47.user")


if __name__ == "__main__":
    #close_all_sessions()
    run_infinite_post_data_loop()
    print('Working')