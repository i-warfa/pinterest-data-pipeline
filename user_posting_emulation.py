import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.types import Integer, Text, String, DateTime, VARCHAR, FLOAT, NullType
import pymysql
from dotenv import load_dotenv
import os


load_dotenv(dotenv_path="./creds.env", verbose=True, override=True)
creds = {
    "host": os.environ.get("host"),
    "database": os.environ.get("database"),
    "user": os.environ.get("user"),
    "password": os.environ.get("password"),
    "port": os.environ.get("port"),
    "invoke_url": os.environ.get("invoke_url")
}

random.seed(100)


class AWSDBConnector:
    def __init__(self):
        self.HOST = creds["host"]
        self.USER = creds["user"]
        self.PASSWORD = creds["password"]
        self.DATABASE = creds["database"]
        self.PORT = creds["port"]

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
        return engine


new_connector = AWSDBConnector()


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

            invoke_url = creds["invoke_url"] + "/topics/" + "{}"
            
            payload_pin = json.dumps(
                {
                    "records": [
                        {
                            "value": {
                                "index": pin_result["index"],
                                "unique_id": pin_result["unique_id"],
                                "title": pin_result["title"],
                                "description": pin_result["description"],
                                "poster_name": pin_result["poster_name"],
                                "follower_count": pin_result["follower_count"],
                                "tag_list": pin_result["tag_list"],
                                "is_image_or_video": pin_result["is_image_or_video"],
                                "image_src": pin_result["image_src"],
                                "downloaded": pin_result["downloaded"],
                                "save_location": pin_result["save_location"],
                                "category": pin_result["category"],
                            }
                        }
                    ]
                },
            ensure_ascii=False,
            indent=4,
            default=str
            )

            payload_geo = json.dumps(
                {
                    "records": [
                        {
                            "value": {
                                "index": geo_result["ind"],
                                "timestamp": geo_result["timestamp"],
                                "latitude": geo_result["latitude"],
                                "longitude": geo_result["longitude"],
                                "country": geo_result["country"],
                            }
                        }
                    ]
                },
            ensure_ascii=False,
            indent=4,
            default=str
            )

            payload_user = json.dumps(
                {
                    "records": [
                        {
                            "value": {
                                "index": user_result["ind"],
                                "first_name": user_result["first_name"],
                                "last_name": user_result["last_name"],
                                "age": user_result["age"],
                                "date_joined": user_result["date_joined"],
                            }
                        }
                    ]
                },
            ensure_ascii=False,
            indent=4,
            default=str
            )

            headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
            
            response_pin = requests.request("POST", invoke_url.format("0eb5ba52116f.pin"), headers=headers, data=payload_pin)
            response_geo = requests.request("POST", invoke_url.format("0eb5ba52116f.geo"), headers=headers, data=payload_geo)
            response_user = requests.request("POST", invoke_url.format("0eb5ba52116f.user"), headers=headers, data=payload_user)



if __name__ == "__main__":
    run_infinite_post_data_loop()
    print("Working")