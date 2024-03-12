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


load_dotenv(dotenv_path="./creds/credentials.env", verbose=True, override=True)
creds = {
    "host": os.environ.get("host"),
    "database": os.environ.get("database"),
    "user": os.environ.get("user"),
    "password": os.environ.get("password"),
    "port": os.environ.get("port"),
    "iam_user": os.environ.get("iam_user_id"),
    "invoke_url": os.environ.get("streaming_invoke_url"),
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
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"
        )
        return engine


new_connector = AWSDBConnector()

# invoke_url = creds["invoke_url"] + "{}" + "/record"


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

            invoke_url = creds["invoke_url"] + "{}" + "/record"

            payload_pin = json.dumps(
                {
                    "StreamName": "streaming-12f6b2c1ae4f-pin",
                    "Data": {
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
                    },
                    "PartitionKey": "pk1",
                },
                ensure_ascii=False,
                indent=4,
                default=str,
            )

            payload_geo = json.dumps(
                {
                    "StreamName": "streaming-12f6b2c1ae4f-geo",
                    "Data": {
                        "index": geo_result["ind"],
                        "timestamp": geo_result["timestamp"],
                        "latitude": geo_result["latitude"],
                        "longitude": geo_result["longitude"],
                        "country": geo_result["country"],
                    },
                    "PartitionKey": "pk2",
                },
                ensure_ascii=False,
                indent=4,
                default=str,
            )

            payload_user = json.dumps(
                {
                    "StreamName": "streaming-12f6b2c1ae4f-user",
                    "Data": {
                        "index": user_result["ind"],
                        "first_name": user_result["first_name"],
                        "last_name": user_result["last_name"],
                        "age": user_result["age"],
                        "date_joined": user_result["date_joined"],
                    },
                    "PartitionKey": "pk3",
                },
                ensure_ascii=False,
                indent=4,
                default=str,
            )

            headers = {"Content-Type": "application/json"}

            response_pin = requests.request(
                "PUT",
                invoke_url.format("streaming-12f6b2c1ae4f-pin"),
                headers=headers,
                data=payload_pin,
            )
            print("response_pin:", response_pin.status_code)


            response_geo = requests.request(
                "PUT",
                invoke_url.format("streaming-12f6b2c1ae4f-geo"),
                headers=headers,
                data=payload_geo,
            )
            print("response_geo:", response_geo.status_code)


            response_user = requests.request(
                "PUT",
                invoke_url.format("streaming-12f6b2c1ae4f-user"),
                headers=headers,
                data=payload_user,
            )
            print("response_user", response_user.status_code)


if __name__ == "__main__":
    # run_infinite_post_data_loop()
    print("Working")
