from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time

input_file = open('./data/bus1.json')
json_array = json.load(input_file)
# print(json_array)

# print(type(json_array),json_array,'jokunan json dosyası tipi')
coordinates = json_array["features"][0]["geometry"]["coordinates"]
# print(type(coordinates),"kordinat tipi")
# print(coordinates)

client = KafkaClient(hosts='127.0.0.1:9092')  # 127.0.0.1  - localhost
topic = client.topics['AutoData']
producer = topic.get_sync_producer()


def generate_uuid():
    return uuid.uuid4()


data = {}

data["busline"] = '0001'

data["key"] = data["busline"] + str("_")

data["timestamp"] = str(datetime.now())

data["latitude"] = coordinates[0][1]

data["longitude"] = coordinates[0][0]


# print(data)

def generate_checkpoint(coordinates):
    i = 0
    while i < len(coordinates):
        data["key"] = data["busline"] + '_'
        data["timestamp"] = str(datetime.utcnow())
        data["latitude"] = coordinates[i][1]
        data["longitude"] = coordinates[i][0]
        message = json.dumps(data)
        producer.produce(message.encode('ascii'))
        time.sleep(1)

        if i == len(coordinates) - 1:
            i = 0
        else:
            i += 1
        print(data)

generate_checkpoint(coordinates)
