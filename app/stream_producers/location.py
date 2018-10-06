import json
import random
import time

from kafka import KafkaProducer


class LocationProducer:

    KAFKA_TOPIC_NAME = 'locations'

    MIN_LAT = -8.008605
    MAX_LAT = -8.154150
    MIN_LONG = -34.865000
    MAX_LONG = -34.968500

    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def push_data(self, data):
        print(f'Pushing new data to kafka: {data}')
        json_data = json.dumps(data).encode('utf-8')
        self.producer.send(self.KAFKA_TOPIC_NAME, json_data)

    def generate_location(self):
        lat = round(random.uniform(self.MIN_LAT, self.MAX_LAT), 6)
        long = round(random.uniform(self.MIN_LONG, self.MAX_LONG), 6)
        return lat, long

    def run(self):
        while True:
            lat, long = self.generate_location()
            data = {
                'id': random.randint(1, 101),
                'lat': lat,
                'long': long,
                'location_timestamp': int(time.time() * 1000),
                'joinner': 1,
            }
            self.push_data(data)
            time.sleep(0.5)


if __name__ == '__main__':
    producer = LocationProducer()
    print(f'Starting the {producer.KAFKA_TOPIC_NAME} producer')
    producer.run()
