import json
import random
import time

from csv import DictReader
from kafka import KafkaProducer


class PriceProducer:

    KAFKA_TOPIC_NAME = 'gas_prices'

    def __init__(self):
        self.data = self.read_data()
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def read_data(self):
        data = []
        with open('sample_data/gas_stations.csv', 'r') as csv_file:
            reader = DictReader(csv_file)
            data = [row for row in reader]
        return data

    def create_price(self, gas_station):
        price = round(random.uniform(4.09, 4.79), 2)
        gas_station.update({
            'price': price,
        })
        return gas_station

    def push_data(self, data):
        new_data = {
            'id': data.get('id'),
            'lat': data.get('latitude'),
            'long': data.get('longitude'),
            'price': data.get('price'),
            'timestamp': int(time.time() * 1000),
        }
        json_data = json.dumps(new_data).encode('utf-8')
        print(f'Pushing new data to kafka: {json_data}')
        key = f'{new_data["id"]}'.encode('utf-8')
        self.producer.send(self.KAFKA_TOPIC_NAME, key=key, value=json_data)

    def run(self):
        while True:
            gas_station = random.choice(self.data)
            data = self.create_price(gas_station)
            self.push_data(data)
            time.sleep(1)


if __name__ == '__main__':
    producer = PriceProducer()
    print(f'Starting the {producer.KAFKA_TOPIC_NAME} producer')
    producer.run()
