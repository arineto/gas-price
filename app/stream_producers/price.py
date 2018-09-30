import json
import random
import time

from csv import DictReader
from kafka import KafkaProducer


class PriceProducer:

    KAFKA_TOPIC_NAME = 'gas_prices'

    def __init__(self):
        self.data = self.read_data()
        self.producer = KafkaProducer(bootstrap_servers=['localhost:29092'])

    def read_data(self):
        data = []
        with open('sample_data/gas_stations.csv', 'r') as csv_file:
            reader = DictReader(csv_file, delimiter=';')
            data = [row for row in reader]
        return data

    def create_price(self, gas_station):
        gas_price = round(random.uniform(4.09, 4.79), 2)
        gas_station.update({
            'fuel_type': 'gas',
            'fuel_price': gas_price,
        })
        return gas_station

    def push_data(self, data):
        json_data = json.dumps(data).encode('utf-8')
        self.producer.send(self.KAFKA_TOPIC_NAME, json_data)

    def run(self):
        while True:
            gas_station = random.choice(self.data)
            data = self.create_price(gas_station)
            print(f'Pushing new data to kafka: {data}')
            self.push_data(data)
            time.sleep(30)

    def start_up(self):
        print(f'Starting up {len(self.data)} gas stations')
        for gas_station in self.data:
            data = self.create_price(gas_station)
            self.push_data(data)


if __name__ == '__main__':
    producer = PriceProducer()
    print(f'Starting the {producer.KAFKA_TOPIC_NAME} producer')
    producer.start_up()
    producer.run()
