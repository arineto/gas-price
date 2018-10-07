import json
import time

from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


def create_price():
    return {
        'id': 1,
        'lat': '-8.0710581',
        'long': '-34.9096468',
        'fuel': 'gas',
        'price': 4.59,
        'price_timestamp': int(time.time() * 1000),
        'joinner': 1,
    }


def create_location():
    return {
        'id': 1,
        'lat': '-8.068362',
        'long': '-34.909811',
        'location_timestamp': int(time.time() * 1000),
        'joinner': 1,
    }


def push_data(topic, data):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    print(f'Pushing new data to kafka: {data}')
    json_data = json.dumps(data).encode('utf-8')
    producer.send(topic, json_data)


def run():
    price = create_price()
    push_data('gas_prices', price)

    location = create_location()
    push_data('locations', location)


if __name__ == '__main__':
    run()
