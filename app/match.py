import json

from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


prices = [
    {'priceid': '1', 'lat': 8.1, 'long': 34.1, 'price': 4.50, 'timestamp': 1, 'joinner': 1},
    {'priceid': '2', 'lat': 8.2, 'long': 34.2, 'price': 4.60, 'timestamp': 2, 'joinner': 1},
    {'priceid': '1', 'lat': 8.1, 'long': 34.1, 'price': 4.70, 'timestamp': 3, 'joinner': 1},
    {'priceid': '1', 'lat': 8.1, 'long': 34.1, 'price': 4.90, 'timestamp': 4, 'joinner': 1},
    {'priceid': '2', 'lat': 8.2, 'long': 34.2, 'price': 4.90, 'timestamp': 5, 'joinner': 1},
]

for price in prices:
    key = price['priceid'].encode('utf-8')
    value = json.dumps(price).encode('utf-8')
    producer.send('gas_prices', key=key, value=value)


locations = [
    {'locationid': '10', 'lat': 8.5, 'long': 34.5, 'timestamp': 1, 'joinner': 1}
]

for location in locations:
    key = location['locationid'].encode('utf-8')
    value = json.dumps(location).encode('utf-8')
    producer.send('locations', key=key, value=value)
