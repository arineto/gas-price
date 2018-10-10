import json

from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

prices = [
    {'stationid': '1', 'lat': 8.071058, 'long': 34.909646, 'price': 4.50, 'recordtime': 1539079200000, 'joinner': 1},
]

for price in prices:
    key = price['stationid'].encode('utf-8')
    value = json.dumps(price).encode('utf-8')
    producer.send('gas_prices', key=key, value=value)

locations = [
    {'userid': '10', 'lat':  8.070081, 'long': 34.909722, 'recordtime': 1539079200000, 'joinner': 1},
    {'userid': '11', 'lat':  8.065163, 'long': 34.909089, 'recordtime': 1539079200000, 'joinner': 1},
    {'userid': '12', 'lat':  8.070081, 'long': 34.909722, 'recordtime': 1539086400000, 'joinner': 1},
]

for location in locations:
    key = location['userid'].encode('utf-8')
    value = json.dumps(location).encode('utf-8')
    producer.send('locations', key=key, value=value)
