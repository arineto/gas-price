import json

from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient


class LocationConsumer:

    KAFKA_READ_TOPIC_NAME = 'locations'
    KAFKA_WRITE_TOPIC_NAME = 'alerts'
    MONGO_DB = 'gas_prices'
    MONGO_COLLECTION = 'prices'

    def __init__(self):
        self.consumer = KafkaConsumer(
            self.KAFKA_TOPIC_NAME, group_id='test-group', bootstrap_servers=['localhost:9092']
        )
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

        client = MongoClient()
        db = client.get_database(self.MONGO_DB)
        self.collection = db.get_collection(self.MONGO_COLLECTION)

    def check_alert(self, data):
        # TODO: Filter by location and time, then push alert to kafka
        pass

    def run(self):
        print(f'Starting the {self.KAFKA_TOPIC_NAME} consumer')
        for message in self.consumer:
            data = json.loads(message.value)
            print(f'Received new data: {data}')


if __name__ == '__main__':
    consumer = LocationConsumer()
    consumer.run()
