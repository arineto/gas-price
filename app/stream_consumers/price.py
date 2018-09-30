import json

from pymongo import MongoClient
from kafka import KafkaConsumer


class PriceConsumer:

    KAFKA_TOPIC_NAME = 'gas_prices'
    MONGO_DB = 'gas_prices'
    MONGO_COLLECTION = 'prices'

    def __init__(self):
        self.consumer = KafkaConsumer(
            self.KAFKA_TOPIC_NAME, group_id='test-group', bootstrap_servers=['localhost:29092']
        )

        client = MongoClient()
        db = client.get_database(self.MONGO_DB)
        self.collection = db.get_collection(self.MONGO_COLLECTION)

    def persist(self, data):
        document = self.collection.find_one({'id': data['id']})
        if document:
            document_id = document['_id']
            self.collection.update({'_id': document_id}, data)
        else:
            document_id = self.collection.insert(data)
        print(f'Pushed to MongoDB: {document_id}')

    def run(self):
        print(f'Starting the {self.KAFKA_TOPIC_NAME} consumer')
        for message in self.consumer:
            data = json.loads(message.value)
            print(f'Received new data: {data}')
            self.persist(data)


if __name__ == '__main__':
    consumer = PriceConsumer()
    consumer.run()
