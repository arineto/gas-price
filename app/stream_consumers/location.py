import json

from kafka import KafkaConsumer


class LocationConsumer:

    KAFKA_TOPIC_NAME = 'locations'

    def __init__(self):
        self.consumer = KafkaConsumer(
            self.KAFKA_TOPIC_NAME, group_id='test-group', bootstrap_servers=['localhost:29092']
        )

    def run(self):
        print(f'Starting the {self.KAFKA_TOPIC_NAME} consumer')
        for message in self.consumer:
            data = json.loads(message.value)
            print(f'Received new data: {data}')


if __name__ == '__main__':
    consumer = LocationConsumer()
    consumer.run()
