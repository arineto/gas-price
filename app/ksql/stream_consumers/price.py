import json

from kafka import KafkaConsumer


class PriceConsumer:

    KAFKA_TOPIC_NAME = 'gas_prices'

    def __init__(self):
        self.consumer = KafkaConsumer(
            self.KAFKA_TOPIC_NAME, group_id='test-group', bootstrap_servers=['localhost:9092']
        )

    def run(self):
        print(f'Starting the {self.KAFKA_TOPIC_NAME} consumer')
        for message in self.consumer:
            print(f'Received new data: {message.key} - {message.value}')


if __name__ == '__main__':
    consumer = PriceConsumer()
    consumer.run()
