from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))


if __name__ == "__main__":
    for e in range(1000):
        data = {'number': e}
        producer.send('python_kafka_topic', value=data)
        print(f"Message sent: {data}")
        sleep(1)
