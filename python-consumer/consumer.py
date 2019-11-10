from kafka import KafkaConsumer
from json import loads
import threading


def get_consumer_kafka():
    consumer = KafkaConsumer('python_twiter',
                             bootstrap_servers=['localhost:29092'],
                             auto_offset_reset='earliest',
                             enable_auto_commit=True,
                             group_id='python-kakfa-group',
                             value_deserializer=lambda x: loads(x.decode('utf-8')))
    return consumer


def get_messages(consumer, thread_name):
    for message in consumer:
        print(f"""
        Partition: {message.partition}
        Value: {message.value}
        Thread: {thread_name}
        """)


class MyThread(threading.Thread):
    def __init__(self, thread_id, name):
        threading.Thread.__init__(self)
        self.threadID = thread_id
        self.name = name

    def run(self):
        get_messages(get_consumer_kafka(), self.name)


if __name__ == "__main__":
    # Create two threads as follows
    # try:
    #     _thread.start_new_thread(get_messages, get_consumer_kafka())
    #     _thread.start_new_thread(get_messages, get_consumer_kafka())
    # except:
    #     print("Error: unable to start thread")
    # Create new threads
    thread_0 = MyThread(1, "Thread-0")
    thread_1 = MyThread(2, "Thread-1")
    thread_2 = MyThread(2, "Thread-2")

    thread_0.start()
    thread_1.start()
    thread_2.start()
