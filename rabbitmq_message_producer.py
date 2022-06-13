import sys
import uuid

import pika
import datetime


def createRandomMessageWithDate():
    now_time = datetime.datetime.now()
    formatted_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
    uuid_ = uuid.uuid4().hex
    return uuid_ + ":" + formatted_time


class Client:
    hostname = "localhost"
    connection = None
    channel = None
    queue_name = "test_queue_name"

    def __init__(self, input_queue_name="test_queue_name"):
        self.queue_name = input_queue_name

    def connect(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.hostname))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def close(self):
        self.channel.close()
        self.connection.close()

    def send_string(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        print(" [X] Sent: '" + message + "'")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("The 1st parameter is required to define the queue")
        print("A 2nd parameter is required to confirm how many messages to send")
        sys.exit(0)
    queue_name = str(sys.argv[1])
    message_count = int(sys.argv[2])

    client = Client(queue_name)
    client.connect()
    for i in range(0, message_count):
        message = createRandomMessageWithDate()
        client.send_string(message)
    client.close()
