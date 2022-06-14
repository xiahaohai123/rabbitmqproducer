import sys
import uuid

import pika
import datetime
import ssl


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

    def connect_by_tls(self, ca_file, cert_file, key_file):
        context = ssl.create_default_context(cafile=ca_file)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        context.load_cert_chain(certfile=cert_file, keyfile=key_file)
        ssl_options = pika.SSLOptions(context, self.hostname)
        parameters = pika.ConnectionParameters(host=self.hostname, port=5672,
                                               credentials=pika.PlainCredentials(username="guest", password="guest"),
                                               ssl_options=ssl_options)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def close(self):
        self.channel.close()
        self.connection.close()

    def send_string(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        print(" [X] Sent: '" + message + "'")


if __name__ == '__main__':
    queue_name = str(sys.argv[1])
    message_count = int(sys.argv[2])
    ca_file_path = str(sys.argv[3])
    cert_file_path = str(sys.argv[4])
    key_file_path = str(sys.argv[5])

    client = Client(queue_name)
    client.connect_by_tls(ca_file=ca_file_path, cert_file=cert_file_path, key_file=key_file_path)
    for i in range(0, message_count):
        message = createRandomMessageWithDate()
        client.send_string(message)
    client.close()
