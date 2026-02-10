import pika
import os
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name

        self.setupRMQConnection()

    def setupRMQConnection(self):
        amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:8888/%2F")
        params = pika.URLParameters(amqp_url)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=self.queue_name)
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')

        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.binding_key)
    
    def on_message_callback(self, channel, method_frame, header_frame, body):
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        print(f"Received message: {body.decode()}")
    
    def startConsuming(self):
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.on_message_callback)
        self.channel.start_consuming()
    
    def __del__(self):
        print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()