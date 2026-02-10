from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface): 
    def __init__(self, routing_key, exchange_name):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self)->None:
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        self.channel.exchange_declare(self.exchange_name)

    def publishOrder(self, message:str)->None:
        # Basic Publish to Exchange
        self.channel.basic_publish(self.exchange_name, self.routing_key, message)

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()