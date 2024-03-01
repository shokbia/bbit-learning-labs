from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str):
        # Save parameters to class variables
        self.routing_key = routing_key
        self.exchange_name = exchange_name

        # Call setupRMQConnection
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        exchange = self.channel.exchange_declare(exchange="Producer Exchange")

    def publishOrder(self, message: str):
        # Basic Publish to Exchange
        self.channel.basic_publish(
            exchange="Producer Exchange",
            routing_key="Routing Key",
            body="Message",
        )

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()

