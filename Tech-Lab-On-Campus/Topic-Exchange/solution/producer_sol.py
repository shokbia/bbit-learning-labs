from producer_interface import mqProducerInterface
import pika
import os

from stock import Stock
import json

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
        exchange = self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="topic"
        )

    def publishOrder(self, sector: str, stock: Stock):
        # Create Appropiate Topic String
        topic = f"stock.{stock.ticker}.sector.{sector}"

        dict = {
            "ticker": stock.ticker,
            "price": stock.price,
            "volume": stock.volume
        }
        message = json.dumps(dict)


        # Send serialized message or String
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=topic,
            body=message,
        )

        # Print Confirmation
        print(f" [x] Sent {self.topic}: {message}")

        # Close channel and connection
        self.channel.close()
        self.connection.close()
