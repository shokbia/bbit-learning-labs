import json
from consumer_interface import mqConsumerInterface

import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(self, exchange_name: str):
        self.exchange_name = exchange_name
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.channel = self.connection.channel()
        # Create the exchange if not already present
        self.exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")
        
    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue=queueName,
            routing_key= topic,
            exchange=self.exchange_name,
        )
        
    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present
        self.channel.queue_declare(queue=queueName)
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            queueName, self.on_message_callback, auto_ack=True
        )
        pass
    
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
       # De-Serialize JSON message object if Stock Object Sent
        message = json.loads(body)
        # Acknowledge And Print Message
        print(f"{message['name']} current price is ${message['price']}")

    def startConsuming(self) -> None:
         # Start consuming messages
        print("[*] Waiting for messages. To exit press CTRL + C")
        self.channel.start_consuming()

    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.channel.close()
        # Close Connection
        self.connection.close()
