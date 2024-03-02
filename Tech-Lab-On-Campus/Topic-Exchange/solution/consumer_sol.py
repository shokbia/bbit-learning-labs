import json
from consumer_interface import mqConsumerInterface

import pika
import os

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()
    
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        # Establish Channel
        self.channel = self.connection.channel()
        
        # Create the exchange if not already present
        exchange = self.channel.exchange_declare(exchange=self.exchange_name, exchange_type="topic")
        
    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
             queue=self.queue_name,
            routing_key= self.routing_key,
            exchange=self.exchange_name,
        )
        
    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present
        self.channel.queue_declare(queue=self.queue_name)
        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            "Consumer Queue", self.on_message_callback, auto_ack=False
        )
        pass
    
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
       # De-Serialize JSON message object if Stock Object Sent
        message = json.loads(body)
        # Acknowledge And Print Message
        channel.basic_ack(method_frame.delivery_tag, False)
        print(f" [x] Received Message:  {body}")

    def startConsuming(self) -> None:
         # Start consuming messages
        self.channel.start_consuming()

    def __del__(self) -> None:
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")
        # Close Channel
        self.channel.close()
        # Close Connection
        self.connection.close()
