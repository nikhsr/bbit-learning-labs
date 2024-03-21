import pika
from consumer_interface import mqConsumerInterface 
import os
import json

class mqConsumer(mqConsumerInterface):
    routingKey = ""
    exchangename = ""
    queueName = ""

    def __init__(self, binding_key,exchange_name,queue_name):
        self.exchangename = exchange_name
        self.routingKey = binding_key
        self.queueName = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchangename, exchange_type='topic')
        self.channel.queue_declare(queue=self.queueName, durable=True)
        self.channel.queue_bind(exchange=self.exchangename, queue=self.queueName, routing_key=self.routingKey)

    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present
        # Set-up Callback function for receiving messages
        self.channel.queue_declare(queue=queueName, durable=True)
        self.channel.basic_consume(queue=queueName, on_message_callback=self.on_message_callback, auto_ack=True)

    def on_message_callback(self, channel, method_frame, header_frame, body):
        # De-Serialize JSON message object if Stock Object Sent
        message = json.loads(body)
        # Acknowledge And Print Message
        print(f" [x] Received {message}")


    def processOrder(self, ch, method, properties, body):
        print(" [x] Received %r" % body)

    def startConsuming(self) -> None:
        self.channel.basic_consume(queue=self.queueName, on_message_callback=self.processOrder, auto_ack=True)
        print(' [*] Waiting for orders. To exit press CTRL+C')
        self.channel.start_consuming()


