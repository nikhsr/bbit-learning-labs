import pika
import os
from producer_interface import mqProducerInterface 

class mqProducer(mqProducerInterface):

    exchange_name = ""
    routing_key = ""

    def __init__(self, routing_key, exchange_name ) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        # Create the exchange if not already present
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.exchange_name, durable=True)
        self.channel.queue_bind(
            queue= self.exchange_name,
            routing_key= self.routing_key,
            exchange= self.exchange_name,
        )
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')

    def publishOrder(self, message):
        self.channel.basic_publish(exchange=self.exchange_name,
                      routing_key=self.routing_key,
                      body=message)
        print(f" [x] Sent {message}")
        self.connection.close()


       


    
    



