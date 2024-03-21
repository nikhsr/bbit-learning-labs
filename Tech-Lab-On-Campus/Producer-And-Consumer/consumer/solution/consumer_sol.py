import pika
from consumer_interface import mqConsumerInterface 
import os

class mqConsumer(mqConsumerInterface):

    def __init__(self, bindingkey, routingkey, exchangename):
        self.routingKey = routingkey
        self.exchangename = exchangename
        self.bindingKey = bindingkey
        self.setupRMQConnection()

    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchangename, exchange_type='direct')
        self.channel.queue_declare(queue='order_queue', durable=True)
        self.channel.queue_bind(exchange=self.exchangename, queue='order_queue', routing_key=self.routingKey)

    def consumeOrders(self):
        self.channel.basic_consume(queue='order_queue', on_message_callback=self.processOrder, auto_ack=True)
        print(' [*] Waiting for orders. To exit press CTRL+C')
        self.channel.start_consuming()

    def processOrder(self, ch, method, properties, body):
        print(" [x] Received %r" % body)

if __name__ == '__main__':
    consumer = mqConsumer(routingkey='my_routing_key', exchangename='my_exchange')
    consumer.consumeOrders()
